import asyncio
from dataclasses import dataclass, field

from src.common.kafka.dlq import DeadLetterPublisher, replay_count
from src.jobs.replay_dlq import replay

DLQ = "cbp-ai.course_ingestion.dlq"
SOURCE = "course-events"


@dataclass
class Record:
    value: bytes
    offset: int
    headers: list = field(default_factory=list)
    key: bytes | None = None
    topic: str = DLQ
    partition: int = 0


def dlq_record(offset, value=b"{}", replays=0):
    return Record(value, offset, [("x-source-topic", SOURCE.encode()), ("x-replay-count", str(replays).encode()),
                                  ("x-error-type", b"RuntimeError"), ("x-error-message", b"boom")])


class FakeClusterMetadata:
    def partitions_for_topic(self, topic):
        return {0} if topic == DLQ else None


class FakeKafkaClient:
    async def fetch_all_metadata(self):
        return FakeClusterMetadata()


class FakeDlqConsumer:
    """One-partition DLQ. ``log`` can grow while replay runs."""

    def __init__(self, log):
        self.log = log
        self.committed = 0
        self.fetch_pos = 0
        self.commits = []
        self._client = FakeKafkaClient()  # replay reads partitions from fresh cluster metadata

    def assign(self, tps):
        self.tps = tps
        self.fetch_pos = self.committed

    async def end_offsets(self, tps):
        return {tp: len(self.log) for tp in tps}

    async def position(self, tp):
        return self.fetch_pos

    async def getmany(self, *tps, timeout_ms, max_records):
        records = self.log[self.fetch_pos : self.fetch_pos + max_records]
        if not records:
            return {}
        self.fetch_pos += len(records)
        return {tps[0]: records}

    async def commit(self, offsets):
        (offset,) = offsets.values()
        self.committed = offset
        self.commits.append(offset)


class FailingAgainProducer:
    """Every replayed message fails again in the pipeline and is dead-lettered
    straight back into the DLQ, carrying the replay count like the real worker does."""

    def __init__(self, dlq_log):
        self.dlq_log = dlq_log
        self.sent = []

    async def send_and_wait(self, topic, value, key=None, headers=None):
        self.sent.append((topic, value, headers))
        replayed = Record(value, offset=-1, headers=headers or [], topic=topic)

        async def failing_pipeline_dead_letters_it():
            publisher = DeadLetterPublisher(_Appender(self.dlq_log), DLQ, "course_ingestion")
            await publisher.publish(replayed, RuntimeError("still broken"), attempts=4)

        await failing_pipeline_dead_letters_it()


class _Appender:
    def __init__(self, log):
        self.log = log

    async def send_and_wait(self, topic, value, key=None, headers=None):
        self.log.append(Record(value, len(self.log), headers))


def run(coro):
    return asyncio.run(coro)


def test_messages_failing_again_are_not_replayed_in_the_same_run():
    log = [dlq_record(0, b"a"), dlq_record(1, b"b"), dlq_record(2, b"c")]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)

    stats = run(replay(consumer, producer, DLQ, max_messages=10_000, max_replays=3, idle_polls_allowed=1))

    assert stats["replayed"] == 3
    assert len(producer.sent) == 3                 # each original sent exactly once
    assert len(log) == 6                           # all three failed again and came back
    assert consumer.committed == 3                 # cut-off: the new copies were not touched
    assert [replay_count(r) for r in log[3:]] == [1, 1, 1]


def test_each_run_increments_the_count_until_parked():
    log = [dlq_record(0, b"a")]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)

    results = [run(replay(consumer, producer, DLQ, max_messages=100, max_replays=3, idle_polls_allowed=1))
               for _ in range(5)]

    assert [r["replayed"] for r in results] == [1, 1, 1, 0, 0]
    assert results[3]["parked"] == 1
    assert len(producer.sent) == 3                 # bounded: never more than max_replays sends
    assert results[4] == {}                        # nothing left; the loop has ended


def test_parked_message_does_not_block_others():
    log = [dlq_record(0, b"old", replays=3), dlq_record(1, b"new")]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)
    stats = run(replay(consumer, producer, DLQ, max_messages=100, max_replays=3, idle_polls_allowed=1))
    assert stats == {"parked": 1, "replayed": 1}
    assert producer.sent[0][1] == b"new"


def test_dry_run_sends_and_commits_nothing():
    log = [dlq_record(0), dlq_record(1, replays=5)]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)
    stats = run(replay(consumer, producer, DLQ, max_messages=100, max_replays=3, dry_run=True, idle_polls_allowed=1))
    assert stats == {"would_replay": 1, "parked": 1}
    assert not producer.sent
    assert consumer.commits == []


def test_max_messages_limits_a_run():
    log = [dlq_record(i) for i in range(5)]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)
    stats = run(replay(consumer, producer, DLQ, max_messages=2, max_replays=3, idle_polls_allowed=1))
    assert stats["replayed"] == 2
    assert consumer.committed == 2


def test_target_topic_override_and_replay_header():
    log = [dlq_record(0)]
    consumer, producer = FakeDlqConsumer(log), FailingAgainProducer(log)
    run(replay(consumer, producer, DLQ, max_messages=10, max_replays=3, target_topic="retry", idle_polls_allowed=1))
    topic, _, headers = producer.sent[0]
    assert topic == "retry"
    assert headers == [("x-replay-count", b"1")]
