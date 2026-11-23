"""Move messages from a pipeline's DLQ back to the topic they came from.

    python -m src.jobs.replay_dlq course_ingestion
    python -m src.jobs.replay_dlq course_ingestion --dry-run
    python -m src.jobs.replay_dlq course_ingestion --max-messages 100
    python -m src.jobs.replay_dlq course_ingestion --max-replays 5
    python -m src.jobs.replay_dlq course_ingestion --target-topic my.retry.topic

Loop protection:
  * Only messages already in the DLQ when the job starts are replayed. A message that
    fails again during this run lands back in the DLQ after the cut-off and waits for
    the next run, instead of being replayed over and over in the same run.
  * Every replayed message carries an x-replay-count header, which the pipeline keeps if
    the message fails again. Messages already replayed --max-replays times are skipped
    ("parked"): they stay readable in the DLQ topic for investigation but this job won't
    send them again.

Fix the root cause before replaying. The original topic is read by every pipeline
subscribed to it.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from typing import Any

from aiokafka.structs import TopicPartition

from ..common.kafka.consumer import build_consumer
from ..common.kafka.dlq import REPLAY_COUNT_HEADER, SOURCE_TOPIC_HEADER, get_header, replay_count
from ..common.kafka.producer import build_producer
from ..common.log import get_logger, setup_logging
from ..common.settings import PipelineSettings

log = get_logger("jobs.replay_dlq")


async def replay(
    consumer: Any,
    producer: Any,
    dlq_topic: str,
    *,
    max_messages: int,
    max_replays: int,
    target_topic: str = "",
    dry_run: bool = False,
    idle_polls_allowed: int = 3,
) -> Counter:
    """Replay messages that were in ``dlq_topic`` when this function started.

    ``consumer`` must be started and have a group_id; offsets are committed explicitly,
    only up to the last message actually handled.
    """
    stats: Counter = Counter()

    # consumer.topics() fetches metadata but doesn't update the cache partitions_for_topic() reads,
    # so for a topic the consumer isn't subscribed to it would always return None. Read the
    # partitions from the freshly fetched metadata instead.
    cluster = await consumer._client.fetch_all_metadata()
    partitions = cluster.partitions_for_topic(dlq_topic) or set()
    if not partitions:
        log.warning("DLQ topic %s not found or has no partitions", dlq_topic)
        return stats

    tps = [TopicPartition(dlq_topic, p) for p in sorted(partitions)]
    consumer.assign(tps)
    end_offsets = await consumer.end_offsets(tps)  # the cut-off: nothing at or after this is touched

    remaining = set()
    for tp in tps:
        if await consumer.position(tp) < end_offsets[tp]:
            remaining.add(tp)
    if not remaining:
        log.info("Nothing to replay in %s", dlq_topic)
        return stats

    idle = 0
    handled = 0
    while remaining and handled < max_messages and idle < idle_polls_allowed:
        batch = await consumer.getmany(*remaining, timeout_ms=2000, max_records=min(100, max_messages - handled))
        if not batch:
            idle += 1
            continue
        idle = 0
        commit: dict[TopicPartition, int] = {}
        for tp, records in batch.items():
            for record in records:
                if record.offset >= end_offsets[tp] or handled >= max_messages:
                    remaining.discard(tp)
                    break
                handled += 1
                commit[tp] = record.offset + 1
                if commit[tp] >= end_offsets[tp]:
                    remaining.discard(tp)

                count = replay_count(record)
                target = target_topic or get_header(record, SOURCE_TOPIC_HEADER)
                where = f"{tp.partition}@{record.offset}"
                if count >= max_replays:
                    stats["parked"] += 1
                    log.warning(
                        "Parked %s: already replayed %d time(s); last error %s: %s",
                        where, count, get_header(record, "x-error-type"), get_header(record, "x-error-message"),
                    )
                    continue
                if not target:
                    stats["no_target"] += 1
                    log.error("No target topic for %s; skipping", where)
                    continue
                if dry_run:
                    stats["would_replay"] += 1
                    log.info("Would replay %s to %s (replay #%d)", where, target, count + 1)
                    continue
                await producer.send_and_wait(
                    target,
                    value=record.value,
                    key=record.key,
                    headers=[(REPLAY_COUNT_HEADER, str(count + 1).encode("utf-8"))],
                )
                stats["replayed"] += 1
        if commit and not dry_run:
            await consumer.commit(commit)
        log.info("Progress: %s", dict(stats))

    return stats


async def main_async(args: argparse.Namespace) -> None:
    settings = PipelineSettings()
    setup_logging(settings.LOG_LEVEL)
    dlq_topic = settings.dlq_topic(args.pipeline)
    consumer = build_consumer(settings, [], settings.consumer_group(f"{args.pipeline}.dlq-replay"))
    producer = build_producer(settings)
    await consumer.start()
    await producer.start()
    try:
        stats = await replay(
            consumer, producer, dlq_topic,
            max_messages=args.max_messages,
            max_replays=args.max_replays,
            target_topic=args.target_topic,
            dry_run=args.dry_run,
        )
    finally:
        await consumer.stop()
        await producer.stop()
    log.info("Done (%s): %s", dlq_topic, dict(stats) or "nothing to replay")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pipeline")
    parser.add_argument("--max-messages", type=int, default=10_000, help="Stop after this many DLQ messages")
    parser.add_argument("--max-replays", type=int, default=3, help="Skip messages already replayed this many times")
    parser.add_argument("--target-topic", default="", help="Send here instead of each message's source topic")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be replayed; commit nothing")
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
