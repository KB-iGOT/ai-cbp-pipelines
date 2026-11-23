"""Entry point for one pipeline worker.

    python -m src.runner course_ingestion
    python -m src.runner --list
"""
from __future__ import annotations

import argparse
import asyncio
import signal

from .common.context import PipelineContext
from .common.health import Heartbeat
from .common.kafka.consumer import build_consumer
from .common.kafka.dlq import DeadLetterPublisher
from .common.kafka.producer import build_producer
from .common.log import get_logger, setup_logging
from .common.settings import PipelineSettings
from .common.worker import PipelineWorker
from .registry import PIPELINES, get_pipeline

log = get_logger("runner")


async def run(name: str) -> None:
    settings = PipelineSettings()
    setup_logging(settings.LOG_LEVEL)
    pipeline_cls = get_pipeline(name)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    ctx = await PipelineContext.create(settings, with_embeddings=pipeline_cls.needs_embeddings)
    producer = build_producer(settings)
    await producer.start()
    pipeline = pipeline_cls(ctx)
    try:
        await pipeline.on_start()
        consumer = build_consumer(settings, pipeline.topics(), settings.consumer_group(name))
        worker = PipelineWorker(
            pipeline,
            consumer,
            DeadLetterPublisher(producer, settings.dlq_topic(name), name),
            settings,
            heartbeat=Heartbeat(settings.PIPELINE_HEARTBEAT_FILE),
        )
        log.info("Starting %s (group=%s, dlq=%s)", name, settings.consumer_group(name), settings.dlq_topic(name))
        await worker.run(stop)
    finally:
        await pipeline.on_stop()
        await producer.stop()
        await ctx.close()
        log.info("%s stopped", name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a cbp-ai Kafka pipeline worker")
    parser.add_argument("pipeline", nargs="?", help="Pipeline name")
    parser.add_argument("--list", action="store_true", help="List available pipelines")
    args = parser.parse_args()
    if args.list or not args.pipeline:
        print("\n".join(sorted(PIPELINES)))
        return
    asyncio.run(run(args.pipeline))


if __name__ == "__main__":
    main()
