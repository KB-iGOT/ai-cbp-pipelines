"""Publish a test retirement event (local/dev only).

    uv run python scripts/publish_test_event.py --topic local.course.retirement --identifier do_123 --operation-type RETIRED

It sends only identifier / operationType, which is not a valid ingestion event (that needs the full
format with primaryCategory, courseCategory and transactionData); use the console producer for
ingestion, see README "Send test messages"."""
import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.kafka.producer import build_producer  # noqa: E402
from src.common.settings import PipelineSettings  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--identifier", required=True)
    parser.add_argument("--status", default=None)
    parser.add_argument("--action", default=None)
    parser.add_argument("--operation-type", default=None, help="e.g. RETIRED for course_retirement")
    args = parser.parse_args()

    payload = {"identifier": args.identifier, "contentId": args.identifier}
    if args.status:
        payload["status"] = args.status
    if args.action:
        payload["action"] = args.action
    if args.operation_type:
        payload["operationType"] = args.operation_type

    producer = build_producer(PipelineSettings())
    await producer.start()
    try:
        await producer.send_and_wait(args.topic, json.dumps(payload).encode(), key=args.identifier.encode())
    finally:
        await producer.stop()
    print(f"Sent to {args.topic}: {payload}")


if __name__ == "__main__":
    asyncio.run(main())
