"""Async websocket consumer for ride lifecycle events."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from typing import Any

from config import settings
from ingestion import batcher


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_message(message: str) -> dict[str, Any]:
    parsed = json.loads(message)
    if not isinstance(parsed, dict):
        raise ValueError("websocket message must be a JSON object")
    return parsed


async def flush_buffer(buffer: list[dict[str, Any]], local_dir: str | None = None) -> None:
    if not buffer:
        return

    events_to_flush = list(buffer)
    buffer.clear()
    await asyncio.to_thread(batcher.flush_events_to_minio, events_to_flush, local_dir or settings.LOCAL_BATCH_DIR)


async def consume_events(websocket_url: str = settings.WEBSOCKET_URL, local_dir: str | None = None) -> None:
    """Consume websocket events forever and flush batches to MinIO."""
    import websockets

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    buffer: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    logger.info("Connecting to websocket: %s", websocket_url)
    async with websockets.connect(websocket_url) as websocket:
        while not stop_event.is_set():
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                if batcher.should_flush(buffer, last_flush):
                    await flush_buffer(buffer, local_dir)
                    last_flush = time.monotonic()
                continue

            try:
                event = parse_message(message)
            except json.JSONDecodeError as exc:
                await asyncio.to_thread(batcher.upload_invalid_json, message, str(exc), local_dir or settings.LOCAL_BATCH_DIR)
                continue
            except ValueError as exc:
                await asyncio.to_thread(batcher.upload_invalid_json, message, str(exc), local_dir or settings.LOCAL_BATCH_DIR)
                continue

            buffer.append(event)
            if batcher.should_flush(buffer, last_flush):
                await flush_buffer(buffer, local_dir)
                last_flush = time.monotonic()

    await flush_buffer(buffer, local_dir)
    logger.info("Websocket consumer stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume websocket events and upload Parquet batches to MinIO.")
    parser.add_argument("--websocket-url", default=settings.WEBSOCKET_URL)
    parser.add_argument("--local-dir", default=str(settings.LOCAL_BATCH_DIR))
    args = parser.parse_args()

    asyncio.run(consume_events(args.websocket_url, args.local_dir))


if __name__ == "__main__":
    main()

