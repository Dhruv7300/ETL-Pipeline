"""Mock websocket server that emits Uber-like lifecycle events."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import uuid
from datetime import datetime, timezone
from typing import Any

INVALID_EVENTS = [
    {"event_type": "trip_completed", "fare": "not_a_number"},
    {"event_type": "unknown_event_type", "data": "test"},
    {"ride_id": str(uuid.uuid4())},
]


def generate_event_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def with_common_fields(event: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(event)
    enriched.setdefault("event_id", generate_event_id())
    enriched.setdefault("event_ts", now_iso())
    return enriched


def random_location() -> dict[str, float]:
    return {
        "latitude": round(random.uniform(37.0, 38.0), 6),
        "longitude": round(random.uniform(-123.0, -122.0), 6),
    }


def generate_ride_chain() -> list[dict[str, Any]]:
    """Generate a complete chain of related events for one ride."""
    ride_id = str(uuid.uuid4())
    rider_id = str(uuid.uuid4())
    driver_id = str(uuid.uuid4())
    vehicle_id = str(uuid.uuid4())
    service_id = str(uuid.uuid4())
    payment_id = str(uuid.uuid4())

    chain = [
        {
            "event_type": "new_vehicle",
            "vehicle_id": vehicle_id,
            "type": random.choice(["sedan", "suv", "compact", "premium"]),
            "model": random.choice(["Toyota Camry", "Honda Civic", "Tesla Model 3"]),
            "license_plate": f"{random.choice('ABCDEF')}{random.randint(1000, 9999)}",
        },
        {
            "event_type": "new_service",
            "service_id": service_id,
            "name": random.choice(["UberX", "Uber Comfort", "Uber Black"]),
            "service_type": "ride",
            "base_fare": round(random.uniform(5, 15), 2),
            "per_km_rate": round(random.uniform(1, 3), 2),
        },
        {
            "event_type": "new_driver",
            "driver_id": driver_id,
            "name": f"Driver {random.randint(1, 1000)}",
            "license_number": f"DL{random.randint(100000, 999999)}",
            "rating": round(random.uniform(4.0, 5.0), 2),
            "vehicle_id": vehicle_id,
        },
        {
            "event_type": "new_rider",
            "rider_id": rider_id,
            "name": f"Rider {random.randint(1, 1000)}",
            "email": f"rider{random.randint(1, 1000)}@example.com",
            "phone": f"+1{random.randint(1000000000, 9999999999)}",
        },
        {
            "event_type": "ride_requested",
            "ride_id": ride_id,
            "rider_id": rider_id,
            "driver_id": driver_id,
            "service_id": service_id,
            "pickup": random_location(),
            "dropoff": random_location(),
        },
        {
            "event_type": "ride_started",
            "ride_id": ride_id,
            "driver_id": driver_id,
            "started_at": now_iso(),
        },
        {
            "event_type": "trip_completed",
            "ride_id": ride_id,
            "distance_km": round(random.uniform(1, 50), 2),
            "fare": round(random.uniform(10, 100), 2),
            "completed_at": now_iso(),
        },
        {
            "event_type": "payment_completed",
            "payment_id": payment_id,
            "ride_id": ride_id,
            "rider_id": rider_id,
            "amount": round(random.uniform(10, 100), 2),
            "payment_method": random.choice(["card", "apple_pay", "google_pay"]),
            "transaction_id": f"TXN{uuid.uuid4().hex[:8].upper()}",
            "paid_at": now_iso(),
        },
    ]
    return [with_common_fields(event) for event in chain]


def generate_simple_events() -> list[dict[str, Any]]:
    """Generate standalone events that do not need a full ride chain."""
    events = [
        {
            "event_type": "new_service",
            "service_id": str(uuid.uuid4()),
            "name": random.choice(["UberX", "Uber Comfort", "Uber Black"]),
            "service_type": "ride",
            "base_fare": round(random.uniform(5, 15), 2),
            "per_km_rate": round(random.uniform(1, 3), 2),
        },
        {
            "event_type": "new_vehicle",
            "vehicle_id": str(uuid.uuid4()),
            "type": random.choice(["sedan", "suv", "compact", "premium"]),
            "model": random.choice(["Toyota Camry", "Honda Civic", "Tesla Model 3"]),
            "license_plate": f"{random.choice('ABCDEF')}{random.randint(1000, 9999)}",
        },
        {
            "event_type": "fare_changed",
            "service_id": str(uuid.uuid4()),
            "old_fare": round(random.uniform(5, 15), 2),
            "new_fare": round(random.uniform(5, 15), 2),
            "effective_from": now_iso(),
        },
    ]
    return [with_common_fields(event) for event in events]


async def send_json(websocket, event: dict[str, Any]) -> None:
    print(f"Sending event: {json.dumps(event, sort_keys=True)}")
    await websocket.send(json.dumps(event))


async def send_events(
    websocket,
    chain_delay: float = 0.5,
    batch_delay_min: float = 2.0,
    batch_delay_max: float = 5.0,
) -> None:
    while True:
        try:
            rand = random.random()

            if rand < 0.70:
                for event in generate_ride_chain():
                    await send_json(websocket, event)
                    await asyncio.sleep(chain_delay)
            elif rand < 0.90:
                await send_json(websocket, random.choice(generate_simple_events()))
            else:
                await send_json(websocket, random.choice(INVALID_EVENTS))

            await asyncio.sleep(random.uniform(batch_delay_min, batch_delay_max))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"Error sending event: {exc}")
            break


async def handler(
    websocket,
    chain_delay: float = 0.5,
    batch_delay_min: float = 2.0,
    batch_delay_max: float = 5.0,
) -> None:
    print("Client connected")
    send_task = asyncio.create_task(
        send_events(
            websocket,
            chain_delay=chain_delay,
            batch_delay_min=batch_delay_min,
            batch_delay_max=batch_delay_max,
        )
    )
    try:
        await send_task
    except Exception as exc:
        print(f"Client disconnected: {exc}")
    finally:
        send_task.cancel()
        try:
            await send_task
        except asyncio.CancelledError:
            pass


async def run_server(host: str, port: int, chain_delay: float, batch_delay_min: float, batch_delay_max: float) -> None:
    from websockets.server import serve

    print(f"Starting mock WebSocket server on ws://{host}:{port}")
    print("Generating related event chains:")
    print("  new_vehicle -> new_service -> new_driver -> new_rider -> ride_requested -> ride_started -> trip_completed -> payment_completed")
    print("With about 10% invalid events for DLQ testing")

    async def configured_handler(websocket):
        await handler(
            websocket,
            chain_delay=chain_delay,
            batch_delay_min=batch_delay_min,
            batch_delay_max=batch_delay_max,
        )

    async with serve(configured_handler, host, port):
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local mock websocket event source.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--chain-delay", type=float, default=0.5)
    parser.add_argument("--batch-delay-min", type=float, default=2.0)
    parser.add_argument("--batch-delay-max", type=float, default=5.0)
    args = parser.parse_args()

    if args.batch_delay_min > args.batch_delay_max:
        raise ValueError("--batch-delay-min cannot be greater than --batch-delay-max")

    asyncio.run(
        run_server(
            host=args.host,
            port=args.port,
            chain_delay=args.chain_delay,
            batch_delay_min=args.batch_delay_min,
            batch_delay_max=args.batch_delay_max,
        )
    )


if __name__ == "__main__":
    main()
