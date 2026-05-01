import asyncio
import json
import random
import uuid
from datetime import datetime
from websockets.server import serve


def generate_ride_chain():
    """Generate a complete chain of related events for one ride"""
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
            "vehicle_type": random.choice(["sedan", "suv", "compact", "premium"]),
            "model": random.choice(["Toyota Camry", "Honda Civic", "Tesla Model 3"]),
            "license_plate": f"{random.choice('ABCDEF')}{random.randint(1000, 9999)}",
        },
        {
            "event_type": "new_service",
            "service_id": service_id,
            "service_name": random.choice(["UberX", "Uber Comfort", "Uber Black"]),
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
            "pickup": {
                "lat": round(random.uniform(37.0, 38.0), 6),
                "lng": round(random.uniform(-123.0, -122.0), 6),
            },
            "dropoff": {
                "lat": round(random.uniform(37.0, 38.0), 6),
                "lng": round(random.uniform(-123.0, -122.0), 6),
            },
        },
        {
            "event_type": "ride_started",
            "ride_id": ride_id,
            "driver_id": driver_id,
            "started_at": datetime.now().isoformat(),
        },
        {
            "event_type": "trip_completed",
            "ride_id": ride_id,
            "distance_km": round(random.uniform(1, 50), 2),
            "fare": round(random.uniform(10, 100), 2),
            "completed_at": datetime.now().isoformat(),
        },
        {
            "event_type": "payment_completed",
            "payment_id": payment_id,
            "ride_id": ride_id,
            "rider_id": rider_id,
            "amount": round(random.uniform(10, 100), 2),
            "payment_method": random.choice(["card", "apple_pay", "google_pay"]),
            "transaction_id": f"TXN{uuid.uuid4().hex[:8].upper()}",
            "paid_at": datetime.now().isoformat(),
        },
    ]
    return chain


def generate_simple_events():
    """Generate standalone events that don't require FK (services, vehicles standalone)"""
    return [
        {
            "event_type": "new_service",
            "service_id": str(uuid.uuid4()),
            "service_name": random.choice(["UberX", "Uber Comfort", "Uber Black"]),
            "service_type": "ride",
            "base_fare": round(random.uniform(5, 15), 2),
            "per_km_rate": round(random.uniform(1, 3), 2),
        },
        {
            "event_type": "new_vehicle",
            "vehicle_id": str(uuid.uuid4()),
            "vehicle_type": random.choice(["sedan", "suv", "compact", "premium"]),
            "model": random.choice(["Toyota Camry", "Honda Civic", "Tesla Model 3"]),
            "license_plate": f"{random.choice('ABCDEF')}{random.randint(1000, 9999)}",
        },
        {
            "event_type": "fare_changed",
            "service_id": str(uuid.uuid4()),
            "old_fare": round(random.uniform(5, 15), 2),
            "new_fare": round(random.uniform(5, 15), 2),
            "effective_from": datetime.now().isoformat(),
        },
    ]


INVALID_EVENTS = [
    {"event_type": "trip_completed", "fare": "not_a_number"},
    {"event_type": "unknown_event_type", "data": "test"},
    {"ride_id": str(uuid.uuid4())},
]


async def handler(websocket):
    print("Client connected")
    send_task = asyncio.create_task(send_events(websocket))
    try:
        await send_task
    except Exception as e:
        print(f"Client disconnected: {e}")
    finally:
        send_task.cancel()
        try:
            await send_task
        except asyncio.CancelledError:
            pass


async def send_events(websocket):
    while True:
        try:
            # 70% chance: generate complete ride chain (all related events)
            # 20% chance: generate standalone events
            # 10% chance: generate invalid event (for DLQ testing)

            rand = random.random()

            if rand < 0.70:
                # Generate complete ride chain
                chain = generate_ride_chain()
                for event in chain:
                    await websocket.send(json.dumps(event))
                    await asyncio.sleep(0.5)
            elif rand < 0.90:
                # Generate standalone events
                events = generate_simple_events()
                event = random.choice(events)
                await websocket.send(json.dumps(event))
            else:
                # Generate invalid event for DLQ testing
                event = random.choice(INVALID_EVENTS)
                await websocket.send(json.dumps(event))

            # Wait before next batch
            await asyncio.sleep(random.uniform(2, 5))
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error sending event: {e}")
            break


async def main():
    print("Starting mock WebSocket server on ws://localhost:8765")
    print("Generating related event chains:")
    print(
        "  - new_vehicle → new_service → new_driver → new_rider → ride_requested → ride_started → trip_completed → payment_completed"
    )
    print("With 10% chance of invalid events for DLQ testing")
    async with serve(handler, "localhost", 8765):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
