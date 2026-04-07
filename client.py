import asyncio
import json

from websockets import connect

from pipeline import app


async def consume_events():
    uri = "ws://localhost:8765"
    async with connect(uri) as websocket:
        print(f"Connected to {uri}")
        while True:
            try:
                message = await websocket.recv()
                event = json.loads(message)
                app.invoke({"raw_event": event})
            except Exception as exc:
                print("Client error:", exc)


if __name__ == "__main__":
    asyncio.run(consume_events())
