import asyncio
import json
import random

from websockets import serve

from utils import generate_event


async def stream_events(websocket):
    while True:
        event = generate_event()
        await websocket.send(json.dumps(event))
        print("Sent:", event)
        await asyncio.sleep(random.uniform(1, 2))


async def main():
    host = "localhost"
    port = 8765
    async with serve(stream_events, host, port):
        print(f"WebSocket server running at ws://{host}:{port}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
