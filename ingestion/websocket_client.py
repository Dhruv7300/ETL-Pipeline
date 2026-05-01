import json
import logging
import time
from websocket import create_connection, WebSocketException

logger = logging.getLogger(__name__)


class WebSocketClient:
    def __init__(self, url, max_retries=10, max_delay=60):
        self.url = url
        self.max_retries = max_retries
        self.max_delay = max_delay
        self.ws = None
        self.message_callback = None
        self.error_callback = None
        self.disconnect_callback = None

    def connect(self):
        attempt = 1
        delay = 1

        while attempt <= self.max_retries:
            try:
                self.ws = create_connection(self.url)
                logger.info(f"Connected to {self.url}")
                return True
            except WebSocketException as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                time.sleep(delay)
                delay = min(delay * 2, self.max_delay)
                attempt += 1

        logger.error(f"Max retries ({self.max_retries}) reached")
        return False

    def on_message(self, callback):
        self.message_callback = callback

    def on_error(self, callback):
        self.error_callback = callback

    def on_disconnect(self, callback):
        self.disconnect_callback = callback

    def listen(self):
        if not self.ws:
            logger.error("No connection established")
            return

        while True:
            try:
                message = self.ws.recv()
                if message and self.message_callback:
                    try:
                        event = json.loads(message)
                        self.message_callback(event)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON: {e}")
                        if self.error_callback:
                            self.error_callback({"raw": message, "error": str(e)})
            except WebSocketException as e:
                logger.error(f"WebSocket error: {e}")
                if self.disconnect_callback:
                    self.disconnect_callback()
                # Auto-reconnect
                if self.connect():
                    continue
                break

    def send(self, message):
        if self.ws:
            self.ws.send(json.dumps(message))

    def close(self):
        if self.ws:
            self.ws.close()
