import logging
import sys
import signal
from config.settings import settings
from ingestion.websocket_client import WebSocketClient
from transform.schema_validator import SchemaValidator
from transform.event_handlers import EventHandlers
from load.postgres_writer import PostgresWriter

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self):
        self.ws_client = None
        self.db_writer = None
        self.validator = SchemaValidator()
        self.handlers = None
        self.running = False

    def setup(self):
        logger.info("Setting up pipeline...")

        self.db_writer = PostgresWriter(settings)
        self.db_writer.connect()
        self.handlers = EventHandlers(self.db_writer)

        self.ws_client = WebSocketClient(
            settings.ws_url,
            max_retries=settings.ws_reconnect_max_attempts,
            max_delay=settings.ws_reconnect_max_delay,
        )

        self.ws_client.on_message(self.process_event)
        self.ws_client.on_error(self.handle_error)
        self.ws_client.on_disconnect(self.handle_disconnect)

    def process_event(self, event):
        logger.info(f"Received event: {event.get('event_type')}")

        is_valid, errors = self.validator.validate(event)

        if not is_valid:
            logger.warning(f"Validation failed: {errors}")
            self.db_writer.write_to_dlq(event, "; ".join(errors))
            return

        success = self.handlers.handle_event(event)

        if not success:
            logger.error(f"Failed to process event: {event.get('event_type')}")
            self.db_writer.write_to_dlq(event, "Handler failed")

    def handle_error(self, error):
        logger.error(f"Error: {error}")
        self.db_writer.write_to_dlq(error.get("raw", {}), error.get("error", "Unknown"))

    def handle_disconnect(self):
        logger.warning("WebSocket disconnected, attempting reconnect...")

    def run(self):
        self.running = True
        logger.info("Starting pipeline...")

        if self.ws_client.connect():
            logger.info("Connected to WebSocket")
            self.ws_client.listen()
        else:
            logger.error("Failed to connect to WebSocket")

    def stop(self):
        self.running = False
        logger.info("Stopping pipeline...")
        if self.ws_client:
            self.ws_client.close()
        if self.db_writer:
            self.db_writer.close()


if __name__ == "__main__":
    pipeline = ETLPipeline()

    def signal_handler(sig, frame):
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    pipeline.setup()
    pipeline.run()
