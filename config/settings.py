import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    # PostgreSQL
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "uber_etl")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    # WebSocket
    ws_url: str = os.getenv("WS_URL", "ws://localhost:8765")
    ws_reconnect_max_attempts: int = 10
    ws_reconnect_max_delay: int = 60

    # Retry settings
    db_retry_attempts: int = 3
    db_retry_delay: int = 5


settings = Settings()
