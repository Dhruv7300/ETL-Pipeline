import json
import logging
import uuid
import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class PostgresWriter:
    def __init__(self, config, retry_attempts=3):
        self.config = config
        self.retry_attempts = retry_attempts
        self.pool = None

    def connect(self):
        self.pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=5,
            host=self.config.db_host,
            port=self.config.db_port,
            dbname=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password,
        )
        logger.info("Database connection pool created")

    def insert(self, table, data):
        if not data.get("id"):
            data["id"] = str(uuid.uuid4())

        columns = list(data.keys())
        values = [self._serialize_value(v) for v in data.values()]
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({", ".join(columns)})
            VALUES ({", ".join(placeholders)})
            ON CONFLICT (id) DO UPDATE SET
            {", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "id"])}
        """

        return self._execute_with_retry(query, values)

    def update(self, table, id_value, data):
        columns = [f"{k} = %s" for k in data.keys()]
        values = [self._serialize_value(v) for v in data.values()]
        values.append(id_value)

        query = f"UPDATE {table} SET {', '.join(columns)} WHERE id = %s"
        return self._execute_with_retry(query, values)

    def write_to_dlq(self, raw_event, error_message):
        query = """
            INSERT INTO dead_letter_queue (raw_event, error_message)
            VALUES (%s, %s)
        """
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor()
            cursor.execute(query, (Json(raw_event), error_message))
            conn.commit()
            cursor.close()
            logger.warning(f"Event written to DLQ: {error_message}")
            return True
        except Exception as e:
            logger.error(f"Failed to write to DLQ: {e}")
            return False
        finally:
            self.pool.putconn(conn)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _execute_with_retry(self, query, values):
        conn = self.pool.getconn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def _serialize_value(self, value):
        if isinstance(value, (dict, list)):
            return Json(value)
        return value

    def close(self):
        if self.pool:
            self.pool.closeall()
