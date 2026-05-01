import logging
from typing import Dict, List, Tuple
import uuid

logger = logging.getLogger(__name__)

EVENT_SCHEMAS = {
    "new_vehicle": {
        "required": ["vehicle_id", "vehicle_type", "model", "license_plate"],
        "optional": ["metadata"],
    },
    "new_service": {
        "required": ["service_id", "service_name", "service_type"],
        "optional": ["base_fare", "per_km_rate", "metadata"],
    },
    "new_driver": {
        "required": ["driver_id", "name", "license_number"],
        "optional": ["rating", "vehicle_id", "metadata"],
    },
    "new_rider": {
        "required": ["rider_id", "name", "email"],
        "optional": ["phone", "rating", "metadata"],
    },
    "ride_requested": {
        "required": [
            "ride_id",
            "rider_id",
            "driver_id",
            "service_id",
            "pickup",
            "dropoff",
        ],
        "optional": ["metadata"],
    },
    "ride_started": {
        "required": ["ride_id", "driver_id", "started_at"],
        "optional": [],
    },
    "trip_completed": {
        "required": ["ride_id", "distance_km", "fare", "completed_at"],
        "optional": ["status", "metadata"],
    },
    "payment_completed": {
        "required": ["payment_id", "ride_id", "rider_id", "amount"],
        "optional": ["payment_method", "currency", "transaction_id", "metadata"],
    },
    "fare_changed": {
        "required": ["service_id", "old_fare", "new_fare", "effective_from"],
        "optional": [],
    },
}


class SchemaValidator:
    def validate(self, event: Dict) -> Tuple[bool, List[str]]:
        event_type = event.get("event_type")

        if not event_type:
            return False, ["Missing 'event_type' field"]

        if event_type not in EVENT_SCHEMAS:
            return False, [f"Unknown event_type: {event_type}"]

        schema = EVENT_SCHEMAS[event_type]
        errors = []

        for field in schema["required"]:
            if field not in event or event[field] is None:
                errors.append(f"Missing required field: {field}")

        for field in [
            "vehicle_id",
            "service_id",
            "driver_id",
            "rider_id",
            "ride_id",
            "payment_id",
        ]:
            if field in event and event[field]:
                try:
                    uuid.UUID(str(event[field]))
                except (ValueError, TypeError):
                    errors.append(f"Invalid UUID format: {field}")

        return len(errors) == 0, errors

    def get_event_type(self, event: Dict) -> str:
        return event.get("event_type", "unknown")
