import logging
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


class EventHandlers:
    def __init__(self, db_writer):
        self.db_writer = db_writer

    def handle_event(self, event):
        event_type = event.get("event_type")

        handlers = {
            "new_vehicle": self.handle_new_vehicle,
            "new_service": self.handle_new_service,
            "new_driver": self.handle_new_driver,
            "new_rider": self.handle_new_rider,
            "ride_requested": self.handle_ride_requested,
            "ride_started": self.handle_ride_started,
            "trip_completed": self.handle_trip_completed,
            "payment_completed": self.handle_payment_completed,
            "fare_changed": self.handle_fare_changed,
        }

        handler = handlers.get(event_type)
        if handler:
            return handler(event)

        logger.warning(f"No handler for event type: {event_type}")
        return False

    def handle_new_vehicle(self, event):
        data = {
            "id": event.get("vehicle_id"),
            "vehicle_type": event.get("vehicle_type"),
            "model": event.get("model"),
            "license_plate": event.get("license_plate"),
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("vehicles", data)

    def handle_new_service(self, event):
        data = {
            "id": event.get("service_id"),
            "service_name": event.get("service_name"),
            "service_type": event.get("service_type"),
            "base_fare": event.get("base_fare"),
            "per_km_rate": event.get("per_km_rate"),
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("services", data)

    def handle_new_driver(self, event):
        data = {
            "id": event.get("driver_id"),
            "name": event.get("name"),
            "license_number": event.get("license_number"),
            "rating": event.get("rating"),
            "vehicle_id": event.get("vehicle_id"),
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("drivers", data)

    def handle_new_rider(self, event):
        data = {
            "id": event.get("rider_id"),
            "name": event.get("name"),
            "email": event.get("email"),
            "phone": event.get("phone"),
            "rating": event.get("rating"),
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("riders", data)

    def handle_ride_requested(self, event):
        data = {
            "id": event.get("ride_id"),
            "rider_id": event.get("rider_id"),
            "driver_id": event.get("driver_id"),
            "service_id": event.get("service_id"),
            "pickup_location": event.get("pickup"),
            "dropoff_location": event.get("dropoff"),
            "status": "requested",
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("rides", data)

    def handle_ride_started(self, event):
        data = {
            "status": "started",
            "started_at": event.get("started_at"),
        }
        return self.db_writer.update("rides", event.get("ride_id"), data)

    def handle_trip_completed(self, event):
        data = {
            "status": event.get("status", "completed"),
            "distance_km": event.get("distance_km"),
            "fare": event.get("fare"),
            "completed_at": event.get("completed_at"),
        }
        return self.db_writer.update("rides", event.get("ride_id"), data)

    def handle_payment_completed(self, event):
        data = {
            "id": event.get("payment_id"),
            "ride_id": event.get("ride_id"),
            "rider_id": event.get("rider_id"),
            "amount": event.get("amount"),
            "currency": event.get("currency", "USD"),
            "payment_method": event.get("payment_method"),
            "payment_status": "completed",
            "transaction_id": event.get("transaction_id"),
            "paid_at": event.get("paid_at"),
            "metadata": event.get("metadata", {}),
        }
        return self.db_writer.insert("payments", data)

    def handle_fare_changed(self, event):
        data = {
            "service_id": event.get("service_id"),
            "old_fare": event.get("old_fare"),
            "new_fare": event.get("new_fare"),
            "effective_from": event.get("effective_from"),
        }
        return self.db_writer.insert("fare_changes", data)
