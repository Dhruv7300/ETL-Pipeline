from processing.spark_job import REQUIRED_FIELDS, TABLE_BY_EVENT_TYPE, validate_event_dict, validate_event_json


BASE = {
    "event_id": "evt-1",
    "event_ts": "2026-04-20T10:00:00Z",
}


VALID_PAYLOADS = {
    "new_vehicle": {
        "vehicle_id": "veh-1",
        "type": "sedan",
        "model": "Toyota Camry",
        "license_plate": "DL01AB1234",
    },
    "new_service": {
        "service_id": "svc-1",
        "name": "Premier",
        "service_type": "premium",
    },
    "new_driver": {
        "driver_id": "driver-1",
        "name": "Asha",
        "license_number": "LIC123",
        "rating": 4.8,
        "vehicle_id": "veh-1",
    },
    "new_rider": {
        "rider_id": "rider-1",
        "name": "Ravi",
        "email": "ravi@example.com",
        "phone": "+919999999999",
    },
    "ride_requested": {
        "ride_id": "ride-1",
        "rider_id": "rider-1",
        "driver_id": "driver-1",
        "service_id": "svc-1",
        "pickup": {"latitude": 28.6139, "longitude": 77.209},
        "dropoff": {"latitude": 28.5355, "longitude": 77.391},
    },
    "ride_started": {
        "ride_id": "ride-1",
        "driver_id": "driver-1",
        "started_at": "2026-04-20T10:05:00Z",
    },
    "trip_completed": {
        "ride_id": "ride-1",
        "distance_km": 18.5,
        "fare": 420.0,
        "completed_at": "2026-04-20T10:45:00Z",
    },
    "payment_completed": {
        "payment_id": "pay-1",
        "ride_id": "ride-1",
        "rider_id": "rider-1",
        "amount": 420.0,
        "payment_method": "card",
    },
    "fare_changed": {
        "service_id": "svc-1",
        "old_fare": 12.0,
        "new_fare": 14.0,
    },
}


def make_event(event_type):
    return {**BASE, "event_type": event_type, **VALID_PAYLOADS[event_type]}


def test_all_supported_event_types_are_valid():
    assert set(REQUIRED_FIELDS) == set(VALID_PAYLOADS)
    assert set(REQUIRED_FIELDS) == set(TABLE_BY_EVENT_TYPE)

    for event_type in REQUIRED_FIELDS:
        is_valid, reason = validate_event_dict(make_event(event_type))
        assert is_valid, reason


def test_missing_required_field_fails_validation():
    event = make_event("new_vehicle")
    event.pop("license_plate")

    is_valid, reason = validate_event_dict(event)

    assert not is_valid
    assert "license_plate" in reason


def test_bad_location_fails_validation():
    event = make_event("ride_requested")
    event["pickup"] = {"latitude": 200, "longitude": 77.209}

    is_valid, reason = validate_event_dict(event)

    assert not is_valid
    assert "pickup" in reason


def test_validate_event_json_returns_target_table():
    result = validate_event_json(
        '{"event_id":"evt-1","event_type":"payment_completed","event_ts":"2026-04-20T10:00:00Z",'
        '"payment_id":"pay-1","ride_id":"ride-1","rider_id":"rider-1","amount":100,"payment_method":"upi"}'
    )

    assert result["is_valid"] is True
    assert result["target_table"] == "payments"

