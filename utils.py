import random
from datetime import datetime, timezone


def generate_event() -> dict:
    """Create a fake order event with occasional bad amount values."""
    amount_choices = [
        round(random.uniform(10, 500), 2),
        "invalid",
        None,
    ]

    # Most events are valid; some contain bad data.
    amount = random.choices(amount_choices, weights=[8, 1, 1], k=1)[0]

    return {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
