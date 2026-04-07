from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph


class PipelineState(TypedDict, total=False):
    raw_event: dict[str, Any]
    validated_event: dict[str, Any] | None
    transformed_event: dict[str, Any] | None


def ingest(state: PipelineState) -> PipelineState:
    return {"raw_event": state.get("raw_event")}


def validate(state: PipelineState) -> PipelineState:
    event = state.get("raw_event") or {}
    required_fields = ["order_id", "user_id", "amount", "timestamp"]

    if not all(field in event for field in required_fields):
        print("Validation failed: missing required fields", event)
        return {"validated_event": None}

    if event.get("amount") in (None, "invalid"):
        print("Validation failed: bad amount", event)
        return {"validated_event": None}

    return {"validated_event": event}


def transform(state: PipelineState) -> PipelineState:
    event = state.get("validated_event")
    if not event:
        return {"transformed_event": None}

    try:
        transformed = {
            "order_id": int(event["order_id"]),
            "user_id": int(event["user_id"]),
            "amount": float(event["amount"]),
            "timestamp": str(event["timestamp"]),
        }
        return {"transformed_event": transformed}
    except Exception as exc:
        print("Transform failed:", exc, event)
        return {"transformed_event": None}


def load(state: PipelineState) -> PipelineState:
    event = state.get("transformed_event")
    if event is None:
        print("Load skipped: event was invalid")
    else:
        print("Loaded:", event)
    return {}


graph = StateGraph(PipelineState)
graph.add_node("ingest", ingest)
graph.add_node("validate", validate)
graph.add_node("transform", transform)
graph.add_node("load", load)

graph.add_edge(START, "ingest")
graph.add_edge("ingest", "validate")
graph.add_edge("validate", "transform")
graph.add_edge("transform", "load")
graph.add_edge("load", END)

app = graph.compile()
