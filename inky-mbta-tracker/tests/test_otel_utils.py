from typing import Any, cast

import pytest
from opentelemetry.trace import Span
from otel_utils import (
    add_cache_key_attributes,
    add_entity_id_attribute,
    add_event_to_span,
    sanitize_attribute_value,
)


class FakeSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, Any] = {}
        self.events: list[tuple[str, dict[str, Any]]] = []

    def is_recording(self) -> bool:
        return True

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any]) -> None:
        self.events.append((name, attributes))


def test_cache_key_attributes_use_namespace_hash_and_length() -> None:
    span = FakeSpan()

    add_cache_key_attributes(cast(Span, span), "api_cache:GET:/vehicles?x=1")

    assert span.attributes["cache.key.namespace"] == "api_cache"
    assert span.attributes["cache.key.length"] == len("api_cache:GET:/vehicles?x=1")
    assert len(span.attributes["cache.key.hash"]) == 16
    assert "api_cache:GET:/vehicles?x=1" not in span.attributes.values()


def test_entity_id_attribute_hashes_vehicle_ids_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IMT_OTEL_INCLUDE_VEHICLE_IDS", raising=False)
    span = FakeSpan()

    add_entity_id_attribute(
        cast(Span, span),
        "vehicle.id",
        "vehicle-high-cardinality",
        entity_type="vehicle",
    )

    assert "vehicle.id" not in span.attributes
    assert len(span.attributes["vehicle.id.hash"]) == 16


def test_entity_id_attribute_can_include_vehicle_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IMT_OTEL_INCLUDE_VEHICLE_IDS", "true")
    span = FakeSpan()

    add_entity_id_attribute(
        cast(Span, span), "vehicle.id", "vehicle-1", entity_type="vehicle"
    )

    assert span.attributes["vehicle.id"] == "vehicle-1"
    assert "vehicle.id.hash" not in span.attributes


def test_sanitize_attribute_value_removes_newlines_and_truncates() -> None:
    value = sanitize_attribute_value("abc\ndef\rghi", max_length=7)

    assert value == "abc def..."


def test_add_event_to_span_converts_non_otel_attribute_values() -> None:
    span = FakeSpan()

    add_event_to_span(
        cast(Span, span),
        "event",
        {
            "count": 1,
            "items": ["a", 2],
            "payload": {"nested": True},
        },
    )

    assert span.events == [
        (
            "event",
            {
                "count": 1,
                "items": ["a", "2"],
                "payload": "{'nested': True}",
            },
        )
    ]
