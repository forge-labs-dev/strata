"""Shared JSON-serializable type aliases."""

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | dict[str, JsonValue] | list[JsonValue]
type JsonObject = dict[str, JsonValue]
