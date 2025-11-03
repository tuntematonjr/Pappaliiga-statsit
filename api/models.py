from __future__ import annotations

from pydantic import BaseModel, ConfigDict


def to_camel(string: str) -> str:
    """Convert snake_case strings to camelCase."""
    if not string:
        return string
    parts = string.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class CamelModel(BaseModel):
    """Base model that serializes using camelCase aliases."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )


class PaginationMeta(CamelModel):
    total: int
    limit: int
    offset: int
