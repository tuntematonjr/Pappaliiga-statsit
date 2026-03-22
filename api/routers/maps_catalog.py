"""Maps API endpoints - Map catalog with images and pretty names."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from db_async import query_async

router = APIRouter()


class MapInfo(BaseModel):
    """Map catalog information."""
    map_id: str
    pretty_name: str
    image_sm: Optional[str] = None
    image_lg: Optional[str] = None


@router.get("", response_model=List[MapInfo])
@router.get("/", response_model=List[MapInfo])
async def list_maps():
    """Get all maps with images and pretty names from the catalog.
    
    Returns:
        List of maps with display information for UI rendering.
    """
    rows = await query_async(
        "SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog ORDER BY pretty_name"
    )
    
    return [
        {
            "map_id": r["map_id"],
            "pretty_name": r["pretty_name"],
            "image_sm": r.get("image_sm"),
            "image_lg": r.get("image_lg"),
        }
        for r in rows
    ]

