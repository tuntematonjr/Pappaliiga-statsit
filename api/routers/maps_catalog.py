"""Maps API endpoints - Map catalog with images and pretty names."""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from async_db import query_async

router = APIRouter()


class MapInfo(BaseModel):
    """Map catalog information."""
    map_id: str
    pretty_name: str
    image_sm: Optional[str] = None
    image_lg: Optional[str] = None


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


@router.get("/{map_id}", response_model=MapInfo)
async def get_map_info(map_id: str):
    """Get map information by map ID.
    
    Args:
        map_id: Map identifier (e.g., 'de_dust2', 'de_mirage')
    
    Returns:
        Map display information including pretty name and image URLs.
    
    Raises:
        HTTPException: 404 if map not found in catalog.
    """
    rows = await query_async(
        "SELECT map_id, pretty_name, image_sm, image_lg FROM maps_catalog WHERE map_id = :map_id",
        {"map_id": map_id}
    )
    
    if not rows:
        raise HTTPException(status_code=404, detail=f"Map '{map_id}' not found in catalog")
    
    r = rows[0]
    return {
        "map_id": r["map_id"],
        "pretty_name": r["pretty_name"],
        "image_sm": r.get("image_sm"),
        "image_lg": r.get("image_lg"),
    }
