"""Scope (District -> Block -> School) endpoints backed by real Mongo data.

These endpoints are used to populate global drilldown dropdowns in the UI.
They intentionally derive values from analytics collections bundled with the package
(e.g. aadhaar_analytics) rather than the optional 'schools' collection.
"""

from fastapi import APIRouter, Query
from typing import List, Optional


router = APIRouter(prefix="/scope", tags=["Scope"])

# Database will be injected
db = None


def init_db(database):
    global db
    db = database


def _source_collection():
    """Pick a reliable collection that includes district/block/school fields."""
    return db.aadhaar_analytics


@router.get("/districts")
async def list_districts() -> List[dict]:
    col = _source_collection()
    pipeline = [
        {
            "$group": {
                "_id": {"district_code": "$district_code", "district_name": "$district_name"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "district_code": "$_id.district_code",
                "district_name": "$_id.district_name",
            }
        },
        {"$match": {"district_code": {"$nin": [None, ""]}}},
        {"$sort": {"district_name": 1}},
    ]
    rows = await col.aggregate(pipeline).to_list(length=1000)
    # Defensive: filter empty names too
    return [r for r in rows if r.get("district_code") and r.get("district_name")]


@router.get("/districts/{district_code}/blocks")
async def list_blocks(district_code: str) -> List[dict]:
    col = _source_collection()
    pipeline = [
        {"$match": {"district_code": district_code}},
        {
            "$group": {
                "_id": {"block_code": "$block_code", "block_name": "$block_name"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "block_code": "$_id.block_code",
                "block_name": "$_id.block_name",
            }
        },
        {"$match": {"block_code": {"$nin": [None, ""]}}},
        {"$sort": {"block_name": 1}},
    ]
    rows = await col.aggregate(pipeline).to_list(length=5000)
    return [r for r in rows if r.get("block_code") and r.get("block_name")]


@router.get("/blocks/{block_code}/schools")
async def list_schools(
    block_code: str,
    limit: int = Query(500, ge=1, le=5000),
    q: Optional[str] = Query(None, description="Optional case-insensitive search by school name"),
) -> List[dict]:
    col = _source_collection()
    match = {"block_code": block_code}
    if q:
        match["school_name"] = {"$regex": q, "$options": "i"}

    pipeline = [
        {"$match": match},
        {
            "$group": {
                "_id": {"udise_code": "$udise_code", "school_name": "$school_name"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "udise_code": "$_id.udise_code",
                "school_name": "$_id.school_name",
            }
        },
        {"$match": {"udise_code": {"$nin": [None, ""]}}},
        {"$sort": {"school_name": 1}},
        {"$limit": limit},
    ]
    rows = await col.aggregate(pipeline).to_list(length=limit)
    return [r for r in rows if r.get("udise_code") and r.get("school_name")]


