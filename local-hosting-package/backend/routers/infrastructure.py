"""Infrastructure & Water Safety Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import httpx
import logging
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/infrastructure", tags=["Infrastructure"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_infrastructure_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Infrastructure & Water Safety Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                # Water metrics
                "tap_water_yes": {"$sum": {"$cond": [{"$gt": ["$tap_water", 0]}, 1, 0]}},
                "tap_water_no": {"$sum": {"$cond": [{"$eq": ["$tap_water", 0]}, 1, 0]}},
                "purification_yes": {"$sum": {"$cond": [{"$gt": ["$water_purifier", 0]}, 1, 0]}},
                "purification_no": {"$sum": {"$cond": [{"$eq": ["$water_purifier", 0]}, 1, 0]}},
                "purification_non_func": {"$sum": {"$cond": [{"$eq": ["$water_purification", "non_functional"]}, 1, 0]}},
                "water_testing_yes": {"$sum": {"$cond": [{"$gt": ["$water_quality_tested", 0]}, 1, 0]}},
                "water_testing_no": {"$sum": {"$cond": [{"$eq": ["$water_quality_tested", 0]}, 1, 0]}},
                "rwh_yes": {"$sum": {"$cond": [{"$gt": ["$rain_water_harvesting", 0]}, 1, 0]}},
                "rwh_no": {"$sum": {"$cond": [{"$eq": ["$rain_water_harvesting", 0]}, 1, 0]}},
                "rwh_non_func": {"$sum": {"$cond": [{"$eq": ["$rainwater_harvesting", "non_functional"]}, 1, 0]}},
                # Hygiene metrics - dustbin fields are integers (1=yes/all, 2=some, 3=no)
                "classroom_dustbin_all": {"$sum": {"$cond": [{"$eq": ["$classroom_dustbin", 1]}, 1, 0]}},
                "classroom_dustbin_some": {"$sum": {"$cond": [{"$eq": ["$classroom_dustbin", 2]}, 1, 0]}},
                "classroom_dustbin_no": {"$sum": {"$cond": [{"$eq": ["$classroom_dustbin", 3]}, 1, 0]}},
                "toilet_dustbin_yes": {"$sum": {"$cond": [{"$eq": ["$toilet_dustbin", 1]}, 1, 0]}},
                "kitchen_dustbin_yes": {"$sum": {"$cond": [{"$eq": ["$kitchen_dustbin", 1]}, 1, 0]}},
                # MDM metrics
                "kitchen_shed_yes": {"$sum": {"$cond": [{"$gt": ["$kitchen_shed", 0]}, 1, 0]}},
                "kitchen_garden_yes": {"$sum": {"$cond": [{"$gt": ["$kitchen_garden", 0]}, 1, 0]}},
                # Health metrics
                "medical_checkup_yes": {"$sum": {"$cond": [{"$gt": ["$medical_checkup", 0]}, 1, 0]}},
                "health_record_yes": {"$sum": {"$cond": [{"$eq": ["$health_record", "yes"]}, 1, 0]}},
                "first_aid_yes": {"$sum": {"$cond": [{"$gt": ["$first_aid", 0]}, 1, 0]}},
                "life_saving_yes": {"$sum": {"$cond": [{"$eq": ["$life_saving", "yes"]}, 1, 0]}},
                "thermal_yes": {"$sum": {"$cond": [{"$eq": ["$thermal_screening", "yes"]}, 1, 0]}},
                # Inclusion metrics
                "ramp_yes": {"$sum": {"$cond": [{"$gt": ["$ramp_available", 0]}, 1, 0]}},
                "special_educator_dedicated": {"$sum": {"$cond": [{"$eq": ["$special_educator", "dedicated"]}, 1, 0]}},
                "special_educator_cluster": {"$sum": {"$cond": [{"$eq": ["$special_educator", "cluster"]}, 1, 0]}},
                "special_educator_no": {"$sum": {"$cond": [{"$eq": ["$special_educator", "no"]}, 1, 0]}},
                # Academic metrics
                "library_yes": {"$sum": {"$cond": [{"$gt": ["$library_available", 0]}, 1, 0]}},
                "total_books": {"$sum": "$library_books"},
                "furniture_all": {"$sum": {"$cond": [{"$eq": ["$furniture", "all"]}, 1, 0]}},
                "furniture_partial": {"$sum": {"$cond": [{"$eq": ["$furniture", "partial"]}, 1, 0]}},
                "furniture_no": {"$sum": {"$cond": [{"$eq": ["$furniture", "no"]}, 1, 0]}},
                "playground_yes": {"$sum": {"$cond": [{"$gt": ["$playground", 0]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "tap_water_coverage": 0.0}
    
    data = result[0]
    total = data.get("total_schools", 0) or 1
    
    # Calculate percentages
    tap_water_pct = round((data.get("tap_water_yes", 0) / total) * 100, 1)
    purification_pct = round((data.get("purification_yes", 0) / total) * 100, 1)
    purification_non_func_pct = round((data.get("purification_non_func", 0) / total) * 100, 1)
    water_testing_pct = round((data.get("water_testing_yes", 0) / total) * 100, 1)
    rwh_pct = round((data.get("rwh_yes", 0) / total) * 100, 1)
    
    # Composite Water Safety Index
    water_safety_index = round((tap_water_pct + purification_pct + water_testing_pct) / 3, 1)
    
    classroom_hygiene_pct = round((data.get("classroom_dustbin_all", 0) / total) * 100, 1)
    toilet_hygiene_pct = round((data.get("toilet_dustbin_yes", 0) / total) * 100, 1)
    kitchen_hygiene_pct = round((data.get("kitchen_dustbin_yes", 0) / total) * 100, 1)
    
    kitchen_shed_pct = round((data.get("kitchen_shed_yes", 0) / total) * 100, 1)
    kitchen_garden_pct = round((data.get("kitchen_garden_yes", 0) / total) * 100, 1)
    
    medical_checkup_pct = round((data.get("medical_checkup_yes", 0) / total) * 100, 1)
    health_record_pct = round((data.get("health_record_yes", 0) / total) * 100, 1)
    first_aid_pct = round((data.get("first_aid_yes", 0) / total) * 100, 1)
    life_saving_pct = round((data.get("life_saving_yes", 0) / total) * 100, 1)
    thermal_pct = round((data.get("thermal_yes", 0) / total) * 100, 1)
    
    ramp_pct = round((data.get("ramp_yes", 0) / total) * 100, 1)
    special_educator_dedicated_pct = round((data.get("special_educator_dedicated", 0) / total) * 100, 1)
    special_educator_cluster_pct = round((data.get("special_educator_cluster", 0) / total) * 100, 1)
    inclusion_gap_pct = round((data.get("special_educator_no", 0) / total) * 100, 1)
    
    library_pct = round((data.get("library_yes", 0) / total) * 100, 1)
    avg_books = round(data.get("total_books", 0) / total, 0)
    furniture_pct = round((data.get("furniture_all", 0) / total) * 100, 1)
    playground_pct = round((data.get("playground_yes", 0) / total) * 100, 1)
    
    # Composite Compliance Score
    compliance_score = round((tap_water_pct + purification_pct + water_testing_pct + 
                             kitchen_shed_pct + toilet_hygiene_pct + first_aid_pct + 
                             medical_checkup_pct + ramp_pct) / 8, 1)
    
    return {
        "total_schools": total,
        # Water
        "tap_water_yes": data.get("tap_water_yes", 0),
        "tap_water_no": data.get("tap_water_no", 0),
        "tap_water_coverage": tap_water_pct,
        "purification_yes": data.get("purification_yes", 0),
        "purification_no": data.get("purification_no", 0),
        "purification_non_func": data.get("purification_non_func", 0),
        "purification_coverage": purification_pct,
        "purification_non_func_pct": purification_non_func_pct,
        "water_testing_yes": data.get("water_testing_yes", 0),
        "water_testing_coverage": water_testing_pct,
        "rwh_yes": data.get("rwh_yes", 0),
        "rwh_coverage": rwh_pct,
        "rwh_non_func": data.get("rwh_non_func", 0),
        "water_safety_index": water_safety_index,
        # Hygiene
        "classroom_dustbin_all": data.get("classroom_dustbin_all", 0),
        "classroom_dustbin_some": data.get("classroom_dustbin_some", 0),
        "classroom_dustbin_no": data.get("classroom_dustbin_no", 0),
        "classroom_hygiene_coverage": classroom_hygiene_pct,
        "toilet_dustbin_yes": data.get("toilet_dustbin_yes", 0),
        "toilet_hygiene_coverage": toilet_hygiene_pct,
        "kitchen_dustbin_yes": data.get("kitchen_dustbin_yes", 0),
        "kitchen_hygiene_coverage": kitchen_hygiene_pct,
        # MDM
        "kitchen_shed_yes": data.get("kitchen_shed_yes", 0),
        "kitchen_shed_coverage": kitchen_shed_pct,
        "kitchen_garden_yes": data.get("kitchen_garden_yes", 0),
        "kitchen_garden_coverage": kitchen_garden_pct,
        # Health
        "medical_checkup_yes": data.get("medical_checkup_yes", 0),
        "medical_checkup_coverage": medical_checkup_pct,
        "health_record_yes": data.get("health_record_yes", 0),
        "health_record_coverage": health_record_pct,
        "first_aid_yes": data.get("first_aid_yes", 0),
        "first_aid_coverage": first_aid_pct,
        "life_saving_yes": data.get("life_saving_yes", 0),
        "life_saving_coverage": life_saving_pct,
        "thermal_yes": data.get("thermal_yes", 0),
        "thermal_coverage": thermal_pct,
        # Inclusion
        "ramp_yes": data.get("ramp_yes", 0),
        "ramp_coverage": ramp_pct,
        "special_educator_dedicated": data.get("special_educator_dedicated", 0),
        "special_educator_dedicated_pct": special_educator_dedicated_pct,
        "special_educator_cluster": data.get("special_educator_cluster", 0),
        "special_educator_cluster_pct": special_educator_cluster_pct,
        "special_educator_no": data.get("special_educator_no", 0),
        "inclusion_gap_pct": inclusion_gap_pct,
        # Academic
        "library_yes": data.get("library_yes", 0),
        "library_coverage": library_pct,
        "total_books": data.get("total_books", 0),
        "avg_books_per_school": avg_books,
        "furniture_all": data.get("furniture_all", 0),
        "furniture_coverage": furniture_pct,
        "playground_yes": data.get("playground_yes", 0),
        "playground_coverage": playground_pct,
        # Composite
        "compliance_score": compliance_score
    }


@router.get("/block-wise")
async def get_infrastructure_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise infrastructure analytics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "tap_water_yes": {"$sum": {"$cond": [{"$gt": ["$tap_water", 0]}, 1, 0]}},
                "purification_yes": {"$sum": {"$cond": [{"$gt": ["$water_purifier", 0]}, 1, 0]}},
                "water_testing_yes": {"$sum": {"$cond": [{"$gt": ["$water_quality_tested", 0]}, 1, 0]}},
                "rwh_yes": {"$sum": {"$cond": [{"$gt": ["$rain_water_harvesting", 0]}, 1, 0]}},
                "kitchen_shed_yes": {"$sum": {"$cond": [{"$gt": ["$kitchen_shed", 0]}, 1, 0]}},
                "toilet_dustbin_yes": {"$sum": {"$cond": [{"$eq": ["$toilet_dustbin", 1]}, 1, 0]}},
                "first_aid_yes": {"$sum": {"$cond": [{"$gt": ["$first_aid", 0]}, 1, 0]}},
                "medical_checkup_yes": {"$sum": {"$cond": [{"$gt": ["$medical_checkup", 0]}, 1, 0]}},
                "ramp_yes": {"$sum": {"$cond": [{"$gt": ["$ramp_available", 0]}, 1, 0]}},
                "special_educator_no": {"$sum": {"$cond": [{"$eq": ["$special_educator", "no"]}, 1, 0]}},
                "library_yes": {"$sum": {"$cond": [{"$gt": ["$library_available", 0]}, 1, 0]}},
            }
        },
        {"$sort": {"total_schools": -1}}
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    block_data = []
    for r in results:
        if not r["_id"]:
            continue
        
        total = r.get("total_schools", 0) or 1
        
        tap_pct = round((r.get("tap_water_yes", 0) / total) * 100, 1)
        purif_pct = round((r.get("purification_yes", 0) / total) * 100, 1)
        testing_pct = round((r.get("water_testing_yes", 0) / total) * 100, 1)
        rwh_pct = round((r.get("rwh_yes", 0) / total) * 100, 1)
        
        water_safety = round((tap_pct + purif_pct + testing_pct) / 3, 1)
        
        kitchen_shed_pct = round((r.get("kitchen_shed_yes", 0) / total) * 100, 1)
        toilet_pct = round((r.get("toilet_dustbin_yes", 0) / total) * 100, 1)
        first_aid_pct = round((r.get("first_aid_yes", 0) / total) * 100, 1)
        medical_pct = round((r.get("medical_checkup_yes", 0) / total) * 100, 1)
        ramp_pct = round((r.get("ramp_yes", 0) / total) * 100, 1)
        inclusion_gap = round((r.get("special_educator_no", 0) / total) * 100, 1)
        library_pct = round((r.get("library_yes", 0) / total) * 100, 1)
        
        # Risk score = count of low coverage areas
        risk_score = 0
        if tap_pct < 80: risk_score += 1
        if purif_pct < 80: risk_score += 1
        if testing_pct < 80: risk_score += 1
        if kitchen_shed_pct < 80: risk_score += 1
        if toilet_pct < 80: risk_score += 1
        
        block_data.append({
            "block_name": r["_id"],
            "block_code": r.get("block_code", ""),
            "total_schools": total,
            "tap_water_pct": tap_pct,
            "purification_pct": purif_pct,
            "water_testing_pct": testing_pct,
            "rwh_pct": rwh_pct,
            "water_safety_index": water_safety,
            "kitchen_shed_pct": kitchen_shed_pct,
            "toilet_hygiene_pct": toilet_pct,
            "first_aid_pct": first_aid_pct,
            "medical_checkup_pct": medical_pct,
            "ramp_pct": ramp_pct,
            "inclusion_gap_pct": inclusion_gap,
            "library_pct": library_pct,
            "risk_score": risk_score
        })
    
    return block_data


@router.get("/water-distribution")
async def get_water_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get water facility distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "tap_yes": {"$sum": {"$cond": [{"$gt": ["$tap_water", 0]}, 1, 0]}},
                "tap_no": {"$sum": {"$cond": [{"$eq": ["$tap_water", 0]}, 1, 0]}},
                "purif_yes": {"$sum": {"$cond": [{"$gt": ["$water_purifier", 0]}, 1, 0]}},
                "purif_no": {"$sum": {"$cond": [{"$eq": ["$water_purifier", 0]}, 1, 0]}},
                "purif_nf": {"$sum": {"$cond": [{"$eq": ["$water_purification", "non_functional"]}, 1, 0]}},
                "test_yes": {"$sum": {"$cond": [{"$gt": ["$water_quality_tested", 0]}, 1, 0]}},
                "test_no": {"$sum": {"$cond": [{"$eq": ["$water_quality_tested", 0]}, 1, 0]}},
                "rwh_yes": {"$sum": {"$cond": [{"$gt": ["$rain_water_harvesting", 0]}, 1, 0]}},
                "rwh_no": {"$sum": {"$cond": [{"$eq": ["$rain_water_harvesting", 0]}, 1, 0]}},
                "rwh_nf": {"$sum": {"$cond": [{"$eq": ["$rainwater_harvesting", "non_functional"]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"tap_water": [], "purification": [], "testing": [], "rwh": []}
    
    data = result[0]
    
    return {
        "tap_water": [
            {"name": "Available", "value": data.get("tap_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("tap_no", 0), "color": "#ef4444"}
        ],
        "purification": [
            {"name": "Functional", "value": data.get("purif_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("purif_no", 0), "color": "#ef4444"},
            {"name": "Non-Functional", "value": data.get("purif_nf", 0), "color": "#f59e0b"}
        ],
        "testing": [
            {"name": "Done", "value": data.get("test_yes", 0), "color": "#10b981"},
            {"name": "Not Done", "value": data.get("test_no", 0), "color": "#ef4444"}
        ],
        "rwh": [
            {"name": "Functional", "value": data.get("rwh_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("rwh_no", 0), "color": "#ef4444"},
            {"name": "Non-Functional", "value": data.get("rwh_nf", 0), "color": "#f59e0b"}
        ]
    }


@router.get("/hygiene-distribution")
async def get_hygiene_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get hygiene facility distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "class_yes": {"$sum": {"$cond": [{"$gt": ["$classroom_dustbin", 0]}, 1, 0]}},
                "class_no": {"$sum": {"$cond": [{"$eq": ["$classroom_dustbin", 0]}, 1, 0]}},
                "toilet_yes": {"$sum": {"$cond": [{"$gt": ["$toilet_dustbin", 0]}, 1, 0]}},
                "toilet_no": {"$sum": {"$cond": [{"$eq": ["$toilet_dustbin", 0]}, 1, 0]}},
                "kitchen_yes": {"$sum": {"$cond": [{"$gt": ["$kitchen_dustbin", 0]}, 1, 0]}},
                "kitchen_no": {"$sum": {"$cond": [{"$eq": ["$kitchen_dustbin", 0]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"classroom": [], "toilet": [], "kitchen": []}
    
    data = result[0]
    
    return {
        "classroom": [
            {"name": "Available", "value": data.get("class_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("class_no", 0), "color": "#ef4444"}
        ],
        "toilet": [
            {"name": "Available", "value": data.get("toilet_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("toilet_no", 0), "color": "#ef4444"}
        ],
        "kitchen": [
            {"name": "Available", "value": data.get("kitchen_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("kitchen_no", 0), "color": "#ef4444"}
        ]
    }


@router.get("/health-metrics")
async def get_health_metrics(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get health and safety metrics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "medical_yes": {"$sum": {"$cond": [{"$gt": ["$medical_checkup", 0]}, 1, 0]}},
                "health_record_yes": {"$sum": {"$cond": [{"$eq": ["$health_record", "yes"]}, 1, 0]}},
                "first_aid_yes": {"$sum": {"$cond": [{"$gt": ["$first_aid", 0]}, 1, 0]}},
                "life_saving_yes": {"$sum": {"$cond": [{"$eq": ["$life_saving", "yes"]}, 1, 0]}},
                "thermal_yes": {"$sum": {"$cond": [{"$eq": ["$thermal_screening", "yes"]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    total = data.get("total", 0) or 1
    
    return [
        {
            "name": "Medical Check-up",
            "count": data.get("medical_yes", 0),
            "total": total,
            "percentage": round((data.get("medical_yes", 0) / total) * 100, 1),
            "color": "#3b82f6"
        },
        {
            "name": "Health Records",
            "count": data.get("health_record_yes", 0),
            "total": total,
            "percentage": round((data.get("health_record_yes", 0) / total) * 100, 1),
            "color": "#8b5cf6"
        },
        {
            "name": "First Aid",
            "count": data.get("first_aid_yes", 0),
            "total": total,
            "percentage": round((data.get("first_aid_yes", 0) / total) * 100, 1),
            "color": "#10b981"
        },
        {
            "name": "Life-Saving Equip",
            "count": data.get("life_saving_yes", 0),
            "total": total,
            "percentage": round((data.get("life_saving_yes", 0) / total) * 100, 1),
            "color": "#f59e0b"
        },
        {
            "name": "Thermal Screening",
            "count": data.get("thermal_yes", 0),
            "total": total,
            "percentage": round((data.get("thermal_yes", 0) / total) * 100, 1),
            "color": "#06b6d4"
        }
    ]


@router.get("/inclusion-metrics")
async def get_inclusion_metrics(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get inclusion and accessibility metrics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "ramp_yes": {"$sum": {"$cond": [{"$gt": ["$ramp_available", 0]}, 1, 0]}},
                "ramp_no": {"$sum": {"$cond": [{"$eq": ["$ramp_available", 0]}, 1, 0]}},
                "sp_yes": {"$sum": {"$cond": [{"$gt": ["$special_educator", 0]}, 1, 0]}},
                "sp_no": {"$sum": {"$cond": [{"$eq": ["$special_educator", 0]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"ramp": [], "special_educator": []}
    
    data = result[0]
    
    return {
        "ramp": [
            {"name": "Available", "value": data.get("ramp_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("ramp_no", 0), "color": "#ef4444"}
        ],
        "special_educator": [
            {"name": "Available", "value": data.get("sp_yes", 0), "color": "#10b981"},
            {"name": "Not Available", "value": data.get("sp_no", 0), "color": "#ef4444"}
        ]
    }


@router.get("/high-risk-schools")
async def get_high_risk_schools(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools with highest infrastructure risk"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = [
        {"$match": scope_match} if scope_match else {"$match": {}},
        {
            "$project": {
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "block_code": 1,
                "district_code": 1,
                "district_name": 1,
                "tap_water": 1,
                "water_purification": 1,
                "water_testing": 1,
                "kitchen_shed": 1,
                "toilet_dustbin": 1,
                "risk_count": {
                    "$sum": [
                        {"$cond": [{"$eq": ["$tap_water", 0]}, 1, 0]},
                        {"$cond": [{"$ne": ["$water_purification", "yes"]}, 1, 0]},
                        {"$cond": [{"$eq": ["$water_quality_tested", 0]}, 1, 0]},
                        {"$cond": [{"$eq": ["$kitchen_shed", "no"]}, 1, 0]},
                        {"$cond": [{"$ne": ["$toilet_dustbin", "yes"]}, 1, 0]}
                    ]
                }
            }
        },
        {"$match": {"risk_count": {"$gte": 3}}},
        {"$sort": {"risk_count": -1}},
        {"$limit": 20}
    ]
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    return [{
        "udise_code": r.get("udise_code", ""),
        "school_name": r.get("school_name", ""),
        "block_name": r.get("block_name", ""),
        "block_code": r.get("block_code", ""),
        "district_code": r.get("district_code", ""),
        "district_name": r.get("district_name", ""),
        "risk_count": r.get("risk_count", 0),
        "tap_water": r.get("tap_water", ""),
        "purification": r.get("water_purification", ""),
        "testing": r.get("water_testing", ""),
        "kitchen_shed": r.get("kitchen_shed", ""),
        "toilet_dustbin": r.get("toilet_dustbin", "")
    } for r in results]


@router.get("/bottom-blocks")
async def get_bottom_blocks(
    metric: str = Query("water_safety", description="water_safety, hygiene, health"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get bottom performing blocks by metric"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_schools": {"$sum": 1},
                "tap_yes": {"$sum": {"$cond": [{"$gt": ["$tap_water", 0]}, 1, 0]}},
                "purif_yes": {"$sum": {"$cond": [{"$gt": ["$water_purifier", 0]}, 1, 0]}},
                "test_yes": {"$sum": {"$cond": [{"$gt": ["$water_quality_tested", 0]}, 1, 0]}},
                "toilet_yes": {"$sum": {"$cond": [{"$eq": ["$toilet_dustbin", "yes"]}, 1, 0]}},
                "first_aid_yes": {"$sum": {"$cond": [{"$gt": ["$first_aid", 0]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.infrastructure_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    block_scores = []
    for r in results:
        if not r["_id"]:
            continue
        
        total = r.get("total_schools", 0) or 1
        tap_pct = round((r.get("tap_yes", 0) / total) * 100, 1)
        purif_pct = round((r.get("purif_yes", 0) / total) * 100, 1)
        test_pct = round((r.get("test_yes", 0) / total) * 100, 1)
        toilet_pct = round((r.get("toilet_yes", 0) / total) * 100, 1)
        first_aid_pct = round((r.get("first_aid_yes", 0) / total) * 100, 1)
        
        water_safety = round((tap_pct + purif_pct + test_pct) / 3, 1)
        
        block_scores.append({
            "block_name": r["_id"],
            "total_schools": total,
            "water_safety_index": water_safety,
            "tap_water_pct": tap_pct,
            "purification_pct": purif_pct,
            "testing_pct": test_pct,
            "toilet_hygiene_pct": toilet_pct,
            "first_aid_pct": first_aid_pct
        })
    
    # Sort by selected metric
    if metric == "water_safety":
        block_scores.sort(key=lambda x: x["water_safety_index"])
    elif metric == "hygiene":
        block_scores.sort(key=lambda x: x["toilet_hygiene_pct"])
    else:
        block_scores.sort(key=lambda x: x["first_aid_pct"])
    
    return block_scores[:10]


@router.post("/import")
async def import_infrastructure_data(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL of the Infrastructure Excel file")
):
    """Import Infrastructure analytics data from Excel file"""
    import_id = str(uuid.uuid4())
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            response = await http_client.get(url)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        
        file_path = UPLOADS_DIR / f"infra_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_infrastructure_file, str(file_path), filename, import_id)
        
        return {
            "import_id": import_id,
            "status": "processing",
            "message": "Infrastructure data import started"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")


async def process_infrastructure_file(file_path: str, filename: str, import_id: str):
    """Process Infrastructure Excel file and store in dedicated collection"""
    try:
        logging.info(f"Processing Infrastructure file: {filename}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = [str(col).strip().lower().replace(' ', '_').replace('/', '_').replace('&', 'and') for col in df.columns]
        
        logging.info(f"Infrastructure file columns: {list(df.columns)}")
        
        # Clear existing data
        await db.infrastructure_analytics.delete_many({})
        
        records_processed = 0
        
        for _, row in df.iterrows():
            try:
                udise_col = next((c for c in df.columns if 'udise' in c), None)
                if not udise_col:
                    continue
                    
                udise = str(row[udise_col]).strip() if pd.notna(row[udise_col]) else None
                if not udise or udise == 'nan':
                    continue
                
                if '.' in udise:
                    udise = udise.split('.')[0]
                
                # Extract district and block info
                district_col = next((c for c in df.columns if 'district' in c and 'name' in c), None)
                block_col = next((c for c in df.columns if 'block' in c and 'name' in c), None)
                
                district_raw = str(row[district_col]).strip() if district_col and pd.notna(row[district_col]) else ""
                block_raw = str(row[block_col]).strip() if block_col and pd.notna(row[block_col]) else ""
                
                district_name = district_raw.split('(')[0].strip() if '(' in district_raw else district_raw
                district_code = district_raw.split('(')[1].replace(')', '').strip() if '(' in district_raw else ""
                block_name = block_raw.split('(')[0].strip() if '(' in block_raw else block_raw
                block_code = block_raw.split('(')[1].replace(')', '').strip() if '(' in block_raw else ""
                
                school_col = next((c for c in df.columns if 'school_name' in c), None)
                school_name = str(row[school_col]).strip() if school_col and pd.notna(row[school_col]) else ""
                
                # Helper to parse yes/no values
                def parse_yes_no(val):
                    if pd.isna(val):
                        return ""
                    val_str = str(val).lower().strip()
                    if '1-yes' in val_str or val_str == 'yes' or val_str.startswith('1-'):
                        if 'not functional' in val_str or 'but not' in val_str:
                            return "non_functional"
                        return "yes"
                    elif '2-no' in val_str or val_str == 'no' or val_str.startswith('2-'):
                        return "no"
                    elif '3-' in val_str:
                        if 'not functional' in val_str or 'some' in val_str:
                            return "non_functional" if 'functional' in val_str else "some"
                        return "no"
                    return ""
                
                def parse_dustbin(val):
                    if pd.isna(val):
                        return ""
                    val_str = str(val).lower().strip()
                    if 'all' in val_str or '1-yes' in val_str:
                        return "all" if 'all' in val_str else "yes"
                    elif 'some' in val_str or '3-' in val_str:
                        return "some"
                    elif '2-no' in val_str or 'no' in val_str:
                        return "no"
                    return ""
                
                def parse_furniture(val):
                    if pd.isna(val):
                        return ""
                    val_str = str(val).lower().strip()
                    if 'all' in val_str or '1-yes' in val_str:
                        return "all"
                    elif 'partial' in val_str or '2-' in val_str:
                        return "partial"
                    elif '3-no' in val_str or 'no furniture' in val_str:
                        return "no"
                    return ""
                
                def parse_special_educator(val):
                    if pd.isna(val):
                        return ""
                    val_str = str(val).lower().strip()
                    if '1-dedicated' in val_str or 'dedicated' in val_str:
                        return "dedicated"
                    elif '2-' in val_str or 'cluster' in val_str:
                        return "cluster"
                    elif '3-no' in val_str or val_str == 'no':
                        return "no"
                    return ""
                
                def safe_int(val):
                    if pd.isna(val):
                        return 0
                    try:
                        return int(float(val))
                    except:
                        return 0
                
                # Find columns
                tap_col = next((c for c in df.columns if 'tapwater' in c), None)
                purif_col = next((c for c in df.columns if 'waterpurf' in c or 'purf_ro' in c), None)
                test_col = next((c for c in df.columns if 'waterqltytesting' in c or 'testing' in c), None)
                rwh_col = next((c for c in df.columns if 'rainwaterharv' in c), None)
                
                class_dust_col = next((c for c in df.columns if 'eachclsrms_dustbin' in c or 'classroom' in c and 'dustbin' in c), None)
                toilet_dust_col = next((c for c in df.columns if 'toilet_dustbin' in c), None)
                kitchen_dust_col = next((c for c in df.columns if 'kitchen_dustbin' in c), None)
                
                kitchen_shed_col = next((c for c in df.columns if 'kitchen_shed' in c), None)
                kitchen_gard_col = next((c for c in df.columns if 'kitc_gard' in c or 'kitchen_garden' in c), None)
                
                medical_col = next((c for c in df.columns if 'mdlcheckup' in c or 'medical' in c), None)
                health_rec_col = next((c for c in df.columns if 'annual_health_record' in c), None)
                first_aid_col = next((c for c in df.columns if 'firstaid' in c), None)
                life_save_col = next((c for c in df.columns if 'life_saving' in c), None)
                thermal_col = next((c for c in df.columns if 'thermal' in c), None)
                
                ramp_col = next((c for c in df.columns if 'rampavail' in c), None)
                special_ed_col = next((c for c in df.columns if 'spcl_educator' in c or 'special_educator' in c), None)
                
                library_col = next((c for c in df.columns if c == 'library'), None)
                lib_books_col = next((c for c in df.columns if 'lib_books' in c), None)
                furniture_col = next((c for c in df.columns if 'furniture_avail' in c), None)
                playground_col = next((c for c in df.columns if 'playgrnd_fac' in c or 'playground' in c), None)
                
                record = {
                    "udise_code": udise,
                    "district_name": district_name,
                    "district_code": district_code,
                    "block_name": block_name,
                    "block_code": block_code,
                    "school_name": school_name,
                    # Water
                    "tap_water": parse_yes_no(row.get(tap_col)) if tap_col else "",
                    "water_purification": parse_yes_no(row.get(purif_col)) if purif_col else "",
                    "water_testing": parse_yes_no(row.get(test_col)) if test_col else "",
                    "rainwater_harvesting": parse_yes_no(row.get(rwh_col)) if rwh_col else "",
                    # Hygiene
                    "classroom_dustbin": parse_dustbin(row.get(class_dust_col)) if class_dust_col else "",
                    "toilet_dustbin": parse_dustbin(row.get(toilet_dust_col)) if toilet_dust_col else "",
                    "kitchen_dustbin": parse_dustbin(row.get(kitchen_dust_col)) if kitchen_dust_col else "",
                    # MDM
                    "kitchen_shed": parse_yes_no(row.get(kitchen_shed_col)) if kitchen_shed_col else "",
                    "kitchen_garden": parse_yes_no(row.get(kitchen_gard_col)) if kitchen_gard_col else "",
                    # Health
                    "medical_checkup": parse_yes_no(row.get(medical_col)) if medical_col else "",
                    "health_record": parse_yes_no(row.get(health_rec_col)) if health_rec_col else "",
                    "first_aid": parse_yes_no(row.get(first_aid_col)) if first_aid_col else "",
                    "life_saving": parse_yes_no(row.get(life_save_col)) if life_save_col else "",
                    "thermal_screening": parse_yes_no(row.get(thermal_col)) if thermal_col else "",
                    # Inclusion
                    "ramp_available": parse_yes_no(row.get(ramp_col)) if ramp_col else "",
                    "special_educator": parse_special_educator(row.get(special_ed_col)) if special_ed_col else "",
                    # Academic
                    "library": parse_yes_no(row.get(library_col)) if library_col else "",
                    "library_books": safe_int(row.get(lib_books_col)) if lib_books_col else 0,
                    "furniture": parse_furniture(row.get(furniture_col)) if furniture_col else "",
                    "playground": parse_yes_no(row.get(playground_col)) if playground_col else "",
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.infrastructure_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logging.error(f"Error processing infrastructure row: {str(e)}")
                continue
        
        logging.info(f"Infrastructure import completed: {records_processed} records")
        
    except Exception as e:
        logging.error(f"Infrastructure import failed: {str(e)}")


