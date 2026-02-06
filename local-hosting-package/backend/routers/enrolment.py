"""Enrolment Analytics Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import httpx
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/enrolment", tags=["Enrolment Analytics"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_enrolment_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Enrolment Analytics Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "total_boys": {"$sum": "$boys_enrolment"},
                "total_girls": {"$sum": "$girls_enrolment"},
                "total_trans": {"$sum": "$trans_enrolment"},
                "grand_total": {"$sum": "$total_enrolment"},
                # Pre-Primary (PP3 + PP2 + PP1)
                "pp3_boys": {"$sum": "$pp3_boys"}, "pp3_girls": {"$sum": "$pp3_girls"},
                "pp2_boys": {"$sum": "$pp2_boys"}, "pp2_girls": {"$sum": "$pp2_girls"},
                "pp1_boys": {"$sum": "$pp1_boys"}, "pp1_girls": {"$sum": "$pp1_girls"},
                # Primary (Class 1-5)
                "class1_boys": {"$sum": "$class1_boys"}, "class1_girls": {"$sum": "$class1_girls"},
                "class2_boys": {"$sum": "$class2_boys"}, "class2_girls": {"$sum": "$class2_girls"},
                "class3_boys": {"$sum": "$class3_boys"}, "class3_girls": {"$sum": "$class3_girls"},
                "class4_boys": {"$sum": "$class4_boys"}, "class4_girls": {"$sum": "$class4_girls"},
                "class5_boys": {"$sum": "$class5_boys"}, "class5_girls": {"$sum": "$class5_girls"},
                # Upper Primary (Class 6-8)
                "class6_boys": {"$sum": "$class6_boys"}, "class6_girls": {"$sum": "$class6_girls"},
                "class7_boys": {"$sum": "$class7_boys"}, "class7_girls": {"$sum": "$class7_girls"},
                "class8_boys": {"$sum": "$class8_boys"}, "class8_girls": {"$sum": "$class8_girls"},
                # Secondary (Class 9-10)
                "class9_boys": {"$sum": "$class9_boys"}, "class9_girls": {"$sum": "$class9_girls"},
                "class10_boys": {"$sum": "$class10_boys"}, "class10_girls": {"$sum": "$class10_girls"},
                # Higher Secondary (Class 11-12)
                "class11_boys": {"$sum": "$class11_boys"}, "class11_girls": {"$sum": "$class11_girls"},
                "class12_boys": {"$sum": "$class12_boys"}, "class12_girls": {"$sum": "$class12_girls"},
            }
        }
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "grand_total": 0}
    
    data = result[0]
    total_schools = data.get("total_schools", 0) or 1
    grand_total = data.get("grand_total", 0) or 1
    total_boys = data.get("total_boys", 0) or 0
    total_girls = data.get("total_girls", 0) or 0
    
    # Gender metrics
    girls_pct = round((total_girls / grand_total) * 100, 1) if grand_total > 0 else 0
    gender_parity_index = round(total_girls / total_boys, 2) if total_boys > 0 else 0
    gender_gap = total_boys - total_girls
    
    # Average school size
    avg_school_size = round(grand_total / total_schools, 0)
    
    # Calculate stage totals from individual class data
    pp_total = (data.get("pp3_boys", 0) + data.get("pp3_girls", 0) +
                data.get("pp2_boys", 0) + data.get("pp2_girls", 0) +
                data.get("pp1_boys", 0) + data.get("pp1_girls", 0))
    
    primary_total = (data.get("class1_boys", 0) + data.get("class1_girls", 0) +
                     data.get("class2_boys", 0) + data.get("class2_girls", 0) +
                     data.get("class3_boys", 0) + data.get("class3_girls", 0) +
                     data.get("class4_boys", 0) + data.get("class4_girls", 0) +
                     data.get("class5_boys", 0) + data.get("class5_girls", 0))
    
    upper_primary_total = (data.get("class6_boys", 0) + data.get("class6_girls", 0) +
                           data.get("class7_boys", 0) + data.get("class7_girls", 0) +
                           data.get("class8_boys", 0) + data.get("class8_girls", 0))
    
    secondary_total = (data.get("class9_boys", 0) + data.get("class9_girls", 0) +
                       data.get("class10_boys", 0) + data.get("class10_girls", 0))
    
    hs_total = (data.get("class11_boys", 0) + data.get("class11_girls", 0) +
                data.get("class12_boys", 0) + data.get("class12_girls", 0))
    
    pp_pct = round((pp_total / grand_total) * 100, 1) if grand_total > 0 else 0
    primary_pct = round((primary_total / grand_total) * 100, 1) if grand_total > 0 else 0
    upper_primary_pct = round((upper_primary_total / grand_total) * 100, 1) if grand_total > 0 else 0
    secondary_pct = round((secondary_total / grand_total) * 100, 1) if grand_total > 0 else 0
    hs_pct = round((hs_total / grand_total) * 100, 1) if grand_total > 0 else 0
    
    # Retention KPIs - calculate class totals
    class5_total = data.get("class5_boys", 0) + data.get("class5_girls", 0)
    class6_total = data.get("class6_boys", 0) + data.get("class6_girls", 0)
    class8_total = data.get("class8_boys", 0) + data.get("class8_girls", 0)
    class9_total = data.get("class9_boys", 0) + data.get("class9_girls", 0)
    class10_total = data.get("class10_boys", 0) + data.get("class10_girls", 0)
    class11_total = data.get("class11_boys", 0) + data.get("class11_girls", 0)
    
    primary_upper_retention = round((class6_total / class5_total) * 100, 1) if class5_total > 0 else 0
    upper_secondary_retention = round((class9_total / class8_total) * 100, 1) if class8_total > 0 else 0
    secondary_hs_retention = round((class11_total / class10_total) * 100, 1) if class10_total > 0 else 0
    
    return {
        "total_schools": total_schools,
        "grand_total": grand_total,
        "total_boys": total_boys,
        "total_girls": total_girls,
        "total_trans": data.get("total_trans", 0) or 0,
        "girls_participation_pct": girls_pct,
        "gender_parity_index": gender_parity_index,
        "gender_gap": gender_gap,
        "avg_school_size": avg_school_size,
        # Stage totals
        "pp_total": pp_total,
        "primary_total": primary_total,
        "upper_primary_total": upper_primary_total,
        "secondary_total": secondary_total,
        "hs_total": hs_total,
        # Stage percentages
        "pp_pct": pp_pct,
        "primary_pct": primary_pct,
        "upper_primary_pct": upper_primary_pct,
        "secondary_pct": secondary_pct,
        "hs_pct": hs_pct,
        # Retention KPIs
        "primary_upper_retention": primary_upper_retention,
        "upper_secondary_retention": upper_secondary_retention,
        "secondary_hs_retention": secondary_hs_retention,
        # Class-wise totals (calculated from boys + girls)
        "class_totals": {
            "PP3": data.get("pp3_boys", 0) + data.get("pp3_girls", 0),
            "PP2": data.get("pp2_boys", 0) + data.get("pp2_girls", 0),
            "PP1": data.get("pp1_boys", 0) + data.get("pp1_girls", 0),
            "Class 1": data.get("class1_boys", 0) + data.get("class1_girls", 0),
            "Class 2": data.get("class2_boys", 0) + data.get("class2_girls", 0),
            "Class 3": data.get("class3_boys", 0) + data.get("class3_girls", 0),
            "Class 4": data.get("class4_boys", 0) + data.get("class4_girls", 0),
            "Class 5": class5_total,
            "Class 6": class6_total,
            "Class 7": data.get("class7_boys", 0) + data.get("class7_girls", 0),
            "Class 8": class8_total,
            "Class 9": class9_total,
            "Class 10": class10_total,
            "Class 11": class11_total,
            "Class 12": data.get("class12_boys", 0) + data.get("class12_girls", 0),
        }
    }


@router.get("/class-wise")
async def get_enrolment_class_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get class-wise enrolment breakdown with gender"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                # Boys
                "pp3_boys": {"$sum": "$pp3_boys"}, "pp2_boys": {"$sum": "$pp2_boys"}, "pp1_boys": {"$sum": "$pp1_boys"},
                "class1_boys": {"$sum": "$class1_boys"}, "class2_boys": {"$sum": "$class2_boys"}, "class3_boys": {"$sum": "$class3_boys"},
                "class4_boys": {"$sum": "$class4_boys"}, "class5_boys": {"$sum": "$class5_boys"}, "class6_boys": {"$sum": "$class6_boys"},
                "class7_boys": {"$sum": "$class7_boys"}, "class8_boys": {"$sum": "$class8_boys"}, "class9_boys": {"$sum": "$class9_boys"},
                "class10_boys": {"$sum": "$class10_boys"}, "class11_boys": {"$sum": "$class11_boys"}, "class12_boys": {"$sum": "$class12_boys"},
                # Girls
                "pp3_girls": {"$sum": "$pp3_girls"}, "pp2_girls": {"$sum": "$pp2_girls"}, "pp1_girls": {"$sum": "$pp1_girls"},
                "class1_girls": {"$sum": "$class1_girls"}, "class2_girls": {"$sum": "$class2_girls"}, "class3_girls": {"$sum": "$class3_girls"},
                "class4_girls": {"$sum": "$class4_girls"}, "class5_girls": {"$sum": "$class5_girls"}, "class6_girls": {"$sum": "$class6_girls"},
                "class7_girls": {"$sum": "$class7_girls"}, "class8_girls": {"$sum": "$class8_girls"}, "class9_girls": {"$sum": "$class9_girls"},
                "class10_girls": {"$sum": "$class10_girls"}, "class11_girls": {"$sum": "$class11_girls"}, "class12_girls": {"$sum": "$class12_girls"},
                # Totals
                "pp3_total": {"$sum": "$pp3_total"}, "pp2_total": {"$sum": "$pp2_total"}, "pp1_total": {"$sum": "$pp1_total"},
                "class1_total": {"$sum": "$class1_total"}, "class2_total": {"$sum": "$class2_total"}, "class3_total": {"$sum": "$class3_total"},
                "class4_total": {"$sum": "$class4_total"}, "class5_total": {"$sum": "$class5_total"}, "class6_total": {"$sum": "$class6_total"},
                "class7_total": {"$sum": "$class7_total"}, "class8_total": {"$sum": "$class8_total"}, "class9_total": {"$sum": "$class9_total"},
                "class10_total": {"$sum": "$class10_total"}, "class11_total": {"$sum": "$class11_total"}, "class12_total": {"$sum": "$class12_total"},
            }
        }
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    classes = ["PP3", "PP2", "PP1", "Class 1", "Class 2", "Class 3", "Class 4", "Class 5", 
               "Class 6", "Class 7", "Class 8", "Class 9", "Class 10", "Class 11", "Class 12"]
    
    keys = ["pp3", "pp2", "pp1", "class1", "class2", "class3", "class4", "class5",
            "class6", "class7", "class8", "class9", "class10", "class11", "class12"]
    
    class_data = []
    for i, cls in enumerate(classes):
        key = keys[i]
        boys = data.get(f"{key}_boys", 0) or 0
        girls = data.get(f"{key}_girls", 0) or 0
        total = data.get(f"{key}_total", 0) or 0
        gpi = round(girls / boys, 2) if boys > 0 else 0
        
        class_data.append({
            "class_name": cls,
            "boys": boys,
            "girls": girls,
            "total": total,
            "gpi": gpi,
            "girls_pct": round((girls / total) * 100, 1) if total > 0 else 0
        })
    
    return class_data


@router.get("/stage-wise")
async def get_enrolment_stage_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get education stage-wise distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "pp_boys": {"$sum": {"$add": ["$pp3_boys", "$pp2_boys", "$pp1_boys"]}},
                "pp_girls": {"$sum": {"$add": ["$pp3_girls", "$pp2_girls", "$pp1_girls"]}},
                "primary_boys": {"$sum": {"$add": ["$class1_boys", "$class2_boys", "$class3_boys", "$class4_boys", "$class5_boys"]}},
                "primary_girls": {"$sum": {"$add": ["$class1_girls", "$class2_girls", "$class3_girls", "$class4_girls", "$class5_girls"]}},
                "upper_boys": {"$sum": {"$add": ["$class6_boys", "$class7_boys", "$class8_boys"]}},
                "upper_girls": {"$sum": {"$add": ["$class6_girls", "$class7_girls", "$class8_girls"]}},
                "sec_boys": {"$sum": {"$add": ["$class9_boys", "$class10_boys"]}},
                "sec_girls": {"$sum": {"$add": ["$class9_girls", "$class10_girls"]}},
                "hs_boys": {"$sum": {"$add": ["$class11_boys", "$class12_boys"]}},
                "hs_girls": {"$sum": {"$add": ["$class11_girls", "$class12_girls"]}},
                "grand_total": {"$sum": "$total_enrolment"}
            }
        }
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    grand_total = data.get("grand_total", 0) or 1
    
    stages = [
        {"name": "Pre-Primary", "key": "pp", "color": "#8b5cf6"},
        {"name": "Primary (1-5)", "key": "primary", "color": "#3b82f6"},
        {"name": "Upper Primary (6-8)", "key": "upper", "color": "#10b981"},
        {"name": "Secondary (9-10)", "key": "sec", "color": "#f59e0b"},
        {"name": "Higher Secondary (11-12)", "key": "hs", "color": "#ef4444"},
    ]
    
    stage_data = []
    for stage in stages:
        key = stage["key"]
        boys = data.get(f"{key}_boys", 0) or 0
        girls = data.get(f"{key}_girls", 0) or 0
        total = boys + girls  # Calculate total from boys + girls
        
        stage_data.append({
            "name": stage["name"],
            "boys": boys,
            "girls": girls,
            "total": total,
            "percentage": round((total / grand_total) * 100, 1),
            "gpi": round(girls / boys, 2) if boys > 0 else 0,
            "color": stage["color"]
        })
    
    return stage_data


@router.get("/school-size-distribution")
async def get_school_size_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get school size distribution buckets"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$bucket": {
                "groupBy": "$total_enrolment",
                "boundaries": [0, 50, 100, 200, 300, 500, 800, 1000, 1500, 10000],
                "default": "Other",
                "output": {"count": {"$sum": 1}}
            }
        }
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    labels = {
        0: "0-50",
        50: "51-100",
        100: "101-200",
        200: "201-300",
        300: "301-500",
        500: "501-800",
        800: "801-1000",
        1000: "1001-1500",
        1500: "1500+"
    }
    
    distribution = []
    for r in results:
        bucket = r.get("_id", 0)
        try:
            bucket_int = int(bucket) if bucket else 0
        except (ValueError, TypeError):
            bucket_int = 0
        label = labels.get(bucket_int, str(bucket))
        distribution.append({
            "range": label,
            "count": r.get("count", 0),
            "color": "#ef4444" if bucket_int < 100 else "#f59e0b" if bucket_int < 300 else "#10b981" if bucket_int < 1000 else "#3b82f6"
        })
    
    return distribution


@router.get("/block-wise")
async def get_enrolment_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise enrolment analytics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_boys": {"$sum": "$boys_enrolment"},
                "total_girls": {"$sum": "$girls_enrolment"},
                "grand_total": {"$sum": "$total_enrolment"},
                "class5_total": {"$sum": "$class5_total"},
                "class6_total": {"$sum": "$class6_total"},
                "class10_total": {"$sum": "$class10_total"},
                "class11_total": {"$sum": "$class11_total"},
            }
        },
        {"$sort": {"grand_total": -1}}
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    block_data = []
    for r in results:
        if not r["_id"]:
            continue
        
        total = r.get("grand_total", 0) or 1
        boys = r.get("total_boys", 0) or 0
        girls = r.get("total_girls", 0) or 0
        schools = r.get("total_schools", 0) or 1
        
        gpi = round(girls / boys, 2) if boys > 0 else 0
        girls_pct = round((girls / total) * 100, 1)
        avg_size = round(total / schools, 0)
        
        class5 = r.get("class5_total", 0) or 1
        class6 = r.get("class6_total", 0) or 0
        class10 = r.get("class10_total", 0) or 1
        class11 = r.get("class11_total", 0) or 0
        
        primary_retention = round((class6 / class5) * 100, 1) if class5 > 0 else 0
        secondary_retention = round((class11 / class10) * 100, 1) if class10 > 0 else 0
        
        block_data.append({
            "block_name": r["_id"],
            "block_code": r.get("block_code", ""),
            "total_schools": schools,
            "grand_total": total,
            "total_boys": boys,
            "total_girls": girls,
            "gpi": gpi,
            "girls_pct": girls_pct,
            "avg_school_size": avg_size,
            "primary_retention": primary_retention,
            "secondary_retention": secondary_retention
        })
    
    return block_data


@router.get("/retention-analysis")
async def get_retention_analysis(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get class-wise dropout/retention analysis"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                # Sum boys + girls for each class to get totals
                "pp3_boys": {"$sum": "$pp3_boys"}, "pp3_girls": {"$sum": "$pp3_girls"},
                "pp2_boys": {"$sum": "$pp2_boys"}, "pp2_girls": {"$sum": "$pp2_girls"},
                "pp1_boys": {"$sum": "$pp1_boys"}, "pp1_girls": {"$sum": "$pp1_girls"},
                "c1_boys": {"$sum": "$class1_boys"}, "c1_girls": {"$sum": "$class1_girls"},
                "c2_boys": {"$sum": "$class2_boys"}, "c2_girls": {"$sum": "$class2_girls"},
                "c3_boys": {"$sum": "$class3_boys"}, "c3_girls": {"$sum": "$class3_girls"},
                "c4_boys": {"$sum": "$class4_boys"}, "c4_girls": {"$sum": "$class4_girls"},
                "c5_boys": {"$sum": "$class5_boys"}, "c5_girls": {"$sum": "$class5_girls"},
                "c6_boys": {"$sum": "$class6_boys"}, "c6_girls": {"$sum": "$class6_girls"},
                "c7_boys": {"$sum": "$class7_boys"}, "c7_girls": {"$sum": "$class7_girls"},
                "c8_boys": {"$sum": "$class8_boys"}, "c8_girls": {"$sum": "$class8_girls"},
                "c9_boys": {"$sum": "$class9_boys"}, "c9_girls": {"$sum": "$class9_girls"},
                "c10_boys": {"$sum": "$class10_boys"}, "c10_girls": {"$sum": "$class10_girls"},
                "c11_boys": {"$sum": "$class11_boys"}, "c11_girls": {"$sum": "$class11_girls"},
                "c12_boys": {"$sum": "$class12_boys"}, "c12_girls": {"$sum": "$class12_girls"},
            }
        }
    ], scope_match)
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    
    # Calculate totals for each class
    class_totals = {
        "pp3": data.get("pp3_boys", 0) + data.get("pp3_girls", 0),
        "pp2": data.get("pp2_boys", 0) + data.get("pp2_girls", 0),
        "pp1": data.get("pp1_boys", 0) + data.get("pp1_girls", 0),
        "c1": data.get("c1_boys", 0) + data.get("c1_girls", 0),
        "c2": data.get("c2_boys", 0) + data.get("c2_girls", 0),
        "c3": data.get("c3_boys", 0) + data.get("c3_girls", 0),
        "c4": data.get("c4_boys", 0) + data.get("c4_girls", 0),
        "c5": data.get("c5_boys", 0) + data.get("c5_girls", 0),
        "c6": data.get("c6_boys", 0) + data.get("c6_girls", 0),
        "c7": data.get("c7_boys", 0) + data.get("c7_girls", 0),
        "c8": data.get("c8_boys", 0) + data.get("c8_girls", 0),
        "c9": data.get("c9_boys", 0) + data.get("c9_girls", 0),
        "c10": data.get("c10_boys", 0) + data.get("c10_girls", 0),
        "c11": data.get("c11_boys", 0) + data.get("c11_girls", 0),
        "c12": data.get("c12_boys", 0) + data.get("c12_girls", 0),
    }
    
    classes = [
        ("PP3", "pp3"), ("PP2", "pp2"), ("PP1", "pp1"),
        ("Class 1", "c1"), ("Class 2", "c2"), ("Class 3", "c3"), ("Class 4", "c4"), ("Class 5", "c5"),
        ("Class 6", "c6"), ("Class 7", "c7"), ("Class 8", "c8"),
        ("Class 9", "c9"), ("Class 10", "c10"),
        ("Class 11", "c11"), ("Class 12", "c12")
    ]
    
    retention_data = []
    for i, (name, key) in enumerate(classes):
        current = class_totals.get(key, 0)
        prev_key = classes[i-1][1] if i > 0 else None
        prev = class_totals.get(prev_key, 0) if prev_key else 0
        
        retention = round((current / prev) * 100, 1) if prev > 0 else 100
        drop = round(((prev - current) / prev) * 100, 1) if prev > 0 and i > 0 else 0
        
        retention_data.append({
            "class_name": name,
            "enrolment": current,
            "retention_pct": retention if i > 0 else 100,
            "drop_pct": max(0, drop),
            "is_critical": drop > 15
        })
    
    return retention_data


@router.get("/risk-schools")
async def get_risk_schools(
    risk_type: str = Query("small", description="small, large, gender"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools by risk category"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    if risk_type == "small":
        match_condition = {"total_enrolment": {"$lt": 100}}
        sort_field = "total_enrolment"
        sort_order = 1
    elif risk_type == "large":
        match_condition = {"total_enrolment": {"$gt": 1000}}
        sort_field = "total_enrolment"
        sort_order = -1
    else:  # gender risk
        match_condition = {}
        sort_field = "gpi"
        sort_order = 1
    
    pipeline = [
        {"$match": {**scope_match, **match_condition}} if scope_match or match_condition else {"$match": {}},
        {
            "$project": {
                "_id": 0,
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "total_boys": "$boys_enrolment",
                "total_girls": "$girls_enrolment",
                "grand_total": "$total_enrolment",
                "gpi": {"$cond": [{"$gt": ["$boys_enrolment", 0]}, {"$divide": ["$girls_enrolment", "$boys_enrolment"]}, 0]}
            }
        },
        {"$sort": {sort_field: sort_order}},
        {"$limit": 20}
    ]
    
    if risk_type == "gender":
        pipeline[0] = {"$match": {**scope_match, "boys_enrolment": {"$gt": 0}}} if scope_match else {"$match": {"boys_enrolment": {"$gt": 0}}}
        pipeline.insert(3, {"$match": {"gpi": {"$lt": 0.85}}})
    
    cursor = db.enrolment_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    return [{
        "udise_code": r.get("udise_code", ""),
        "school_name": r.get("school_name", ""),
        "block_name": r.get("block_name", ""),
        "total_boys": r.get("total_boys", 0),
        "total_girls": r.get("total_girls", 0),
        "grand_total": r.get("grand_total", 0),
        "gpi": round(r.get("gpi", 0), 2)
    } for r in results]


@router.post("/import")
async def import_enrolment_data(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL of the Enrolment Excel file")
):
    """Import Enrolment analytics data from Excel file"""
    import_id = str(uuid.uuid4())
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            response = await http_client.get(url)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        
        file_path = UPLOADS_DIR / f"enrolment_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_enrolment_file, str(file_path), filename, import_id)
        
        return {
            "import_id": import_id,
            "status": "processing",
            "message": "Enrolment data import started"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")


async def process_enrolment_file(file_path: str, filename: str, import_id: str):
    """Process Enrolment Excel file and store in dedicated collection"""
    try:
        logger.info(f"Processing Enrolment file: {filename}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Skip header row if it contains column numbers like (1), (2)
        if str(df.iloc[0, 0]).strip() == '(1)':
            df = df.iloc[1:].reset_index(drop=True)
        
        df.columns = [str(col).strip().lower().replace(' ', '_').replace('(', '').replace(')', '') for col in df.columns]
        
        logger.info(f"Enrolment file columns: {list(df.columns)[:20]}")
        
        # Clear existing data
        await db.enrolment_analytics.delete_many({})
        
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
                
                # Extract district and block
                district_col = next((c for c in df.columns if 'district_name' in c or c == 'district_name'), None)
                block_col = next((c for c in df.columns if 'block_name' in c or c == 'block_name'), None)
                
                district_name = str(row[district_col]).strip() if district_col and pd.notna(row[district_col]) else ""
                block_name = str(row[block_col]).strip() if block_col and pd.notna(row[block_col]) else ""
                
                district_code_col = next((c for c in df.columns if 'district_code' in c), None)
                block_code_col = next((c for c in df.columns if 'block_code' in c), None)
                
                district_code = str(row[district_code_col]).strip() if district_code_col and pd.notna(row[district_code_col]) else ""
                block_code = str(row[block_code_col]).strip() if block_code_col and pd.notna(row[block_code_col]) else ""
                
                school_col = next((c for c in df.columns if 'school_name' in c), None)
                school_name = str(row[school_col]).strip() if school_col and pd.notna(row[school_col]) else ""
                
                def safe_int(val):
                    if pd.isna(val):
                        return 0
                    try:
                        return int(float(val))
                    except:
                        return 0
                
                # Build record with class-wise data
                record = {
                    "udise_code": udise,
                    "district_name": district_name,
                    "district_code": district_code,
                    "block_name": block_name,
                    "block_code": block_code,
                    "school_name": school_name,
                    # PP3
                    "pp3_boys": safe_int(row.get("pp3boys", row.get("pp3_boys", 0))),
                    "pp3_girls": safe_int(row.get("pp3girls", row.get("pp3_girls", 0))),
                    "pp3_total": safe_int(row.get("pp3total", row.get("pp3_total", 0))),
                    # PP2
                    "pp2_boys": safe_int(row.get("pp2boys", row.get("pp2_boys", 0))),
                    "pp2_girls": safe_int(row.get("pp2girls", row.get("pp2_girls", 0))),
                    "pp2_total": safe_int(row.get("pp2total", row.get("pp2_total", 0))),
                    # PP1
                    "pp1_boys": safe_int(row.get("pp1boys", row.get("pp1_boys", 0))),
                    "pp1_girls": safe_int(row.get("pp1girls", row.get("pp1_girls", 0))),
                    "pp1_total": safe_int(row.get("pp1total", row.get("pp1_total", 0))),
                    # Class 1-12
                    "class1_boys": safe_int(row.get("class_1boys", 0)), "class1_girls": safe_int(row.get("class_1girls", 0)), "class1_total": safe_int(row.get("class_1total", 0)),
                    "class2_boys": safe_int(row.get("class_2boys", 0)), "class2_girls": safe_int(row.get("class_2girls", 0)), "class2_total": safe_int(row.get("class_2total", 0)),
                    "class3_boys": safe_int(row.get("class_3boys", 0)), "class3_girls": safe_int(row.get("class_3girls", 0)), "class3_total": safe_int(row.get("class_3total", 0)),
                    "class4_boys": safe_int(row.get("class_4boys", 0)), "class4_girls": safe_int(row.get("class_4girls", 0)), "class4_total": safe_int(row.get("class_4total", 0)),
                    "class5_boys": safe_int(row.get("class_5boys", 0)), "class5_girls": safe_int(row.get("class_5girls", 0)), "class5_total": safe_int(row.get("class_5total", 0)),
                    "class6_boys": safe_int(row.get("class_6boys", 0)), "class6_girls": safe_int(row.get("class_6girls", 0)), "class6_total": safe_int(row.get("class_6total", 0)),
                    "class7_boys": safe_int(row.get("class_7boys", 0)), "class7_girls": safe_int(row.get("class_7girls", 0)), "class7_total": safe_int(row.get("class_7total", 0)),
                    "class8_boys": safe_int(row.get("class_8boys", 0)), "class8_girls": safe_int(row.get("class_8girls", 0)), "class8_total": safe_int(row.get("class_8total", 0)),
                    "class9_boys": safe_int(row.get("class_9boys", 0)), "class9_girls": safe_int(row.get("class_9girls", 0)), "class9_total": safe_int(row.get("class_9total", 0)),
                    "class10_boys": safe_int(row.get("class_10boys", 0)), "class10_girls": safe_int(row.get("class_10girls", 0)), "class10_total": safe_int(row.get("class_10total", 0)),
                    "class11_boys": safe_int(row.get("class_11boys", 0)), "class11_girls": safe_int(row.get("class_11girls", 0)), "class11_total": safe_int(row.get("class_11total", 0)),
                    "class12_boys": safe_int(row.get("class_12boys", 0)), "class12_girls": safe_int(row.get("class_12girls", 0)), "class12_total": safe_int(row.get("class_12total", 0)),
                    # Totals
                    "total_boys": safe_int(row.get("total_boys", 0)),
                    "total_girls": safe_int(row.get("total_girls", 0)),
                    "total_trans": safe_int(row.get("total_trans", 0)),
                    "grand_total": safe_int(row.get("grand_total", 0)),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.enrolment_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing enrolment row: {str(e)}")
                continue
        
        logger.info(f"Enrolment import completed: {records_processed} records")
        
    except Exception as e:
        logger.error(f"Enrolment import failed: {str(e)}")


