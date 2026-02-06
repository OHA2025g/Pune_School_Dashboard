"""Classrooms & Toilets Analytics Router"""
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

router = APIRouter(prefix="/classrooms-toilets", tags=["Classrooms & Toilets"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_classrooms_toilets_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get overview KPIs for Classrooms & Toilets Dashboard"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_blocks": {"$addToSet": "$block_name"},
            # Classroom metrics
            "total_classrooms": {"$sum": "$classrooms_instructional"},
            "classrooms_under_construction": {"$sum": "$classrooms_under_construction"},
            "classrooms_dilapidated": {"$sum": "$classrooms_dilapidated"},
            # Classroom condition
            "pucca_good": {"$sum": "$pucca_good"},
            "pucca_minor": {"$sum": "$pucca_minor"},
            "pucca_major": {"$sum": "$pucca_major"},
            "part_pucca_good": {"$sum": "$part_pucca_good"},
            "part_pucca_minor": {"$sum": "$part_pucca_minor"},
            "part_pucca_major": {"$sum": "$part_pucca_major"},
            # Toilet metrics - Boys
            "boys_toilets_total": {"$sum": "$boys_toilets_total"},
            "boys_toilets_functional": {"$sum": "$boys_toilets_functional"},
            "boys_toilets_water": {"$sum": "$boys_toilets_water"},
            # Toilet metrics - Girls
            "girls_toilets_total": {"$sum": "$girls_toilets_total"},
            "girls_toilets_functional": {"$sum": "$girls_toilets_functional"},
            "girls_toilets_water": {"$sum": "$girls_toilets_water"},
            # CWSN Toilets
            "cwsn_boys_total": {"$sum": "$cwsn_boys_total"},
            "cwsn_boys_functional": {"$sum": "$cwsn_boys_functional"},
            "cwsn_girls_total": {"$sum": "$cwsn_girls_total"},
            "cwsn_girls_functional": {"$sum": "$cwsn_girls_functional"},
            # Urinals
            "urinals_boys_total": {"$sum": "$urinals_boys_total"},
            "urinals_boys_functional": {"$sum": "$urinals_boys_functional"},
            "urinals_girls_total": {"$sum": "$urinals_girls_total"},
            "urinals_girls_functional": {"$sum": "$urinals_girls_functional"},
            # Under construction
            "boys_toilets_uc": {"$sum": "$boys_toilets_uc"},
            "girls_toilets_uc": {"$sum": "$girls_toilets_uc"},
            # Hygiene
            "handwash_points": {"$sum": "$handwash_points"},
            "schools_with_handwash": {"$sum": {"$cond": [{"$gt": ["$handwash_facility", 0]}, 1, 0]}},
            "schools_with_sanitary_pad": {"$sum": {"$cond": [{"$gt": ["$sanitary_pad", 0]}, 1, 0]}},
            "schools_with_electricity": {"$sum": {"$cond": [{"$gt": ["$electricity", 0]}, 1, 0]}},
            # Zero toilet schools
            "zero_boys_toilet": {"$sum": {"$cond": [{"$eq": ["$boys_toilets_total", 0]}, 1, 0]}},
            "zero_girls_toilet": {"$sum": {"$cond": [{"$eq": ["$girls_toilets_total", 0]}, 1, 0]}}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "total_blocks": 0}
    
    data = result[0]
    total_schools = data["total_schools"]
    total_blocks = len(data["total_blocks"])
    
    # Classroom calculations
    total_classrooms = data["total_classrooms"]
    total_good = data["pucca_good"] + data["part_pucca_good"]
    total_minor = data["pucca_minor"] + data["part_pucca_minor"]
    total_major = data["pucca_major"] + data["part_pucca_major"]
    
    classroom_health_index = round((total_good + 0.5 * total_minor) / total_classrooms * 100, 1) if total_classrooms > 0 else 0
    repair_backlog_pct = round((total_minor + total_major) / total_classrooms * 100, 1) if total_classrooms > 0 else 0
    
    # Toilet calculations
    boys_total = data["boys_toilets_total"]
    boys_func = data["boys_toilets_functional"]
    boys_water = data["boys_toilets_water"]
    girls_total = data["girls_toilets_total"]
    girls_func = data["girls_toilets_functional"]
    girls_water = data["girls_toilets_water"]
    
    total_toilets = boys_total + girls_total
    total_functional = boys_func + girls_func
    total_with_water = boys_water + girls_water
    
    toilet_functional_pct = round(total_functional / total_toilets * 100, 1) if total_toilets > 0 else 0
    water_coverage_pct = round(total_with_water / total_functional * 100, 1) if total_functional > 0 else 0
    
    # Gender parity
    gender_parity_index = round(girls_total / boys_total, 2) if boys_total > 0 else 0
    
    # CWSN
    cwsn_total = data["cwsn_boys_total"] + data["cwsn_girls_total"]
    cwsn_functional = data["cwsn_boys_functional"] + data["cwsn_girls_functional"]
    cwsn_functional_pct = round(cwsn_functional / cwsn_total * 100, 1) if cwsn_total > 0 else 0
    
    # Hygiene
    handwash_coverage_pct = round(data["schools_with_handwash"] / total_schools * 100, 1)
    sanitary_pad_pct = round(data["schools_with_sanitary_pad"] / total_schools * 100, 1)
    electricity_pct = round(data["schools_with_electricity"] / total_schools * 100, 1)
    
    # Handwash sufficiency
    handwash_sufficiency = round(data["handwash_points"] / total_toilets, 2) if total_toilets > 0 else 0
    
    # Zero toilet schools
    zero_boys_pct = round(data["zero_boys_toilet"] / total_schools * 100, 2)
    zero_girls_pct = round(data["zero_girls_toilet"] / total_schools * 100, 2)
    
    # Water gap
    water_gap = total_functional - total_with_water
    
    # Composite indices
    wash_compliance_index = round((toilet_functional_pct + water_coverage_pct + handwash_coverage_pct) / 3, 1)
    infrastructure_readiness = round((classroom_health_index + toilet_functional_pct + (100 - repair_backlog_pct)) / 3, 1)
    
    return {
        # Scale
        "total_schools": total_schools,
        "total_blocks": total_blocks,
        "schools_per_block": round(total_schools / total_blocks, 1) if total_blocks > 0 else 0,
        
        # Classroom metrics
        "total_classrooms": total_classrooms,
        "avg_classrooms_per_school": round(total_classrooms / total_schools, 1) if total_schools > 0 else 0,
        "classrooms_good": total_good,
        "classrooms_minor_repair": total_minor,
        "classrooms_major_repair": total_major,
        "classroom_health_index": classroom_health_index,
        "repair_backlog_pct": repair_backlog_pct,
        "classrooms_under_construction": data["classrooms_under_construction"],
        "classrooms_dilapidated": data["classrooms_dilapidated"],
        
        # Toilet metrics
        "boys_toilets_total": boys_total,
        "boys_toilets_functional": boys_func,
        "boys_toilets_water": boys_water,
        "boys_functional_pct": round(boys_func / boys_total * 100, 1) if boys_total > 0 else 0,
        "boys_water_pct": round(boys_water / boys_func * 100, 1) if boys_func > 0 else 0,
        
        "girls_toilets_total": girls_total,
        "girls_toilets_functional": girls_func,
        "girls_toilets_water": girls_water,
        "girls_functional_pct": round(girls_func / girls_total * 100, 1) if girls_total > 0 else 0,
        "girls_water_pct": round(girls_water / girls_func * 100, 1) if girls_func > 0 else 0,
        
        "total_toilets": total_toilets,
        "total_functional": total_functional,
        "toilet_functional_pct": toilet_functional_pct,
        "water_coverage_pct": water_coverage_pct,
        "water_gap": water_gap,
        "gender_parity_index": gender_parity_index,
        
        # CWSN
        "cwsn_total": cwsn_total,
        "cwsn_functional": cwsn_functional,
        "cwsn_functional_pct": cwsn_functional_pct,
        "cwsn_coverage_pct": round(cwsn_total / total_schools, 2),
        
        # Urinals
        "urinals_total": data["urinals_boys_total"] + data["urinals_girls_total"],
        "urinals_functional": data["urinals_boys_functional"] + data["urinals_girls_functional"],
        
        # Under construction
        "toilets_under_construction": data["boys_toilets_uc"] + data["girls_toilets_uc"],
        
        # Hygiene
        "handwash_points": data["handwash_points"],
        "handwash_coverage_pct": handwash_coverage_pct,
        "handwash_sufficiency_ratio": handwash_sufficiency,
        "sanitary_pad_pct": sanitary_pad_pct,
        "electricity_pct": electricity_pct,
        
        # Zero toilet schools
        "zero_boys_toilet_schools": data["zero_boys_toilet"],
        "zero_girls_toilet_schools": data["zero_girls_toilet"],
        "zero_boys_pct": zero_boys_pct,
        "zero_girls_pct": zero_girls_pct,
        
        # Composite indices
        "wash_compliance_index": wash_compliance_index,
        "infrastructure_readiness_index": infrastructure_readiness
    }


@router.get("/block-wise")
async def get_classrooms_toilets_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get block-wise classroom and toilet metrics"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": "$block_name",
            "total_schools": {"$sum": 1},
            "total_classrooms": {"$sum": "$classrooms_instructional"},
            "classrooms_good": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
            "classrooms_minor": {"$sum": {"$add": ["$pucca_minor", "$part_pucca_minor"]}},
            "classrooms_major": {"$sum": {"$add": ["$pucca_major", "$part_pucca_major"]}},
            "boys_toilets": {"$sum": "$boys_toilets_total"},
            "boys_functional": {"$sum": "$boys_toilets_functional"},
            "boys_water": {"$sum": "$boys_toilets_water"},
            "girls_toilets": {"$sum": "$girls_toilets_total"},
            "girls_functional": {"$sum": "$girls_toilets_functional"},
            "girls_water": {"$sum": "$girls_toilets_water"},
            "cwsn_total": {"$sum": {"$add": ["$cwsn_boys_total", "$cwsn_girls_total"]}},
            "cwsn_functional": {"$sum": {"$add": ["$cwsn_boys_functional", "$cwsn_girls_functional"]}},
            "handwash_points": {"$sum": "$handwash_points"},
            "schools_with_handwash": {"$sum": {"$cond": [{"$gt": ["$handwash_facility", 0]}, 1, 0]}},
            "schools_with_sanitary": {"$sum": {"$cond": [{"$gt": ["$sanitary_pad", 0]}, 1, 0]}},
            "zero_boys": {"$sum": {"$cond": [{"$eq": ["$boys_toilets_total", 0]}, 1, 0]}},
            "zero_girls": {"$sum": {"$cond": [{"$eq": ["$girls_toilets_total", 0]}, 1, 0]}}
        }},
        {"$sort": {"total_schools": -1}}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    results = []
    rank = 1
    
    async for doc in cursor:
        total_classrooms = doc["total_classrooms"]
        good = doc["classrooms_good"]
        minor = doc["classrooms_minor"]
        major = doc["classrooms_major"]
        
        classroom_health = round((good + 0.5 * minor) / total_classrooms * 100, 1) if total_classrooms > 0 else 0
        
        total_toilets = doc["boys_toilets"] + doc["girls_toilets"]
        total_functional = doc["boys_functional"] + doc["girls_functional"]
        total_water = doc["boys_water"] + doc["girls_water"]
        
        functional_pct = round(total_functional / total_toilets * 100, 1) if total_toilets > 0 else 0
        water_pct = round(total_water / total_functional * 100, 1) if total_functional > 0 else 0
        
        results.append({
            "rank": rank,
            "block_name": doc["_id"],
            "total_schools": doc["total_schools"],
            "total_classrooms": total_classrooms,
            "avg_classrooms": round(total_classrooms / doc["total_schools"], 1) if doc["total_schools"] > 0 else 0,
            "classroom_health_index": classroom_health,
            "repair_backlog_pct": round((minor + major) / total_classrooms * 100, 1) if total_classrooms > 0 else 0,
            "total_toilets": total_toilets,
            "functional_pct": functional_pct,
            "water_pct": water_pct,
            "cwsn_total": doc["cwsn_total"],
            "cwsn_functional_pct": round(doc["cwsn_functional"] / doc["cwsn_total"] * 100, 1) if doc["cwsn_total"] > 0 else 0,
            "handwash_pct": round(doc["schools_with_handwash"] / doc["total_schools"] * 100, 1),
            "sanitary_pct": round(doc["schools_with_sanitary"] / doc["total_schools"] * 100, 1),
            "zero_toilet_schools": doc["zero_boys"] + doc["zero_girls"],
            "wash_index": round((functional_pct + water_pct + round(doc["schools_with_handwash"] / doc["total_schools"] * 100, 1)) / 3, 1)
        })
        rank += 1
    
    # Re-rank by WASH index
    results.sort(key=lambda x: x["wash_index"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1
    
    return results


@router.get("/classroom-condition")
async def get_classroom_condition(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get classroom condition distribution"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "pucca_good": {"$sum": "$pucca_good"},
            "pucca_minor": {"$sum": "$pucca_minor"},
            "pucca_major": {"$sum": "$pucca_major"},
            "part_pucca_good": {"$sum": "$part_pucca_good"},
            "part_pucca_minor": {"$sum": "$part_pucca_minor"},
            "part_pucca_major": {"$sum": "$part_pucca_major"},
            "kuchcha_good": {"$sum": "$kuchcha_good"},
            "kuchcha_minor": {"$sum": "$kuchcha_minor"},
            "kuchcha_major": {"$sum": "$kuchcha_major"},
            "tent_good": {"$sum": "$tent_good"},
            "tent_minor": {"$sum": "$tent_minor"},
            "tent_major": {"$sum": "$tent_major"}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    
    # By condition
    condition_data = [
        {"condition": "Good", "count": data["pucca_good"] + data["part_pucca_good"] + data["kuchcha_good"] + data["tent_good"], "color": "#10b981"},
        {"condition": "Minor Repair", "count": data["pucca_minor"] + data["part_pucca_minor"] + data["kuchcha_minor"] + data["tent_minor"], "color": "#f59e0b"},
        {"condition": "Major Repair", "count": data["pucca_major"] + data["part_pucca_major"] + data["kuchcha_major"] + data["tent_major"], "color": "#ef4444"}
    ]
    
    # By type
    type_data = [
        {"type": "Pucca", "good": data["pucca_good"], "minor": data["pucca_minor"], "major": data["pucca_major"]},
        {"type": "Part Pucca", "good": data["part_pucca_good"], "minor": data["part_pucca_minor"], "major": data["part_pucca_major"]},
        {"type": "Kuchcha", "good": data["kuchcha_good"], "minor": data["kuchcha_minor"], "major": data["kuchcha_major"]},
        {"type": "Tent", "good": data["tent_good"], "minor": data["tent_minor"], "major": data["tent_major"]}
    ]
    
    return {"by_condition": condition_data, "by_type": type_data}


@router.get("/toilet-distribution")
async def get_toilet_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get toilet availability and functional distribution"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "boys_total": {"$sum": "$boys_toilets_total"},
            "boys_functional": {"$sum": "$boys_toilets_functional"},
            "boys_water": {"$sum": "$boys_toilets_water"},
            "girls_total": {"$sum": "$girls_toilets_total"},
            "girls_functional": {"$sum": "$girls_toilets_functional"},
            "girls_water": {"$sum": "$girls_toilets_water"},
            "cwsn_boys_total": {"$sum": "$cwsn_boys_total"},
            "cwsn_boys_func": {"$sum": "$cwsn_boys_functional"},
            "cwsn_girls_total": {"$sum": "$cwsn_girls_total"},
            "cwsn_girls_func": {"$sum": "$cwsn_girls_functional"}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {}
    
    data = result[0]
    
    return {
        "by_gender": [
            {"gender": "Boys", "total": data["boys_total"], "functional": data["boys_functional"], "with_water": data["boys_water"]},
            {"gender": "Girls", "total": data["girls_total"], "functional": data["girls_functional"], "with_water": data["girls_water"]}
        ],
        "cwsn": [
            {"gender": "Boys", "total": data["cwsn_boys_total"], "functional": data["cwsn_boys_func"]},
            {"gender": "Girls", "total": data["cwsn_girls_total"], "functional": data["cwsn_girls_func"]}
        ],
        "functional_breakdown": [
            {"status": "Functional", "count": data["boys_functional"] + data["girls_functional"], "color": "#10b981"},
            {"status": "Non-Functional", "count": (data["boys_total"] - data["boys_functional"]) + (data["girls_total"] - data["girls_functional"]), "color": "#ef4444"}
        ],
        "water_breakdown": [
            {"status": "With Water", "count": data["boys_water"] + data["girls_water"], "color": "#3b82f6"},
            {"status": "Without Water", "count": (data["boys_functional"] - data["boys_water"]) + (data["girls_functional"] - data["girls_water"]), "color": "#f59e0b"}
        ]
    }


@router.get("/hygiene-metrics")
async def get_hygiene_metrics(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get hygiene and WASH compliance metrics"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "with_handwash": {"$sum": {"$cond": [{"$gt": ["$handwash_facility", 0]}, 1, 0]}},
            "with_sanitary": {"$sum": {"$cond": [{"$gt": ["$sanitary_pad", 0]}, 1, 0]}},
            "with_electricity": {"$sum": {"$cond": [{"$gt": ["$electricity", 0]}, 1, 0]}},
            "with_incinerator": {"$sum": {"$cond": [{"$eq": ["$incinerator", True]}, 1, 0]}},
            "handwash_points": {"$sum": "$handwash_points"},
            "total_toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {}
    
    data = result[0]
    total = data["total_schools"]
    
    return {
        "handwash_coverage": round(data["with_handwash"] / total * 100, 1),
        "sanitary_pad_coverage": round(data["with_sanitary"] / total * 100, 1),
        "electricity_coverage": round(data["with_electricity"] / total * 100, 1),
        "incinerator_coverage": round(data["with_incinerator"] / total * 100, 1) if data["with_incinerator"] else 0,
        "handwash_points": data["handwash_points"],
        "handwash_sufficiency": round(data["handwash_points"] / data["total_toilets"], 2) if data["total_toilets"] > 0 else 0,
        "distribution": [
            {"facility": "Handwash Facility", "yes": data["with_handwash"], "no": total - data["with_handwash"]},
            {"facility": "Sanitary Pad", "yes": data["with_sanitary"], "no": total - data["with_sanitary"]},
            {"facility": "Electricity", "yes": data["with_electricity"], "no": total - data["with_electricity"]}
        ]
    }


@router.get("/risk-schools")
async def get_risk_schools(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get high-risk schools based on infrastructure deficiencies"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    # Zero toilet schools
    zero_toilet_query = {"$or": [{"boys_toilets_total": 0}, {"girls_toilets_total": 0}]}
    if scope_match:
        zero_toilet_query.update(scope_match)
    zero_toilet_cursor = db.classrooms_toilets.find(
        zero_toilet_query,
        {"_id": 0, "udise_code": 1, "school_name": 1, "block_name": 1, "boys_toilets_total": 1, "girls_toilets_total": 1}
    ).limit(50)
    zero_toilet_schools = await zero_toilet_cursor.to_list(length=50)
    
    # No water schools (functional toilets but no water)
    no_water_match = {
            "$expr": {
                "$and": [
                    {"$gt": [{"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}, 0]},
                    {"$eq": [{"$add": ["$boys_toilets_water", "$girls_toilets_water"]}, 0]}
                ]
            }
    }
    if scope_match:
        no_water_match.update(scope_match)
    no_water_pipeline = [
        {"$match": no_water_match},
        {"$project": {
            "_id": 0, "udise_code": 1, "school_name": 1, "block_name": 1,
            "functional_toilets": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}
        }},
        {"$limit": 50}
    ]
    no_water_cursor = db.classrooms_toilets.aggregate(no_water_pipeline)
    no_water_schools = await no_water_cursor.to_list(length=50)
    
    # No handwash schools
    no_handwash_query = {"handwash_facility": False}
    if scope_match:
        no_handwash_query.update(scope_match)
    no_handwash_cursor = db.classrooms_toilets.find(
        no_handwash_query,
        {"_id": 0, "udise_code": 1, "school_name": 1, "block_name": 1}
    ).limit(50)
    no_handwash_schools = await no_handwash_cursor.to_list(length=50)
    
    # Major repair schools (>50% classrooms need major repair)
    major_repair_pipeline = prepend_match([
        {"$project": {
            "_id": 0, "udise_code": 1, "school_name": 1, "block_name": 1,
            "total_classrooms": "$classrooms_instructional",
            "major_repair": {"$add": ["$pucca_major", "$part_pucca_major", "$kuchcha_major", "$tent_major"]},
            "major_pct": {
                "$cond": [
                    {"$gt": ["$classrooms_instructional", 0]},
                    {"$multiply": [{"$divide": [{"$add": ["$pucca_major", "$part_pucca_major", "$kuchcha_major", "$tent_major"]}, "$classrooms_instructional"]}, 100]},
                    0
                ]
            }
        }},
        {"$match": {"major_pct": {"$gt": 30}}},
        {"$sort": {"major_pct": -1}},
        {"$limit": 50}
    ], scope_match)
    major_repair_cursor = db.classrooms_toilets.aggregate(major_repair_pipeline)
    major_repair_schools = await major_repair_cursor.to_list(length=50)
    
    # Risk counts
    count_query = scope_match if scope_match else {}
    total_schools = await db.classrooms_toilets.count_documents(count_query)
    zero_toilet_count_query = {"$or": [{"boys_toilets_total": 0}, {"girls_toilets_total": 0}]}
    if scope_match:
        zero_toilet_count_query.update(scope_match)
    zero_toilet_count = await db.classrooms_toilets.count_documents(zero_toilet_count_query)
    no_handwash_count_query = {"handwash_facility": False}
    if scope_match:
        no_handwash_count_query.update(scope_match)
    no_handwash_count = await db.classrooms_toilets.count_documents(no_handwash_count_query)
    
    return {
        "risk_summary": {
            "total_schools": total_schools,
            "zero_toilet_schools": zero_toilet_count,
            "zero_toilet_pct": round(zero_toilet_count / total_schools * 100, 2) if total_schools > 0 else 0,
            "no_handwash_schools": no_handwash_count,
            "no_handwash_pct": round(no_handwash_count / total_schools * 100, 2) if total_schools > 0 else 0,
            "no_water_schools": len(no_water_schools),
            "major_repair_schools": len(major_repair_schools)
        },
        "zero_toilet_schools": zero_toilet_schools,
        "no_water_schools": no_water_schools,
        "no_handwash_schools": no_handwash_schools,
        "major_repair_schools": [
            {**s, "major_pct": round(s["major_pct"], 1)} for s in major_repair_schools
        ]
    }


@router.get("/construction-status")
async def get_construction_status(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get construction and infrastructure pipeline status"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "classrooms_uc": {"$sum": "$classrooms_under_construction"},
            "classrooms_dilapidated": {"$sum": "$classrooms_dilapidated"},
            "boys_toilets_uc": {"$sum": "$boys_toilets_uc"},
            "girls_toilets_uc": {"$sum": "$girls_toilets_uc"},
            "buildings_uc": {"$sum": "$buildings_under_construction"},
            "total_classrooms": {"$sum": "$classrooms_instructional"},
            "total_toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {}
    
    data = result[0]
    
    return {
        "classrooms_under_construction": data["classrooms_uc"],
        "classrooms_dilapidated": data["classrooms_dilapidated"],
        "toilets_under_construction": data["boys_toilets_uc"] + data["girls_toilets_uc"],
        "boys_toilets_uc": data["boys_toilets_uc"],
        "girls_toilets_uc": data["girls_toilets_uc"],
        "buildings_under_construction": data["buildings_uc"],
        "total_classrooms": data["total_classrooms"],
        "total_toilets": data["total_toilets"],
        "infrastructure_expansion_rate": round((data["classrooms_uc"] / data["total_classrooms"]) * 100, 2) if data["total_classrooms"] > 0 else 0
    }


@router.get("/equity-metrics")
async def get_equity_metrics(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get equity and inclusion metrics (CWSN, Gender)"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "boys_toilets": {"$sum": "$boys_toilets_total"},
            "girls_toilets": {"$sum": "$girls_toilets_total"},
            "cwsn_boys": {"$sum": "$cwsn_boys_total"},
            "cwsn_boys_func": {"$sum": "$cwsn_boys_functional"},
            "cwsn_girls": {"$sum": "$cwsn_girls_total"},
            "cwsn_girls_func": {"$sum": "$cwsn_girls_functional"},
            "schools_with_cwsn": {"$sum": {"$cond": [{"$gt": [{"$add": ["$cwsn_boys_total", "$cwsn_girls_total"]}, 0]}, 1, 0]}},
            "schools_zero_girls_toilet": {"$sum": {"$cond": [{"$eq": ["$girls_toilets_total", 0]}, 1, 0]}},
            "schools_with_sanitary": {"$sum": {"$cond": [{"$gt": ["$sanitary_pad", 0]}, 1, 0]}}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {}
    
    data = result[0]
    
    cwsn_total = data["cwsn_boys"] + data["cwsn_girls"]
    cwsn_func = data["cwsn_boys_func"] + data["cwsn_girls_func"]
    
    return {
        "gender_parity_index": round(data["girls_toilets"] / data["boys_toilets"], 2) if data["boys_toilets"] > 0 else 0,
        "boys_toilets": data["boys_toilets"],
        "girls_toilets": data["girls_toilets"],
        "toilet_gap": data["girls_toilets"] - data["boys_toilets"],
        "cwsn_total": cwsn_total,
        "cwsn_functional": cwsn_func,
        "cwsn_functional_pct": round(cwsn_func / cwsn_total * 100, 1) if cwsn_total > 0 else 0,
        "cwsn_coverage_pct": round(data["schools_with_cwsn"] / data["total_schools"] * 100, 1),
        "schools_without_cwsn": data["total_schools"] - data["schools_with_cwsn"],
        "schools_zero_girls_toilet": data["schools_zero_girls_toilet"],
        "menstrual_hygiene_coverage": round(data["schools_with_sanitary"] / data["total_schools"] * 100, 1)
    }


@router.get("/top-bottom-blocks")
async def get_top_bottom_blocks(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get top and bottom performing blocks"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {"$group": {
            "_id": "$block_name",
            "total_schools": {"$sum": 1},
            "total_classrooms": {"$sum": "$classrooms_instructional"},
            "good_classrooms": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
            "total_toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
            "functional_toilets": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
            "toilets_with_water": {"$sum": {"$add": ["$boys_toilets_water", "$girls_toilets_water"]}},
            "schools_with_handwash": {"$sum": {"$cond": [{"$gt": ["$handwash_facility", 0]}, 1, 0]}}
        }}
    ], scope_match)
    
    cursor = db.classrooms_toilets.aggregate(pipeline)
    blocks = []
    
    async for doc in cursor:
        classroom_health = round(doc["good_classrooms"] / doc["total_classrooms"] * 100, 1) if doc["total_classrooms"] > 0 else 0
        toilet_func_pct = round(doc["functional_toilets"] / doc["total_toilets"] * 100, 1) if doc["total_toilets"] > 0 else 0
        water_pct = round(doc["toilets_with_water"] / doc["functional_toilets"] * 100, 1) if doc["functional_toilets"] > 0 else 0
        handwash_pct = round(doc["schools_with_handwash"] / doc["total_schools"] * 100, 1)
        
        # Composite score
        composite_score = round((classroom_health + toilet_func_pct + water_pct + handwash_pct) / 4, 1)
        
        blocks.append({
            "block_name": doc["_id"],
            "total_schools": doc["total_schools"],
            "classroom_health": classroom_health,
            "toilet_functional_pct": toilet_func_pct,
            "water_coverage_pct": water_pct,
            "handwash_coverage_pct": handwash_pct,
            "composite_score": composite_score
        })
    
    # Sort by composite score
    blocks.sort(key=lambda x: x["composite_score"], reverse=True)
    
    return {
        "top_blocks": blocks[:5],
        "bottom_blocks": blocks[-5:][::-1] if len(blocks) >= 5 else blocks[::-1]
    }


@router.post("/import")
async def import_classrooms_toilets(
    background_tasks: BackgroundTasks,
    url: str = Query(None, description="URL of Excel file to import")
):
    """Import Classrooms & Toilet Details data from Excel file"""
    if not url:
        url = "https://customer-assets.emergentagent.com/job_ab73b0f2-1d8c-414a-a97e-5f9c143b8fe0/artifacts/56e664oo_10.%20Classrooms_%26_Toilet_Details_AY_25-26.xlsx"
    
    import_id = str(uuid.uuid4())[:8]
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True, timeout=120.0)
            response.raise_for_status()
        
        filename = "classrooms_toilets.xlsx"
        file_path = UPLOADS_DIR / f"ct_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_classrooms_toilets_file, str(file_path), filename, import_id)
        
        return {"status": "processing", "import_id": import_id, "message": "Classrooms & Toilets import started"}
    
    except Exception as e:
        logging.error(f"Classrooms Toilets import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_classrooms_toilets_file(file_path: str, filename: str, import_id: str):
    """Process Classrooms & Toilet Details Excel file"""
    try:
        logging.info(f"Processing Classrooms & Toilets file: {filename}")
        
        df = pd.read_excel(file_path)
        logging.info(f"File loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Clear existing data
        await db.classrooms_toilets.delete_many({})
        
        records_processed = 0
        for idx, row in df.iterrows():
            try:
                udise = str(row.get('UDISE_Code', '')).strip() if pd.notna(row.get('UDISE_Code')) else ""
                if not udise:
                    continue
                
                # Parse block name
                block_raw = str(row.get('Block_Name_&_Code', '')).strip() if pd.notna(row.get('Block_Name_&_Code')) else ""
                block_name = block_raw.split(' (')[0] if ' (' in block_raw else block_raw
                
                # Parse district name
                district_raw = str(row.get('District_Name_&_Code', '')).strip() if pd.notna(row.get('District_Name_&_Code')) else ""
                district_name = district_raw.split(' (')[0] if ' (' in district_raw else district_raw
                
                # Helper function to get int value
                def get_int(col):
                    val = row.get(col)
                    return int(float(val)) if pd.notna(val) else 0
                
                # Helper to parse yes/no
                def is_yes(col):
                    val = str(row.get(col, '')).strip().lower() if pd.notna(row.get(col)) else ''
                    return val.startswith('1-yes') or val == '1-yes'
                
                record = {
                    "udise_code": udise,
                    "school_name": str(row.get('School_Name', '')).strip() if pd.notna(row.get('School_Name')) else "",
                    "district_name": district_name,
                    "block_name": block_name,
                    "school_category": get_int('School_Category_Code'),
                    "school_management": get_int('School_Management_Code'),
                    
                    # Building metrics
                    "total_buildings": get_int('No_Bldg_Blks_Sch_Tot'),
                    "pucca_buildings": get_int('Pucca_Bldg'),
                    "part_pucca_buildings": get_int('Part_Pucca'),
                    "kuchcha_buildings": get_int('Kuchcha_Bldg'),
                    "tent_buildings": get_int('Tent'),
                    "dilapidated_buildings": get_int('Dilap_Bldg'),
                    "buildings_under_construction": get_int('Bldg_UnderCons'),
                    
                    # Classroom metrics
                    "classrooms_prepri": get_int('Clsrm_InstPurp_Pre-Pri'),
                    "classrooms_pri": get_int('Clsrm_InstPurp_Pri'),
                    "classrooms_uprpri": get_int('Clsrm_InstPurp_UprPri'),
                    "classrooms_sec": get_int('Clsrm_InstPurp_Sec'),
                    "classrooms_highsec": get_int('Clsrm_InstPurp_HighSec'),
                    "classrooms_instructional": get_int('Clsrm_UsedforInstPurp'),
                    "classrooms_not_in_use": get_int('Currently_Not_in_Use'),
                    "classrooms_under_construction": get_int('Clsrm_UnderCons'),
                    "classrooms_dilapidated": get_int('Clsrm_DilapCond'),
                    
                    # Classroom condition
                    "pucca_good": get_int('Pucca_GudCond'),
                    "pucca_minor": get_int('Pucca_MinRep'),
                    "pucca_major": get_int('Pucca_MajRep'),
                    "part_pucca_good": get_int('PartPucca_GudCond'),
                    "part_pucca_minor": get_int('PartPucca_MinRep'),
                    "part_pucca_major": get_int('PartPucca_MajRep'),
                    "kuchcha_good": get_int('Kuchcha_GudCond'),
                    "kuchcha_minor": get_int('Kuchcha_MinRep'),
                    "kuchcha_major": get_int('Kuchcha_MajRep'),
                    "tent_good": get_int('Tent_GudCond'),
                    "tent_minor": get_int('Tent_MinRep'),
                    "tent_major": get_int('Tent_MajRep'),
                    
                    # Facilities
                    "electricity": is_yes('Electricity'),
                    "fans": get_int('Fans'),
                    "acs": get_int("Ac's"),
                    "solar_panel": is_yes('Solar_Panel'),
                    "computer_labs": get_int('Computer_Labs'),
                    "library_room": is_yes('Library_room'),
                    
                    # Toilets - Boys (excluding CWSN)
                    "boys_toilets_total": get_int('Toilet_ExclCWSN_B_Tot'),
                    "boys_toilets_functional": get_int('Toilet_ExclCWSN_B_Func'),
                    "boys_toilets_water": get_int('Toilet_ExclCWSN_RunWat_B'),
                    
                    # Toilets - Girls (excluding CWSN)
                    "girls_toilets_total": get_int('Toilet_ExclCWSN_G_Tot'),
                    "girls_toilets_functional": get_int('Toilet_ExclCWSN_G_Func'),
                    "girls_toilets_water": get_int('Toilet_ExclCWSN_RunWat_G'),
                    
                    # CWSN Toilets
                    "cwsn_boys_total": get_int('Toilet_CWSN_B_Tot'),
                    "cwsn_boys_functional": get_int('Toilet_CWSN_B_Func'),
                    "cwsn_boys_water": get_int('Toilet_CWSN_RunWat_B'),
                    "cwsn_girls_total": get_int('Toilet_CWSN_G_Tot'),
                    "cwsn_girls_functional": get_int('Toilet_CWSN_G_Func'),
                    "cwsn_girls_water": get_int('Toilet_CWSN_RunWat_G'),
                    
                    # Urinals
                    "urinals_boys_total": get_int('Urnl_B_Tot'),
                    "urinals_boys_functional": get_int('Urnl_B_Func'),
                    "urinals_boys_water": get_int('Urnl_RunWat_B'),
                    "urinals_girls_total": get_int('Urnl_G_Tot'),
                    "urinals_girls_functional": get_int('Urnl_G_Func'),
                    "urinals_girls_water": get_int('Urnl_RunWat_G'),
                    
                    # Under construction
                    "boys_toilets_uc": get_int('Number of Boys Toilet Under Construction'),
                    "girls_toilets_uc": get_int('Number of Girls Toilet Under Construction'),
                    
                    # Hygiene
                    "handwash_near_toilet": is_yes('HandwashFac_Toilet/Urnl'),
                    "handwash_facility": is_yes('Handwash_Facility'),
                    "handwash_points": get_int('Handwash_Points'),
                    "sanitary_pad": is_yes('Sanitary_Pad'),
                    "incinerator": is_yes('IncerAvail_GToilet'),
                    
                    "academic_year": str(row.get('Academic_Year', '2025-26')).strip() if pd.notna(row.get('Academic_Year')) else "2025-26"
                }
                
                await db.classrooms_toilets.insert_one(record)
                records_processed += 1
                
            except Exception as e:
                logging.error(f"Error processing row {idx}: {str(e)}")
                continue
        
        logging.info(f"Classrooms & Toilets import complete: {records_processed} records")
        
    except Exception as e:
        logging.error(f"Error processing Classrooms & Toilets file: {str(e)}")


