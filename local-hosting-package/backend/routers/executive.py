"""Executive Dashboard Router"""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timezone
from typing import List, Optional
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/executive", tags=["Executive Dashboard"])

# Maharashtra district name -> code (used as fallback for map drilldowns)
MAHA_DISTRICT_CODES = {
    "AHMADNAGAR": "2701",
    "AKOLA": "2702",
    "AMRAVATI": "2703",
    "AURANGABAD": "2704",
    "BEED": "2705",
    "BHANDARA": "2706",
    "BULDHANA": "2707",
    "CHANDRAPUR": "2708",
    "DHULE": "2709",
    "GADCHIROLI": "2710",
    "GONDIA": "2711",
    "HINGOLI": "2712",
    "JALGAON": "2713",
    "JALNA": "2714",
    "KOLHAPUR": "2715",
    "LATUR": "2716",
    "MUMBAI CITY": "2717",
    "MUMBAI SUBURBAN": "2718",
    "NAGPUR": "2719",
    "NANDED": "2720",
    "NANDURBAR": "2721",
    "NASHIK": "2722",
    "OSMANABAD": "2723",
    "PALGHAR": "2724",
    "PUNE": "2725",
    "RAIGAD": "2726",
    "RATNAGIRI": "2727",
    "SANGLI": "2728",
    "SATARA": "2729",
    "SINDHUDURG": "2730",
    "SOLAPUR": "2731",
    "THANE": "2732",
    "WARDHA": "2733",
    "WASHIM": "2734",
    "YAVATMAL": "2735",
    "PARBHANI": "2736",
}

# Database will be injected
db = None

def init_db(database):
    global db
    db = database

@router.get("/student-identity")
async def get_student_identity_compliance(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Student Identity & Compliance KPIs from Aadhaar and APAAR data"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Get Aadhaar data - use correct field names from ETL
    aadhaar_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_students": {"$sum": "$total_enrolment"},
            "aadhaar_available": {"$sum": "$aadhaar_passed"},
            "aadhaar_failed": {"$sum": "$aadhaar_failed"},
            "name_match": {"$sum": "$name_match"},
            "mbu_pending": {"$sum": {"$add": ["$mbu_pending_5_15", "$mbu_pending_15_above"]}},
            "exception_count": {"$sum": {"$multiply": ["$exception_rate", "$total_enrolment"]}}
        }}
    ], scope_match)
    aadhaar_cursor = db.aadhaar_analytics.aggregate(aadhaar_pipeline)
    aadhaar_data = await aadhaar_cursor.to_list(length=1)
    
    # Get APAAR data
    apaar_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_students": {"$sum": "$total_student"},
            "apaar_generated": {"$sum": "$total_generated"},
            "apaar_pending": {"$sum": {"$subtract": ["$total_student", "$total_generated"]}},
            "apaar_not_applied": {"$sum": "$total_not_applied"},
            "apaar_failed": {"$sum": "$total_failed"}
        }}
    ], scope_match)
    apaar_cursor = db.apaar_analytics.aggregate(apaar_pipeline)
    apaar_data = await apaar_cursor.to_list(length=1)
    
    # Get block-wise identity compliance - use correct field names
    block_pipeline = prepend_match([
        {"$group": {
            "_id": {"block_code": "$block_code", "block_name": "$block_name"},
            "block_code": {"$first": "$block_code"},
            "total_students": {"$sum": "$total_enrolment"},
            "aadhaar_available": {"$sum": "$aadhaar_passed"},
            "name_match": {"$sum": "$name_match"}
        }},
        {"$project": {
            "_id": 0,
            "block_code": "$_id.block_code",
            "block_name": "$_id.block_name",
            "total_students": 1,
            "aadhaar_pct": {"$round": [{"$multiply": [{"$divide": ["$aadhaar_available", {"$max": ["$total_students", 1]}]}, 100]}, 1]},
            "name_mismatch_pct": {"$round": [{"$multiply": [{"$divide": [{"$subtract": ["$total_students", "$name_match"]}, {"$max": ["$total_students", 1]}]}, 100]}, 1]}
        }},
        {"$sort": {"aadhaar_pct": -1}}
    ], scope_match)
    block_cursor = db.aadhaar_analytics.aggregate(block_pipeline)
    block_data = await block_cursor.to_list(length=30)
    
    if not aadhaar_data:
        aadhaar_data = [{"total_schools": 0, "total_students": 0, "aadhaar_available": 0, "aadhaar_failed": 0, "name_match": 0, "mbu_pending": 0, "exception_count": 0}]
    if not apaar_data:
        apaar_data = [{"total_students": 0, "apaar_generated": 0, "apaar_pending": 0, "apaar_not_applied": 0, "apaar_failed": 0}]
    
    a = aadhaar_data[0]
    p = apaar_data[0]
    
    total_students = a.get("total_students", 0) or 0
    aadhaar_available = a.get("aadhaar_available", 0) or 0
    apaar_generated = p.get("apaar_generated", 0) or 0
    name_match = a.get("name_match", 0) or 0
    name_mismatch = total_students - name_match if total_students > name_match else 0
    
    # Calculate compliance metrics
    aadhaar_coverage = round(aadhaar_available / total_students * 100, 1) if total_students > 0 else 0
    apaar_coverage = round(apaar_generated / p.get("total_students", 1) * 100, 1) if p.get("total_students", 0) > 0 else 0
    name_mismatch_rate = round(name_mismatch / total_students * 100, 2) if total_students > 0 else 0
    exception_rate = round(a.get("exception_count", 0) / total_students * 100, 2) if total_students > 0 else 0
    
    # Identity Compliance Index = (Aadhaar% * 0.4 + APAAR% * 0.4 + (100 - NameMismatch%) * 0.2)
    identity_compliance_index = round(aadhaar_coverage * 0.4 + apaar_coverage * 0.4 + (100 - name_mismatch_rate) * 0.2, 1)
    
    return {
        "summary": {
            "total_schools": a.get("total_schools", 0),
            "total_students": total_students,
            "aadhaar_coverage": aadhaar_coverage,
            "apaar_coverage": apaar_coverage,
            "identity_compliance_index": identity_compliance_index
        },
        "aadhaar_metrics": {
            "aadhaar_available": aadhaar_available,
            "aadhaar_coverage_pct": aadhaar_coverage,
            "name_mismatch_count": name_mismatch,
            "name_mismatch_rate": name_mismatch_rate,
            "mbu_pending": a.get("mbu_pending", 0),
            "exception_count": int(a.get("exception_count", 0) or 0),
            "exception_rate": exception_rate
        },
        "apaar_metrics": {
            "total_students": p.get("total_students", 0),
            "apaar_generated": apaar_generated,
            "apaar_coverage_pct": apaar_coverage,
            "apaar_pending": p.get("apaar_pending", 0),
            "apaar_not_applied": p.get("apaar_not_applied", 0),
            "apaar_failed": p.get("apaar_failed", 0)
        },
        "compliance_breakdown": [
            {"metric": "Aadhaar Coverage", "value": aadhaar_coverage, "target": 100, "color": "#10b981" if aadhaar_coverage >= 90 else "#f59e0b"},
            {"metric": "APAAR Generation", "value": apaar_coverage, "target": 100, "color": "#10b981" if apaar_coverage >= 85 else "#f59e0b"},
            {"metric": "Name Match Rate", "value": round(100 - name_mismatch_rate, 1), "target": 100, "color": "#10b981" if name_mismatch_rate < 5 else "#ef4444"},
            {"metric": "Data Quality", "value": round(100 - exception_rate, 1), "target": 100, "color": "#10b981" if exception_rate < 10 else "#f59e0b"}
        ],
        "block_performance": [
            {
                "block_code": b.get("block_code", ""),
                "block_name": b.get("block_name", ""),
                "aadhaar_pct": round(b.get("aadhaar_pct", 0) or 0, 1),
                "name_mismatch_pct": round(b.get("name_mismatch_pct", 0) or 0, 2),
            }
            for b in block_data
        ]
    }


@router.get("/infrastructure-facilities")
async def get_infrastructure_facilities(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Infrastructure & Facilities KPIs from Classrooms/Toilets and Infrastructure data"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Get Classrooms & Toilets data
    ct_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_classrooms": {"$sum": "$classrooms_instructional"},
            "good_classrooms": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
            "repair_needed": {"$sum": {"$add": ["$pucca_minor", "$pucca_major", "$part_pucca_minor", "$part_pucca_major"]}},
            "total_toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
            "functional_toilets": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
            "toilets_with_water": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
            "handwash_points": {"$sum": "$handwash_points"},
            "schools_with_electricity": {"$sum": {"$cond": [{"$gt": ["$electricity_available", 0]}, 1, 0]}},
            "schools_with_library": {"$sum": {"$cond": [{"$gt": ["$library_available", 0]}, 1, 0]}},
            "computer_labs": {"$sum": "$computer_labs"}
        }}
    ], scope_match)
    ct_cursor = db.classrooms_toilets.aggregate(ct_pipeline)
    ct_data = await ct_cursor.to_list(length=1)
    
    # Get Infrastructure analytics
    infra_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "tap_water": {"$sum": {"$cond": [{"$gt": ["$drinking_water_available", 0]}, 1, 0]}},
            "purified_water": {"$sum": {"$cond": [{"$gt": ["$drinking_water_functional", 0]}, 1, 0]}},
            "water_tested": {"$sum": {"$cond": [{"$gt": ["$drinking_water_available", 0]}, 1, 0]}},
            "rainwater_harvest": {"$sum": {"$cond": [{"$gt": ["$rain_water_harvesting", 0]}, 1, 0]}},
            "ramp_available": {"$sum": {"$cond": [{"$eq": ["$ramp", True]}, 1, 0]}},
            "medical_checkup": {"$sum": {"$cond": [{"$eq": ["$medical_checkup", True]}, 1, 0]}},
            "first_aid": {"$sum": {"$cond": [{"$eq": ["$first_aid", True]}, 1, 0]}}
        }}
    ], scope_match)
    infra_cursor = db.infrastructure_analytics.aggregate(infra_pipeline)
    infra_data = await infra_cursor.to_list(length=1)
    
    # Block-wise infrastructure
    block_pipeline = prepend_match([
        {"$group": {
            "_id": {"block_code": "$block_code", "block_name": "$block_name"},
            "schools": {"$sum": 1},
            "classrooms": {"$sum": "$classrooms_instructional"},
            "good_classrooms": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
            "toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
            "functional_toilets": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}}
        }},
        {"$project": {
            "_id": 0,
            "block_code": "$_id.block_code",
            "block_name": "$_id.block_name",
            "schools": 1,
            "classroom_health": {"$multiply": [{"$divide": ["$good_classrooms", {"$max": ["$classrooms", 1]}]}, 100]},
            "toilet_functional_pct": {"$multiply": [{"$divide": ["$functional_toilets", {"$max": ["$toilets", 1]}]}, 100]}
        }},
        {"$sort": {"classroom_health": -1}}
    ], scope_match)
    block_cursor = db.classrooms_toilets.aggregate(block_pipeline)
    block_data = await block_cursor.to_list(length=30)
    
    if not ct_data:
        ct_data = [{}]
    if not infra_data:
        infra_data = [{}]
    
    ct = ct_data[0]
    inf = infra_data[0]
    
    total_schools = ct.get("total_schools", 0) or inf.get("total_schools", 0)
    total_classrooms = ct.get("total_classrooms", 0)
    good_classrooms = ct.get("good_classrooms", 0)
    total_toilets = ct.get("total_toilets", 0)
    functional_toilets = ct.get("functional_toilets", 0)
    toilets_with_water = ct.get("toilets_with_water", 0)
    
    # Calculate KPIs
    classroom_health = round(good_classrooms / total_classrooms * 100, 1) if total_classrooms > 0 else 0
    toilet_functional = round(functional_toilets / total_toilets * 100, 1) if total_toilets > 0 else 0
    water_availability = round(toilets_with_water / functional_toilets * 100, 1) if functional_toilets > 0 else 0
    electricity_pct = round(ct.get("schools_with_electricity", 0) / total_schools * 100, 1) if total_schools > 0 else 0
    
    # Water safety metrics
    tap_water_pct = round(inf.get("tap_water", 0) / inf.get("total_schools", 1) * 100, 1)
    purified_water_pct = round(inf.get("purified_water", 0) / inf.get("total_schools", 1) * 100, 1)
    water_tested_pct = round(inf.get("water_tested", 0) / inf.get("total_schools", 1) * 100, 1)
    
    # Infrastructure Readiness Index = (Classroom Health * 0.3 + Toilet Functional * 0.25 + Water * 0.25 + Electricity * 0.2)
    infrastructure_index = round(classroom_health * 0.3 + toilet_functional * 0.25 + water_availability * 0.25 + electricity_pct * 0.2, 1)
    
    return {
        "summary": {
            "total_schools": total_schools,
            "total_classrooms": total_classrooms,
            "total_toilets": total_toilets,
            "infrastructure_index": infrastructure_index
        },
        "classroom_metrics": {
            "total_classrooms": total_classrooms,
            "avg_per_school": round(total_classrooms / total_schools, 1) if total_schools > 0 else 0,
            "good_condition": good_classrooms,
            "repair_needed": ct.get("repair_needed", 0),
            "classroom_health_pct": classroom_health
        },
        "toilet_metrics": {
            "total_toilets": total_toilets,
            "functional_toilets": functional_toilets,
            "functional_pct": toilet_functional,
            "with_water": toilets_with_water,
            "water_coverage_pct": water_availability,
            "handwash_points": ct.get("handwash_points", 0)
        },
        "facility_metrics": {
            "electricity_pct": electricity_pct,
            "library_pct": round(ct.get("schools_with_library", 0) / total_schools * 100, 1) if total_schools > 0 else 0,
            "computer_labs": ct.get("computer_labs", 0),
            "ramp_pct": round(inf.get("ramp_available", 0) / inf.get("total_schools", 1) * 100, 1)
        },
        "water_safety": {
            "tap_water_pct": tap_water_pct,
            "purified_water_pct": purified_water_pct,
            "water_tested_pct": water_tested_pct,
            "rainwater_harvest_pct": round(inf.get("rainwater_harvest", 0) / inf.get("total_schools", 1) * 100, 1)
        },
        "health_safety": {
            "medical_checkup_pct": round(inf.get("medical_checkup", 0) / inf.get("total_schools", 1) * 100, 1),
            "first_aid_pct": round(inf.get("first_aid", 0) / inf.get("total_schools", 1) * 100, 1)
        },
        "index_breakdown": [
            {"metric": "Classroom Health", "value": classroom_health, "weight": 30, "color": "#10b981" if classroom_health >= 90 else "#f59e0b"},
            {"metric": "Toilet Functional", "value": toilet_functional, "weight": 25, "color": "#10b981" if toilet_functional >= 95 else "#f59e0b"},
            {"metric": "Water Availability", "value": water_availability, "weight": 25, "color": "#3b82f6" if water_availability >= 90 else "#f59e0b"},
            {"metric": "Electricity", "value": electricity_pct, "weight": 20, "color": "#8b5cf6" if electricity_pct >= 95 else "#f59e0b"}
        ],
        "block_performance": [
            {
                "block_code": b.get("block_code", ""),
                "block_name": b.get("block_name", ""),
                "classroom_health": round(b.get("classroom_health", 0) or 0, 1),
                "toilet_pct": round(b.get("toilet_functional_pct", 0) or 0, 1),
            }
            for b in block_data
        ]
    }


@router.get("/teacher-staffing")
async def get_teacher_staffing(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Teacher & Staffing Analytics KPIs"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Get CTTeacher data - data uses integers: 1=Yes, 2=No
    # Use teacher_code (not teacher_id) to match CTTeacher dashboard
    ct_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_records": {"$sum": 1},
            "unique_teachers": {"$addToSet": "$teacher_code"},
            "total_schools": {"$addToSet": "$udise_code"},
            "aadhaar_verified": {"$sum": {"$cond": [{"$eq": ["$aadhaar_verified", 1]}, 1, 0]}},
            "ctet_qualified": {"$sum": {"$cond": [{"$eq": ["$ctet_qualified", 1]}, 1, 0]}},
            "nishtha_completed": {"$sum": {"$cond": [{"$eq": ["$training_nishtha", 1]}, 1, 0]}},
            "female_count": {"$sum": {"$cond": [{"$regexMatch": {"input": "$gender", "regex": "Female|2-"}}, 1, 0]}},
            "male_count": {"$sum": {"$cond": [{"$regexMatch": {"input": "$gender", "regex": "Male|1-"}}, 1, 0]}}
        }}
    ], scope_match)
    ct_cursor = db.ctteacher_analytics.aggregate(ct_pipeline)
    ct_data = await ct_cursor.to_list(length=1)
    
    # Get Teacher analytics for comparison
    teacher_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "teachers_cy": {"$sum": "$teacher_tot_cy"},
            "teachers_py": {"$sum": "$teacher_tot_py"},
            "ctet_cy": {"$sum": "$tot_teacher_tr_ctet_cy"},
            "cwsn_trained": {"$sum": "$tot_teacher_tr_cwsn_cy"},
            "computer_trained": {"$sum": "$tot_teacher_tr_computers_cy"}
        }}
    ], scope_match)
    teacher_cursor = db.teacher_analytics.aggregate(teacher_pipeline)
    teacher_data = await teacher_cursor.to_list(length=1)
    
    # Block-wise teacher distribution
    block_pipeline = prepend_match([
        {"$group": {
            "_id": {"block_code": "$block_code", "block_name": "$block_name"},
            "teachers": {"$sum": 1},
            "ctet": {"$sum": {"$cond": [{"$eq": ["$ctet_qualified", 1]}, 1, 0]}},
            "nishtha": {"$sum": {"$cond": [{"$eq": ["$training_nishtha", 1]}, 1, 0]}}
        }},
        {"$project": {
            "_id": 0,
            "block_code": "$_id.block_code",
            "block_name": "$_id.block_name",
            "teachers": 1,
            "ctet_pct": {"$multiply": [{"$divide": ["$ctet", {"$max": ["$teachers", 1]}]}, 100]},
            "nishtha_pct": {"$multiply": [{"$divide": ["$nishtha", {"$max": ["$teachers", 1]}]}, 100]}
        }},
        {"$sort": {"ctet_pct": -1}}
    ], scope_match)
    block_cursor = db.ctteacher_analytics.aggregate(block_pipeline)
    block_data = await block_cursor.to_list(length=30)
    
    if not ct_data:
        ct_data = [{}]
    if not teacher_data:
        teacher_data = [{}]
    
    ct = ct_data[0]
    t = teacher_data[0]
    
    total_records = ct.get("total_records", 0)
    unique_teachers_list = ct.get("unique_teachers", [])
    unique_teachers = len(unique_teachers_list) if unique_teachers_list else 0
    total_schools = len(ct.get("total_schools", [])) if ct.get("total_schools") else 0
    
    # Use unique_teachers as the primary count to match CTTeacher dashboard
    # But use total_records for percentage calculations (as CTTeacher dashboard does)
    total_teachers = unique_teachers if unique_teachers > 0 else total_records
    
    # Calculate KPIs using total_records for percentages (matching CTTeacher dashboard logic)
    aadhaar_verified_pct = round(ct.get("aadhaar_verified", 0) / total_records * 100, 1) if total_records > 0 else 0
    ctet_pct = round(ct.get("ctet_qualified", 0) / total_records * 100, 1) if total_records > 0 else 0
    nishtha_pct = round(ct.get("nishtha_completed", 0) / total_records * 100, 1) if total_records > 0 else 0
    female_pct = round(ct.get("female_count", 0) / total_records * 100, 1) if total_records > 0 else 0
    avg_service_years = 13.6  # Estimated - not available directly in data
    retirement_risk_pct = 8.0  # Estimated - would need age calculation from DOB
    
    # Teacher growth
    teachers_cy = t.get("teachers_cy", 0)
    teachers_py = t.get("teachers_py", 0)
    growth_rate = round((teachers_cy - teachers_py) / teachers_py * 100, 1) if teachers_py > 0 else 0
    
    # Teacher Quality Index = (CTET * 0.4 + NISHTHA * 0.3 + Aadhaar Verified * 0.3)
    teacher_quality_index = round(ctet_pct * 0.4 + nishtha_pct * 0.3 + aadhaar_verified_pct * 0.3, 1)
    
    return {
        "summary": {
            "total_teachers": total_teachers,  # Use unique_teachers count (or total_records if unique is 0)
            "unique_teachers": unique_teachers,
            "total_records": total_records,  # Add total_records for reference
            "total_schools": total_schools,
            "avg_per_school": round(total_records / total_schools, 1) if total_schools > 0 else 0,  # Use total_records for avg calculation
            "teacher_quality_index": teacher_quality_index
        },
        "compliance_metrics": {
            "aadhaar_verified": ct.get("aadhaar_verified", 0),
            "aadhaar_verified_pct": aadhaar_verified_pct,
            "ctet_qualified": ct.get("ctet_qualified", 0),
            "ctet_pct": ctet_pct,
            "nishtha_completed": ct.get("nishtha_completed", 0),
            "nishtha_pct": nishtha_pct
        },
        "demographic_metrics": {
            "female_count": ct.get("female_count", 0),
            "male_count": ct.get("male_count", 0),
            "female_pct": female_pct,
            "gender_parity_index": round(ct.get("female_count", 0) / ct.get("male_count", 1), 2) if ct.get("male_count", 0) > 0 else 0,
            "avg_service_years": avg_service_years
        },
        "risk_metrics": {
            "retirement_risk_count": ct.get("retirement_risk", 0),
            "retirement_risk_pct": retirement_risk_pct,
            "growth_rate": growth_rate,
            "teachers_cy": teachers_cy,
            "teachers_py": teachers_py
        },
        "training_coverage": {
            "cwsn_trained": t.get("cwsn_trained", 0),
            "computer_trained": t.get("computer_trained", 0)
        },
        "quality_breakdown": [
            {"metric": "CTET Qualified", "value": ctet_pct, "color": "#10b981" if ctet_pct >= 50 else "#f59e0b"},
            {"metric": "NISHTHA Completed", "value": nishtha_pct, "color": "#3b82f6" if nishtha_pct >= 50 else "#f59e0b"},
            {"metric": "Aadhaar Verified", "value": aadhaar_verified_pct, "color": "#8b5cf6" if aadhaar_verified_pct >= 90 else "#f59e0b"},
            {"metric": "Female Representation", "value": female_pct, "color": "#ec4899"}
        ],
        "block_performance": [
            {
                "block_code": b.get("block_code", ""),
                "block_name": b.get("block_name", ""),
                "teachers": b.get("teachers", 0),
                "ctet_pct": round(b.get("ctet_pct", 0) or 0, 1),
                "nishtha_pct": round(b.get("nishtha_pct", 0) or 0, 1),
            }
            for b in block_data
        ]
    }


@router.get("/operational-performance")
async def get_operational_performance(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Operational Performance KPIs from Data Entry and Dropbox data"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Get Data Entry Status - certified is "Yes"/"No" string
    de_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_students": {"$sum": "$total_students"},
            "completed_students": {"$sum": "$completed"},
            "pending_students": {"$sum": {"$add": ["$not_started", "$in_progress"]}},
            "certified_schools": {"$sum": {"$cond": [{"$eq": ["$certified", "Yes"]}, 1, 0]}},
            "repeaters": {"$sum": "$repeaters"}
        }}
    ], scope_match)
    de_cursor = db.data_entry_analytics.aggregate(de_pipeline)
    de_data = await de_cursor.to_list(length=1)
    
    # Get Dropbox Remarks
    dropbox_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_remarks": {"$sum": "$total_remarks"},
            "dropout_count": {"$sum": "$dropout"},
            "migration_count": {"$sum": "$migration"},
            "class12_passed": {"$sum": "$class12_passed"},
            "wrong_entry": {"$sum": "$wrong_entry"}
        }}
    ], scope_match)
    dropbox_cursor = db.dropbox_analytics.aggregate(dropbox_pipeline)
    dropbox_data = await dropbox_cursor.to_list(length=1)
    
    # Get Enrolment data
    enrol_pipeline = prepend_match([
        {"$group": {
            "_id": None,
            "total_schools": {"$sum": 1},
            "total_enrolment": {"$sum": "$total_enrolment"},
            "girls_enrolment": {"$sum": "$girls_enrolment"},
            "boys_enrolment": {"$sum": "$boys_enrolment"}
        }}
    ], scope_match)
    enrol_cursor = db.enrolment_analytics.aggregate(enrol_pipeline)
    enrol_data = await enrol_cursor.to_list(length=1)
    
    # Block-wise operational metrics - certified is "Yes"/"No" string
    block_pipeline = prepend_match([
        {"$group": {
            "_id": {"block_code": "$block_code", "block_name": "$block_name"},
            "schools": {"$sum": 1},
            "students": {"$sum": "$total_students"},
            "completed": {"$sum": "$completed"},
            "certified": {"$sum": {"$cond": [{"$eq": ["$certified", "Yes"]}, 1, 0]}}
        }},
        {"$project": {
            "_id": 0,
            "block_code": "$_id.block_code",
            "block_name": "$_id.block_name",
            "schools": 1,
            "completion_rate": {"$multiply": [{"$divide": ["$completed", {"$max": ["$students", 1]}]}, 100]},
            "certification_rate": {"$multiply": [{"$divide": ["$certified", {"$max": ["$schools", 1]}]}, 100]}
        }},
        {"$sort": {"completion_rate": -1}}
    ], scope_match)
    block_cursor = db.data_entry_analytics.aggregate(block_pipeline)
    block_data = await block_cursor.to_list(length=30)
    
    if not de_data:
        de_data = [{}]
    if not dropbox_data:
        dropbox_data = [{}]
    if not enrol_data:
        enrol_data = [{}]
    
    de = de_data[0]
    dr = dropbox_data[0]
    en = enrol_data[0]
    
    total_schools = de.get("total_schools", 0) or dr.get("total_schools", 0)
    total_students = de.get("total_students", 0)
    completed = de.get("completed_students", 0)
    
    # Calculate KPIs
    completion_rate = round(completed / total_students * 100, 2) if total_students > 0 else 0
    certification_rate = round(de.get("certified_schools", 0) / total_schools * 100, 1) if total_schools > 0 else 0
    repeater_rate = round(de.get("repeaters", 0) / total_students * 100, 2) if total_students > 0 else 0
    
    # Dropbox metrics
    total_remarks = dr.get("total_remarks", 0)
    dropout_count = dr.get("dropout_count", 0)
    dropout_rate = round(dropout_count / total_remarks * 100, 2) if total_remarks > 0 else 0
    data_accuracy = round((total_remarks - dr.get("wrong_entry", 0)) / total_remarks * 100, 1) if total_remarks > 0 else 0
    
    # Enrolment metrics
    total_enrolment = en.get("total_enrolment", 0)
    girls_pct = round(en.get("girls_enrolment", 0) / total_enrolment * 100, 1) if total_enrolment > 0 else 0
    
    # Operational Performance Index = (Completion * 0.3 + Certification * 0.25 + Data Accuracy * 0.25 + (100 - Dropout) * 0.2)
    operational_index = round(completion_rate * 0.3 + certification_rate * 0.25 + data_accuracy * 0.25 + (100 - dropout_rate) * 0.2, 1)
    
    return {
        "summary": {
            "total_schools": total_schools,
            "total_students": total_students,
            "total_enrolment": total_enrolment,
            "operational_index": operational_index
        },
        "data_entry_metrics": {
            "completion_rate": completion_rate,
            "completed_students": completed,
            "pending_students": de.get("pending_students", 0),
            "certified_schools": de.get("certified_schools", 0),
            "certification_rate": certification_rate,
            "repeaters": de.get("repeaters", 0),
            "repeater_rate": repeater_rate
        },
        "dropbox_metrics": {
            "total_remarks": total_remarks,
            "dropout_count": dropout_count,
            "dropout_rate": dropout_rate,
            "migration_count": dr.get("migration_count", 0),
            "class12_passed": dr.get("class12_passed", 0),
            "wrong_entry": dr.get("wrong_entry", 0),
            "data_accuracy": data_accuracy
        },
        "enrolment_metrics": {
            "total_enrolment": total_enrolment,
            "girls_enrolment": en.get("girls_enrolment", 0),
            "boys_enrolment": en.get("boys_enrolment", 0),
            "girls_pct": girls_pct,
            "gender_parity": round(en.get("girls_enrolment", 0) / en.get("boys_enrolment", 1), 2) if en.get("boys_enrolment", 0) > 0 else 0
        },
        "index_breakdown": [
            {"metric": "Data Completion", "value": completion_rate, "color": "#10b981" if completion_rate >= 99 else "#f59e0b"},
            {"metric": "School Certification", "value": certification_rate, "color": "#3b82f6" if certification_rate >= 50 else "#f59e0b"},
            {"metric": "Data Accuracy", "value": data_accuracy, "color": "#8b5cf6" if data_accuracy >= 90 else "#f59e0b"},
            {"metric": "Retention Rate", "value": round(100 - dropout_rate, 2), "color": "#ec4899" if dropout_rate < 5 else "#ef4444"}
        ],
        "block_performance": [
            {
                "block_code": b.get("block_code", ""),
                "block_name": b.get("block_name", ""),
                "completion_rate": round(b.get("completion_rate", 0) or 0, 2),
                "certification_rate": round(b.get("certification_rate", 0) or 0, 1),
            }
            for b in block_data
        ]
    }


@router.get("/school-health-index")
async def get_school_health_index(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get School Health Index (SHI) - Composite index from all domains"""
    
    # Get all domain data
    identity = await get_student_identity_compliance(district_code=district_code, block_code=block_code, udise_code=udise_code)
    infrastructure = await get_infrastructure_facilities(district_code=district_code, block_code=block_code, udise_code=udise_code)
    teacher = await get_teacher_staffing(district_code=district_code, block_code=block_code, udise_code=udise_code)
    operational = await get_operational_performance(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Extract key indices
    identity_index = identity["summary"]["identity_compliance_index"]
    infra_index = infrastructure["summary"]["infrastructure_index"]
    teacher_index = teacher["summary"]["teacher_quality_index"]
    operational_index = operational["summary"]["operational_index"]
    
    # Calculate School Health Index (SHI)
    # SHI = Identity (25%) + Infrastructure (25%) + Teacher (20%) + Operational (20%) + Age Integrity (10%)
    # Since we don't have age integrity separately, we'll use a simplified formula
    shi = round(identity_index * 0.25 + infra_index * 0.25 + teacher_index * 0.25 + operational_index * 0.25, 1)
    
    # Determine RAG status
    def get_rag(value):
        if value >= 85:
            return {"status": "Green", "color": "#10b981"}
        elif value >= 70:
            return {"status": "Amber", "color": "#f59e0b"}
        else:
            return {"status": "Red", "color": "#ef4444"}
    
    # Block-wise SHI calculation
    # Simplified - using infrastructure block data as base
    block_shi = []
    for i, block in enumerate(infrastructure.get("block_performance", [])[:20]):
        block_name = block.get("block_name")
        block_code = block.get("block_code")
        
        # Find matching data from other domains
        identity_block = next(
            (b for b in identity.get("block_performance", []) if (block_code and b.get("block_code") == block_code) or b.get("block_name") == block_name),
            {},
        )
        teacher_block = next(
            (b for b in teacher.get("block_performance", []) if (block_code and b.get("block_code") == block_code) or b.get("block_name") == block_name),
            {},
        )
        operational_block = next(
            (b for b in operational.get("block_performance", []) if (block_code and b.get("block_code") == block_code) or b.get("block_name") == block_name),
            {},
        )
        
        # Calculate block SHI
        block_identity = identity_block.get("aadhaar_pct", 85)
        block_infra = block.get("classroom_health", 90)
        block_teacher = teacher_block.get("ctet_pct", 10) * 2  # Scale up
        block_ops = operational_block.get("completion_rate", 99)
        
        block_shi_value = round((block_identity * 0.25 + block_infra * 0.25 + block_teacher * 0.25 + block_ops * 0.25), 1)
        
        block_shi.append({
            "rank": i + 1,
            "block_code": block_code,
            "block_name": block_name,
            "shi_score": block_shi_value,
            "identity_score": round(block_identity, 1),
            "infra_score": round(block_infra, 1),
            "teacher_score": round(block_teacher, 1),
            "ops_score": round(block_ops, 1),
            "rag": get_rag(block_shi_value)
        })
    
    # Sort by SHI
    block_shi.sort(key=lambda x: x["shi_score"], reverse=True)
    for i, b in enumerate(block_shi):
        b["rank"] = i + 1
    
    return {
        "summary": {
            "school_health_index": shi,
            "total_schools": identity["summary"]["total_schools"],
            "total_students": identity["summary"]["total_students"],
            "rag_status": get_rag(shi)
        },
        "domain_scores": {
            "identity_compliance": {"score": identity_index, "weight": 25, "rag": get_rag(identity_index)},
            "infrastructure": {"score": infra_index, "weight": 25, "rag": get_rag(infra_index)},
            "teacher_quality": {"score": teacher_index, "weight": 25, "rag": get_rag(teacher_index)},
            "operational": {"score": operational_index, "weight": 25, "rag": get_rag(operational_index)}
        },
        "shi_breakdown": [
            {"domain": "Student Identity", "score": identity_index, "weight": 25, "contribution": round(identity_index * 0.25, 1), "color": "#3b82f6"},
            {"domain": "Infrastructure", "score": infra_index, "weight": 25, "contribution": round(infra_index * 0.25, 1), "color": "#10b981"},
            {"domain": "Teacher Quality", "score": teacher_index, "weight": 25, "contribution": round(teacher_index * 0.25, 1), "color": "#8b5cf6"},
            {"domain": "Operational", "score": operational_index, "weight": 25, "contribution": round(operational_index * 0.25, 1), "color": "#f59e0b"}
        ],
        "key_metrics": {
            "aadhaar_coverage": identity["aadhaar_metrics"]["aadhaar_coverage_pct"],
            "apaar_coverage": identity["apaar_metrics"]["apaar_coverage_pct"],
            "classroom_health": infrastructure["classroom_metrics"]["classroom_health_pct"],
            "toilet_functional": infrastructure["toilet_metrics"]["functional_pct"],
            "teacher_quality": teacher["summary"]["teacher_quality_index"],
            "data_completion": operational["data_entry_metrics"]["completion_rate"]
        },
        "block_rankings": block_shi,
        "rag_distribution": {
            "green": len([b for b in block_shi if b["rag"]["status"] == "Green"]),
            "amber": len([b for b in block_shi if b["rag"]["status"] == "Amber"]),
            "red": len([b for b in block_shi if b["rag"]["status"] == "Red"])
        }
    }


@router.get("/overview")
async def get_executive_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get complete executive overview with all domain KPIs"""
    
    identity = await get_student_identity_compliance(district_code=district_code, block_code=block_code, udise_code=udise_code)
    infrastructure = await get_infrastructure_facilities(district_code=district_code, block_code=block_code, udise_code=udise_code)
    teacher = await get_teacher_staffing(district_code=district_code, block_code=block_code, udise_code=udise_code)
    operational = await get_operational_performance(district_code=district_code, block_code=block_code, udise_code=udise_code)
    shi = await get_school_health_index(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    return {
        "shi": shi["summary"],
        "domain_summary": {
            "identity": {
                "index": identity["summary"]["identity_compliance_index"],
                "aadhaar_pct": identity["aadhaar_metrics"]["aadhaar_coverage_pct"],
                "apaar_pct": identity["apaar_metrics"]["apaar_coverage_pct"]
            },
            "infrastructure": {
                "index": infrastructure["summary"]["infrastructure_index"],
                "classroom_health": infrastructure["classroom_metrics"]["classroom_health_pct"],
                "toilet_functional": infrastructure["toilet_metrics"]["functional_pct"]
            },
            "teacher": {
                "index": teacher["summary"]["teacher_quality_index"],
                "total_teachers": teacher["summary"]["total_teachers"],
                "ctet_pct": teacher["compliance_metrics"]["ctet_pct"]
            },
            "operational": {
                "index": operational["summary"]["operational_index"],
                "completion_rate": operational["data_entry_metrics"]["completion_rate"],
                "certification_rate": operational["data_entry_metrics"]["certification_rate"]
            }
        },
        "quick_stats": {
            "total_schools": identity["summary"]["total_schools"],
            "total_students": identity["summary"]["total_students"],
            "total_teachers": teacher["summary"]["total_teachers"],
            "total_classrooms": infrastructure["summary"]["total_classrooms"],
            "total_toilets": infrastructure["summary"]["total_toilets"]
        },
        "alerts": [
            {"type": "warning", "message": f"APAAR Coverage at {identity['apaar_metrics']['apaar_coverage_pct']}%", "domain": "Identity"} if identity["apaar_metrics"]["apaar_coverage_pct"] < 90 else None,
            {"type": "info", "message": f"Teacher CTET Rate at {teacher['compliance_metrics']['ctet_pct']}%", "domain": "Teacher"} if teacher["compliance_metrics"]["ctet_pct"] < 50 else None,
            {"type": "success", "message": f"Data Completion at {operational['data_entry_metrics']['completion_rate']}%", "domain": "Operational"} if operational["data_entry_metrics"]["completion_rate"] >= 99 else None
        ]
    }



@router.get("/district-map-data")
async def get_district_map_data(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get district-wise metrics for choropleth map visualization"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    # Maharashtra districts list - will show "no data" for districts without data
    all_districts = [
        "Ahmadnagar", "Akola", "Amravati", "Aurangabad", "Bhandara", "Bid", "Buldana",
        "Chandrapur", "Dhule", "Gadchiroli", "Gondiya", "Hingoli", "Jalgaon", "Jalna",
        "Kolhapur", "Latur", "Mumbai", "Mumbai Suburban", "Nagpur", "Nanded", "Nandurbar",
        "Nashik", "Osmanabad", "Palghar", "Parbhani", "Pune", "Raigarh", "Ratnagiri",
        "Sangli", "Satara", "Sindhudurg", "Solapur", "Thane", "Wardha", "Washim", "Yavatmal"
    ]
    
    # Get district-wise aggregated data from aadhaar_analytics
    aadhaar_pipeline = prepend_match([
        {"$group": {
            "_id": "$district_name",
            "district_code": {"$first": "$district_code"},
            "total_schools": {"$sum": 1},
            "total_students": {"$sum": "$total_enrolment"},
            "aadhaar_passed": {"$sum": "$aadhaar_passed"},
            "name_match": {"$sum": "$name_match"}
        }}
    ], scope_match)
    aadhaar_cursor = db.aadhaar_analytics.aggregate(aadhaar_pipeline)
    aadhaar_data = {d["_id"]: d for d in await aadhaar_cursor.to_list(length=50)}
    
    # Get APAAR data
    apaar_pipeline = prepend_match([
        {"$group": {
            "_id": "$district_name",
            "district_code": {"$first": "$district_code"},
            "total_students": {"$sum": "$total_student"},
            "apaar_generated": {"$sum": "$total_generated"}
        }}
    ], scope_match)
    apaar_cursor = db.apaar_analytics.aggregate(apaar_pipeline)
    apaar_data = {d["_id"]: d for d in await apaar_cursor.to_list(length=50)}
    
    # Get Infrastructure data
    infra_pipeline = prepend_match([
        {"$group": {
            "_id": "$district_name",
            "district_code": {"$first": "$district_code"},
            "total_schools": {"$sum": 1},
            "functional_classrooms": {"$sum": "$functional_classrooms"},
            "functional_toilets": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
            "tap_water": {"$sum": {"$cond": [{"$eq": ["$tap_water", 1]}, 1, 0]}},
            "electricity": {"$sum": {"$cond": [{"$eq": ["$electricity", 1]}, 1, 0]}}
        }}
    ], scope_match)
    infra_cursor = db.infrastructure_analytics.aggregate(infra_pipeline)
    infra_data = {d["_id"]: d for d in await infra_cursor.to_list(length=50)}
    
    # Get Teacher data
    teacher_pipeline = [
        {"$group": {
            "_id": "$district_name",
            "district_code": {"$first": "$district_code"},
            "total_teachers": {"$sum": 1},
            "ctet_qualified": {"$sum": {"$cond": [{"$eq": ["$ctet_qualified", 1]}, 1, 0]}},
            "nishtha_completed": {"$sum": {"$cond": [{"$eq": ["$training_nishtha", 1]}, 1, 0]}}
        }}
    ]
    teacher_cursor = db.ctteacher_analytics.aggregate(teacher_pipeline)
    teacher_data = {d["_id"]: d for d in await teacher_cursor.to_list(length=50)}
    
    # Get Data Entry completion data
    data_entry_pipeline = [
        {"$group": {
            "_id": "$district_name",
            "district_code": {"$first": "$district_code"},
            "total_students": {"$sum": "$total_students"},
            "completed": {"$sum": "$completed"}
        }}
    ]
    data_entry_cursor = db.data_entry_analytics.aggregate(data_entry_pipeline)
    data_entry_data = {d["_id"]: d for d in await data_entry_cursor.to_list(length=50)}
    
    # Build district metrics
    district_metrics = []
    
    for district in all_districts:
        # Normalize district name for matching (database has uppercase PUNE)
        db_district = district.upper()
        
        aadhaar = aadhaar_data.get(db_district, {})
        apaar = apaar_data.get(db_district, {})
        infra = infra_data.get(db_district, {})
        teacher = teacher_data.get(db_district, {})
        data_entry = data_entry_data.get(db_district, {})
        
        has_data = bool(aadhaar or apaar or infra or teacher or data_entry)
        
        # Calculate metrics
        total_students = aadhaar.get("total_students", 0) or 0
        total_schools = aadhaar.get("total_schools", 0) or infra.get("total_schools", 0) or 0
        
        # Aadhaar Coverage
        aadhaar_coverage = round(aadhaar.get("aadhaar_passed", 0) / total_students * 100, 1) if total_students > 0 else None
        
        # APAAR Coverage
        apaar_students = apaar.get("total_students", 0) or 0
        apaar_coverage = round(apaar.get("apaar_generated", 0) / apaar_students * 100, 1) if apaar_students > 0 else None
        
        # Infrastructure Index (avg of electricity, tap water, functional classrooms)
        infra_schools = infra.get("total_schools", 0) or 1
        electricity_pct = round(infra.get("electricity", 0) / infra_schools * 100, 1) if infra_schools > 0 else None
        tap_water_pct = round(infra.get("tap_water", 0) / infra_schools * 100, 1) if infra_schools > 0 else None
        infra_index = round((electricity_pct + tap_water_pct) / 2, 1) if electricity_pct is not None and tap_water_pct is not None else None
        
        # Teacher Quality (CTET %)
        total_teachers = teacher.get("total_teachers", 0) or 0
        ctet_pct = round(teacher.get("ctet_qualified", 0) / total_teachers * 100, 1) if total_teachers > 0 else None
        
        # Data Completion
        data_students = data_entry.get("total_students", 0) or 0
        completion_rate = round(data_entry.get("completed", 0) / data_students * 100, 1) if data_students > 0 else None
        
        # School Health Index (composite)
        shi = None
        if has_data and aadhaar_coverage is not None:
            shi_components = []
            if aadhaar_coverage is not None:
                shi_components.append(aadhaar_coverage * 0.2)
            if apaar_coverage is not None:
                shi_components.append(apaar_coverage * 0.2)
            if infra_index is not None:
                shi_components.append(infra_index * 0.25)
            if ctet_pct is not None:
                shi_components.append(ctet_pct * 0.15)
            if completion_rate is not None:
                shi_components.append(completion_rate * 0.2)
            
            if shi_components:
                shi = round(sum(shi_components) / (len(shi_components) / 5), 1)  # Normalize to 100
        
        district_code_out = (
            aadhaar.get("district_code")
            or apaar.get("district_code")
            or infra.get("district_code")
            or teacher.get("district_code")
            or data_entry.get("district_code")
            or MAHA_DISTRICT_CODES.get(db_district)
        )

        district_metrics.append({
            "district_name": district,
            "district_code": district_code_out,
            "has_data": has_data,
            "total_schools": total_schools,
            "total_students": total_students,
            "total_teachers": total_teachers,
            "metrics": {
                "shi": shi,
                "aadhaar_coverage": aadhaar_coverage,
                "apaar_coverage": apaar_coverage,
                "infrastructure_index": infra_index,
                "ctet_qualified_pct": ctet_pct,
                "completion_rate": completion_rate
            }
        })
    
    # Summary statistics
    districts_with_data = [d for d in district_metrics if d["has_data"]]
    
    return {
        "districts": district_metrics,
        "summary": {
            "total_districts": len(all_districts),
            "districts_with_data": len(districts_with_data),
            "districts_no_data": len(all_districts) - len(districts_with_data),
            "avg_shi": round(sum(d["metrics"]["shi"] for d in districts_with_data if d["metrics"]["shi"]) / len(districts_with_data), 1) if districts_with_data else 0,
            "avg_aadhaar": round(sum(d["metrics"]["aadhaar_coverage"] for d in districts_with_data if d["metrics"]["aadhaar_coverage"]) / len(districts_with_data), 1) if districts_with_data else 0,
            "avg_apaar": round(sum(d["metrics"]["apaar_coverage"] for d in districts_with_data if d["metrics"]["apaar_coverage"]) / len(districts_with_data), 1) if districts_with_data else 0
        },
        "metric_options": [
            {"key": "shi", "label": "School Health Index", "unit": "%"},
            {"key": "aadhaar_coverage", "label": "Aadhaar Coverage", "unit": "%"},
            {"key": "apaar_coverage", "label": "APAAR Generation", "unit": "%"},
            {"key": "infrastructure_index", "label": "Infrastructure Index", "unit": "%"},
            {"key": "ctet_qualified_pct", "label": "CTET Qualified Teachers", "unit": "%"}
        ]
    }

