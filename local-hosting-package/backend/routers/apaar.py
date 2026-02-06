"""APAAR Entry Status Analytics Router"""
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

router = APIRouter(prefix="/apaar", tags=["APAAR Status"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_apaar_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Get executive overview KPIs for APAAR Dashboard"""
    scope_match = build_scope_match(
        district_code=district_code, 
        block_code=block_code, 
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name
    )
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_student"},
                "total_generated": {"$sum": "$total_generated"},
                "total_requested": {"$sum": "$total_requested"},
                "total_failed": {"$sum": "$total_failed"},
                "total_not_applied": {"$sum": "$total_not_applied"}
            }
        }
    ], scope_match)
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "total_students": 0}
    
    data = result[0]
    total_students = data.get("total_students", 0) or 1
    total_generated = data.get("total_generated", 0) or 0
    total_requested = data.get("total_requested", 0) or 0
    total_failed = data.get("total_failed", 0) or 0
    total_not_applied = data.get("total_not_applied", 0) or 0
    pending = total_students - total_generated
    
    # Get block count with scope filter
    blocks_query = scope_match if scope_match else {}
    blocks = await db.apaar_analytics.distinct("block_name", blocks_query)
    
    generation_rate = round((total_generated / total_students) * 100, 2) if total_students > 0 else 0
    pending_pct = round((pending / total_students) * 100, 2) if total_students > 0 else 0
    not_applied_pct = round((total_not_applied / total_students) * 100, 2) if total_students > 0 else 0
    failure_rate = round((total_failed / total_students) * 1000, 2) if total_students > 0 else 0
    
    return {
        "total_schools": data.get("total_schools", 0),
        "total_blocks": len(blocks),
        "total_students": total_students,
        "total_generated": total_generated,
        "generation_rate": generation_rate,
        "total_pending": pending,
        "pending_pct": pending_pct,
        "total_requested": total_requested,
        "total_failed": total_failed,
        "failure_rate_per_1000": failure_rate,
        "total_not_applied": total_not_applied,
        "not_applied_pct": not_applied_pct
    }


@router.get("/status-funnel")
async def get_apaar_status_funnel(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get status funnel data"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "generated": {"$sum": "$total_generated"},
                "requested": {"$sum": "$total_requested"},
                "failed": {"$sum": "$total_failed"},
                "not_applied": {"$sum": "$total_not_applied"}
            }
        }
    ], scope_match)
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    return [
        {"status": "Generated", "count": data.get("generated", 0), "color": "#10b981"},
        {"status": "Not Applied", "count": data.get("not_applied", 0), "color": "#f59e0b"},
        {"status": "Failed", "count": data.get("failed", 0), "color": "#ef4444"},
        {"status": "Requested", "count": data.get("requested", 0), "color": "#3b82f6"}
    ]


@router.get("/block-wise")
async def get_apaar_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise APAAR performance"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_student"},
                "total_generated": {"$sum": "$total_generated"},
                "total_requested": {"$sum": "$total_requested"},
                "total_failed": {"$sum": "$total_failed"},
                "total_not_applied": {"$sum": "$total_not_applied"}
            }
        },
        {"$sort": {"total_students": -1}}
    ], scope_match)
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    # Calculate total pending for priority index
    total_pending_all = sum(b.get("total_students", 0) - b.get("total_generated", 0) for b in blocks)
    
    result = []
    for idx, block in enumerate(blocks):
        total_students = block.get("total_students", 0) or 1
        total_generated = block.get("total_generated", 0) or 0
        pending = total_students - total_generated
        
        generation_rate = round((total_generated / total_students) * 100, 1) if total_students > 0 else 0
        pending_pct = round((pending / total_students) * 100, 1) if total_students > 0 else 0
        not_applied_pct = round((block.get("total_not_applied", 0) / total_students) * 100, 1) if total_students > 0 else 0
        priority_index = round((pending / total_pending_all) * 100, 1) if total_pending_all > 0 else 0
        
        result.append({
            "rank": idx + 1,
            "block_name": block["_id"],
            "total_schools": block.get("total_schools", 0),
            "total_students": total_students,
            "total_generated": total_generated,
            "generation_rate": generation_rate,
            "pending": pending,
            "pending_pct": pending_pct,
            "total_not_applied": block.get("total_not_applied", 0),
            "not_applied_pct": not_applied_pct,
            "total_failed": block.get("total_failed", 0),
            "failure_rate": round((block.get("total_failed", 0) / total_students) * 1000, 1) if total_students > 0 else 0,
            "priority_index": priority_index
        })
    
    return result


@router.get("/class-wise")
async def get_apaar_class_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get class-wise APAAR generation"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    # Define class columns
    classes = ['PP3', 'PP2', 'PP1', 'Class1', 'Class2', 'Class3', 'Class4', 'Class5', 
               'Class6', 'Class7', 'Class8', 'Class9', 'Class10', 'Class11', 'Class12']
    
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                **{f"{c}_students": {"$sum": f"${c.lower()}_total_student"} for c in classes},
                **{f"{c}_generated": {"$sum": f"${c.lower()}_total_generated"} for c in classes},
                **{f"{c}_not_applied": {"$sum": f"${c.lower()}_not_applied"} for c in classes}
            }
        }
    ], scope_match)
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    class_data = []
    
    for cls in classes:
        students = data.get(f"{cls}_students", 0) or 0
        generated = data.get(f"{cls}_generated", 0) or 0
        not_applied = data.get(f"{cls}_not_applied", 0) or 0
        
        if students > 0:
            class_data.append({
                "class": cls,
                "total_students": students,
                "total_generated": generated,
                "generation_rate": round((generated / students) * 100, 1),
                "pending": students - generated,
                "not_applied": not_applied,
                "not_applied_pct": round((not_applied / students) * 100, 1)
            })
    
    return class_data


@router.get("/top-pending-schools")
async def get_top_pending_schools(
    n: int = Query(20, description="Number of schools"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get top schools by pending APAAR count"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    match_stage = {"total_student": {"$gt": 0}}
    if scope_match:
        match_stage.update(scope_match)
    pipeline = [
        {"$match": match_stage},
        {
            "$addFields": {
                "pending": {"$subtract": ["$total_student", "$total_generated"]},
                "generation_rate": {
                    "$multiply": [{"$divide": ["$total_generated", "$total_student"]}, 100]
                }
            }
        },
        {"$sort": {"pending": -1}},
        {"$limit": n},
        {
            "$project": {
                "_id": 0,
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "total_student": 1,
                "total_generated": 1,
                "pending": 1,
                "generation_rate": {"$round": ["$generation_rate", 1]},
                "total_not_applied": 1,
                "total_failed": 1
            }
        }
    ]
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    schools = await cursor.to_list(length=n)
    
    return [{"rank": idx + 1, **school} for idx, school in enumerate(schools)]


@router.get("/low-performing-schools")
async def get_low_performing_schools(
    threshold: float = Query(80, description="Generation rate threshold"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools below generation rate threshold"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    match_stage = {"total_student": {"$gt": 50}}
    if scope_match:
        match_stage.update(scope_match)
    pipeline = [
        {"$match": match_stage},
        {
            "$addFields": {
                "generation_rate": {
                    "$multiply": [{"$divide": ["$total_generated", "$total_student"]}, 100]
                }
            }
        },
        {"$match": {"generation_rate": {"$lt": threshold}}},
        {"$sort": {"generation_rate": 1}},
        {"$limit": 50},
        {
            "$project": {
                "_id": 0,
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "total_student": 1,
                "total_generated": 1,
                "generation_rate": {"$round": ["$generation_rate", 1]},
                "total_not_applied": 1,
                "total_failed": 1
            }
        }
    ]
    
    cursor = db.apaar_analytics.aggregate(pipeline)
    schools = await cursor.to_list(length=50)
    
    return schools


@router.get("/risk-schools")
async def get_risk_schools(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get high-risk schools requiring intervention"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    # Critical: Gen% < 70% AND Students > 200
    critical_match = {"total_student": {"$gt": 200}}
    if scope_match:
        critical_match.update(scope_match)
    critical_pipeline = [
        {"$match": critical_match},
        {
            "$addFields": {
                "generation_rate": {
                    "$multiply": [{"$divide": ["$total_generated", "$total_student"]}, 100]
                }
            }
        },
        {"$match": {"generation_rate": {"$lt": 70}}},
        {"$sort": {"generation_rate": 1}},
        {"$limit": 20}
    ]
    
    cursor = db.apaar_analytics.aggregate(critical_pipeline)
    critical = await cursor.to_list(length=20)
    
    # High failure: Failed > 50
    failure_match = {"total_failed": {"$gt": 50}}
    if scope_match:
        failure_match.update(scope_match)
    failure_pipeline = [
        {"$match": failure_match},
        {"$sort": {"total_failed": -1}},
        {"$limit": 20}
    ]
    
    cursor = db.apaar_analytics.aggregate(failure_pipeline)
    high_failure = await cursor.to_list(length=20)
    
    # High not applied: Not Applied > 60%
    consent_match = {"total_student": {"$gt": 100}}
    if scope_match:
        consent_match.update(scope_match)
    consent_pipeline = [
        {"$match": consent_match},
        {
            "$addFields": {
                "not_applied_pct": {
                    "$multiply": [{"$divide": ["$total_not_applied", "$total_student"]}, 100]
                }
            }
        },
        {"$match": {"not_applied_pct": {"$gt": 60}}},
        {"$sort": {"not_applied_pct": -1}},
        {"$limit": 20}
    ]
    
    cursor = db.apaar_analytics.aggregate(consent_pipeline)
    consent_gap = await cursor.to_list(length=20)
    
    return {
        "critical_schools": len(critical),
        "high_failure_schools": len(high_failure),
        "consent_gap_schools": len(consent_gap),
        "critical_list": [{"udise_code": s["udise_code"], "school_name": s["school_name"], "block_name": s["block_name"], "students": s["total_student"], "gen_rate": round(s["generation_rate"], 1)} for s in critical[:10]],
        "failure_list": [{"udise_code": s["udise_code"], "school_name": s["school_name"], "block_name": s["block_name"], "failed": s["total_failed"]} for s in high_failure[:10]],
        "consent_list": [{"udise_code": s["udise_code"], "school_name": s["school_name"], "block_name": s["block_name"], "not_applied_pct": round(s["not_applied_pct"], 1)} for s in consent_gap[:10]]
    }


@router.post("/import")
async def import_apaar_status(
    background_tasks: BackgroundTasks,
    url: str = Query(None, description="URL of Excel file to import")
):
    """Import APAAR Entry Status data from Excel file"""
    if not url:
        url = "https://customer-assets.emergentagent.com/job_e600aca7-d1b5-4003-a850-c6b4b2f65c48/artifacts/48nc19ll_9.%20APAAR%20Entry%20Status%20-%20School%20Wise%20%28Only%20operational%29%20-%20%28%20State%20%29%20MAHARASHTRA%20%281%29.xlsx"
    
    import_id = str(uuid.uuid4())[:8]
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True, timeout=120.0)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        file_path = UPLOADS_DIR / f"apaar_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_apaar_file, str(file_path), filename, import_id)
        
        return {"status": "processing", "import_id": import_id, "message": "APAAR import started"}
    
    except Exception as e:
        logging.error(f"APAAR import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_apaar_file(file_path: str, filename: str, import_id: str):
    """Process APAAR Entry Status Excel file"""
    try:
        logging.info(f"Processing APAAR file: {filename}")
        
        df = pd.read_excel(file_path)
        logging.info(f"APAAR file loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Clear existing data
        await db.apaar_analytics.delete_many({})
        
        records_processed = 0
        for idx, row in df.iterrows():
            try:
                udise = str(row.get('UDISE Code', '')).strip() if pd.notna(row.get('UDISE Code')) else ""
                if not udise:
                    continue
                
                year = str(row.get('Year', '')).strip() if pd.notna(row.get('Year')) else ""
                
                record = {
                    "udise_code": udise,
                    "district_name": str(row.get('District Name', '')).strip() if pd.notna(row.get('District Name')) else "",
                    "block_code": str(row.get('Block Code', '')).strip() if pd.notna(row.get('Block Code')) else "",
                    "block_name": str(row.get('Block Name', '')).strip() if pd.notna(row.get('Block Name')) else "",
                    "school_name": str(row.get('School Name', '')).strip() if pd.notna(row.get('School Name')) else "",
                    "school_management": int(row.get('School Management', 0)) if pd.notna(row.get('School Management')) else 0,
                    "school_category": int(row.get('School Category', 0)) if pd.notna(row.get('School Category')) else 0,
                    "year": year,
                    "total_student": int(row.get('Total Student', 0)) if pd.notna(row.get('Total Student')) else 0,
                    "total_generated": int(row.get('Total Generated', 0)) if pd.notna(row.get('Total Generated')) else 0,
                    "total_requested": int(row.get('Total Requested', 0)) if pd.notna(row.get('Total Requested')) else 0,
                    "total_failed": int(row.get('Total Failed', 0)) if pd.notna(row.get('Total Failed')) else 0,
                    "total_not_applied": int(row.get('Total Not Applied', 0)) if pd.notna(row.get('Total Not Applied')) else 0,
                    # Class-wise data
                    "pp3_total_student": int(row.get('PP3 Total Student', 0)) if pd.notna(row.get('PP3 Total Student')) else 0,
                    "pp3_total_generated": int(row.get('PP3 Total APAAR Generated', 0)) if pd.notna(row.get('PP3 Total APAAR Generated')) else 0,
                    "pp3_not_applied": int(row.get('PP3 APAAR Not Applied', 0)) if pd.notna(row.get('PP3 APAAR Not Applied')) else 0,
                    "pp2_total_student": int(row.get('PP2 Total Student', 0)) if pd.notna(row.get('PP2 Total Student')) else 0,
                    "pp2_total_generated": int(row.get('PP2 Total APAAR Generated', 0)) if pd.notna(row.get('PP2 Total APAAR Generated')) else 0,
                    "pp2_not_applied": int(row.get('PP2 APAAR Not Applied', 0)) if pd.notna(row.get('PP2 APAAR Not Applied')) else 0,
                    "pp1_total_student": int(row.get('PP1 Total Student', 0)) if pd.notna(row.get('PP1 Total Student')) else 0,
                    "pp1_total_generated": int(row.get('PP1 Total APAAR Generated', 0)) if pd.notna(row.get('PP1 Total APAAR Generated')) else 0,
                    "pp1_not_applied": int(row.get('PP1 APAAR Not Applied', 0)) if pd.notna(row.get('PP1 APAAR Not Applied')) else 0,
                    "class1_total_student": int(row.get('Class1 Total Student', 0)) if pd.notna(row.get('Class1 Total Student')) else 0,
                    "class1_total_generated": int(row.get('Class1 Total APAAR Generated', 0)) if pd.notna(row.get('Class1 Total APAAR Generated')) else 0,
                    "class1_not_applied": int(row.get('Class1 APAAR Not Applied', 0)) if pd.notna(row.get('Class1 APAAR Not Applied')) else 0,
                    "class2_total_student": int(row.get('Class2 Total Student', 0)) if pd.notna(row.get('Class2 Total Student')) else 0,
                    "class2_total_generated": int(row.get('Class2 Total APAAR Generated', 0)) if pd.notna(row.get('Class2 Total APAAR Generated')) else 0,
                    "class2_not_applied": int(row.get('Class2 APAAR Not Applied', 0)) if pd.notna(row.get('Class2 APAAR Not Applied')) else 0,
                    "class3_total_student": int(row.get('Class3 Total Student', 0)) if pd.notna(row.get('Class3 Total Student')) else 0,
                    "class3_total_generated": int(row.get('Class3 Total APAAR Generated', 0)) if pd.notna(row.get('Class3 Total APAAR Generated')) else 0,
                    "class3_not_applied": int(row.get('Class3 APAAR Not Applied', 0)) if pd.notna(row.get('Class3 APAAR Not Applied')) else 0,
                    "class4_total_student": int(row.get('Class4 Total Student', 0)) if pd.notna(row.get('Class4 Total Student')) else 0,
                    "class4_total_generated": int(row.get('Class4 Total APAAR Generated', 0)) if pd.notna(row.get('Class4 Total APAAR Generated')) else 0,
                    "class4_not_applied": int(row.get('Class4 APAAR Not Applied', 0)) if pd.notna(row.get('Class4 APAAR Not Applied')) else 0,
                    "class5_total_student": int(row.get('Class5 Total Student', 0)) if pd.notna(row.get('Class5 Total Student')) else 0,
                    "class5_total_generated": int(row.get('Class5 Total APAAR Generated', 0)) if pd.notna(row.get('Class5 Total APAAR Generated')) else 0,
                    "class5_not_applied": int(row.get('Class5 APAAR Not Applied', 0)) if pd.notna(row.get('Class5 APAAR Not Applied')) else 0,
                    "class6_total_student": int(row.get('Class6 Total Student', 0)) if pd.notna(row.get('Class6 Total Student')) else 0,
                    "class6_total_generated": int(row.get('Class6 Total APAAR Generated', 0)) if pd.notna(row.get('Class6 Total APAAR Generated')) else 0,
                    "class6_not_applied": int(row.get('Class6 APAAR Not Applied', 0)) if pd.notna(row.get('Class6 APAAR Not Applied')) else 0,
                    "class7_total_student": int(row.get('Class7 Total Student', 0)) if pd.notna(row.get('Class7 Total Student')) else 0,
                    "class7_total_generated": int(row.get('Class7 Total APAAR Generated', 0)) if pd.notna(row.get('Class7 Total APAAR Generated')) else 0,
                    "class7_not_applied": int(row.get('Class7 APAAR Not Applied', 0)) if pd.notna(row.get('Class7 APAAR Not Applied')) else 0,
                    "class8_total_student": int(row.get('Class8 Total Student', 0)) if pd.notna(row.get('Class8 Total Student')) else 0,
                    "class8_total_generated": int(row.get('Class8 Total APAAR Generated', 0)) if pd.notna(row.get('Class8 Total APAAR Generated')) else 0,
                    "class8_not_applied": int(row.get('Class8 APAAR Not Applied', 0)) if pd.notna(row.get('Class8 APAAR Not Applied')) else 0,
                    "class9_total_student": int(row.get('Class9 Total Student', 0)) if pd.notna(row.get('Class9 Total Student')) else 0,
                    "class9_total_generated": int(row.get('Class9 Total APAAR Generated', 0)) if pd.notna(row.get('Class9 Total APAAR Generated')) else 0,
                    "class9_not_applied": int(row.get('Class9 APAAR Not Applied', 0)) if pd.notna(row.get('Class9 APAAR Not Applied')) else 0,
                    "class10_total_student": int(row.get('Class10 Total Student', 0)) if pd.notna(row.get('Class10 Total Student')) else 0,
                    "class10_total_generated": int(row.get('Class10 Total APAAR Generated', 0)) if pd.notna(row.get('Class10 Total APAAR Generated')) else 0,
                    "class10_not_applied": int(row.get('Class10 APAAR Not Applied', 0)) if pd.notna(row.get('Class10 APAAR Not Applied')) else 0,
                    "class11_total_student": int(row.get('Class11 Total Student', 0)) if pd.notna(row.get('Class11 Total Student')) else 0,
                    "class11_total_generated": int(row.get('Class11 Total APAAR Generated', 0)) if pd.notna(row.get('Class11 Total APAAR Generated')) else 0,
                    "class11_not_applied": int(row.get('Class11 APAAR Not Applied', 0)) if pd.notna(row.get('Class11 APAAR Not Applied')) else 0,
                    "class12_total_student": int(row.get('Class12 Total Student', 0)) if pd.notna(row.get('Class12 Total Student')) else 0,
                    "class12_total_generated": int(row.get('Class12 Total APAAR Generated', 0)) if pd.notna(row.get('Class12 Total APAAR Generated')) else 0,
                    "class12_not_applied": int(row.get('Class12 APAAR Not Applied', 0)) if pd.notna(row.get('Class12 APAAR Not Applied')) else 0,
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.apaar_analytics.insert_one(record)
                records_processed += 1
                
            except Exception as e:
                logging.error(f"Error processing APAAR row {idx}: {str(e)}")
                continue
        
        logging.info(f"APAAR import completed: {records_processed} records")
        
    except Exception as e:
        logging.error(f"APAAR import failed: {str(e)}")


# Search
@router.get("/search")
async def search(
    q: str = Query(..., description="Search query"),
    type: str = Query("all", description="Search type: all, district, block, school")
):
    """Search for districts, blocks, or schools"""
    results = {"districts": [], "blocks": [], "schools": []}
    q_lower = q.lower()
    
    has_data = await get_data_from_db()
    
    if type in ["all", "district"]:
        districts = await get_districts_from_db() if has_data else generate_mock_district_data()
        results["districts"] = [
            {"code": d.district_code, "name": d.district_name, "shi_score": d.shi_score}
            for d in districts if q_lower in d.district_name.lower()
        ]
    
    if type in ["all", "school"] and has_data:
        cursor = db.schools.find(
            {"school_name": {"$regex": q, "$options": "i"}},
            {"_id": 0, "udise_code": 1, "school_name": 1, "district_name": 1}
        ).limit(20)
        async for doc in cursor:
            results["schools"].append(doc)
    
    return results

