"""Aadhaar Analytics Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import httpx
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/aadhaar", tags=["Aadhaar Analytics"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_aadhaar_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get comprehensive Aadhaar analytics overview - Executive Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    # Aggregate Aadhaar data from aadhaar_analytics collection
    pipeline = prepend_match(
        [
            {
                "$group": {
                    "_id": None,
                    "total_schools": {"$sum": 1},
                    "total_enrolment": {"$sum": "$total_enrolment"},
                    "aadhaar_passed": {"$sum": "$aadhaar_passed"},
                    "aadhaar_failed": {"$sum": "$aadhaar_failed"},
                    "aadhaar_pending": {"$sum": "$aadhaar_pending"},
                    "aadhaar_not_provided": {"$sum": "$aadhaar_not_provided"},
                    "name_match_total": {"$sum": "$name_match"},
                    "name_match_verified": {"$sum": "$name_match_verified"},
                    "mbu_pending_5_15": {"$sum": "$mbu_pending_5_15"},
                    "mbu_pending_15_plus": {"$sum": "$mbu_pending_15_plus"},
                }
            }
        ],
        scope_match,
    )
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {
            "total_schools": 0,
            "total_enrolment": 0,
            "aadhaar_coverage_pct": 0.0,
            "aadhaar_exception_pct": 0.0,
            "aadhaar_passed_pct": 0.0,
            "aadhaar_failed_pct": 0.0,
            "aadhaar_pending_pct": 0.0,
            "aadhaar_not_provided_pct": 0.0,
            "name_match_pct": 0.0,
            "name_mismatch_pct": 0.0,
            "mbu_pending_total_pct": 0.0,
            "mbu_pending_5_15_pct": 0.0,
            "mbu_pending_15_plus_pct": 0.0,
            "high_risk_schools": 0,
        }
    
    data = result[0]
    total = data.get("total_enrolment", 1) or 1
    passed = data.get("aadhaar_passed", 0)
    failed = data.get("aadhaar_failed", 0)
    pending = data.get("aadhaar_pending", 0)
    not_provided = data.get("aadhaar_not_provided", 0)
    name_match = data.get("name_match_verified", 0)
    mbu_5_15 = data.get("mbu_pending_5_15", 0)
    mbu_15_plus = data.get("mbu_pending_15_plus", 0)
    
    exceptions = failed + pending + not_provided
    
    # Count high-risk schools (exception rate > 10%)
    high_risk_query = {
        **scope_match,
        "$expr": {
            "$gt": [
                {
                    "$divide": [
                        {"$add": ["$aadhaar_failed", "$aadhaar_pending", "$aadhaar_not_provided"]},
                        {"$max": ["$total_enrolment", 1]},
                    ]
                },
                0.10,
            ]
        },
    }
    high_risk_cursor = db.aadhaar_analytics.count_documents(high_risk_query)
    high_risk_count = await high_risk_cursor if isinstance(high_risk_cursor, int) else 0
    
    return {
        "total_schools": data.get("total_schools", 0),
        "total_enrolment": total,
        "aadhaar_coverage_pct": round((passed / total) * 100, 2),
        "aadhaar_exception_pct": round((exceptions / total) * 100, 2),
        "aadhaar_passed": passed,
        "aadhaar_passed_pct": round((passed / total) * 100, 2),
        "aadhaar_failed": failed,
        "aadhaar_failed_pct": round((failed / total) * 100, 2),
        "aadhaar_pending": pending,
        "aadhaar_pending_pct": round((pending / total) * 100, 2),
        "aadhaar_not_provided": not_provided,
        "aadhaar_not_provided_pct": round((not_provided / total) * 100, 2),
        "name_match": name_match,
        "name_match_pct": round((name_match / max(passed, 1)) * 100, 2),
        "name_mismatch_pct": round(100 - (name_match / max(passed, 1)) * 100, 2),
        "mbu_pending_total": mbu_5_15 + mbu_15_plus,
        "mbu_pending_total_pct": round(((mbu_5_15 + mbu_15_plus) / total) * 100, 2),
        "mbu_pending_5_15": mbu_5_15,
        "mbu_pending_5_15_pct": round((mbu_5_15 / total) * 100, 2),
        "mbu_pending_15_plus": mbu_15_plus,
        "mbu_pending_15_plus_pct": round((mbu_15_plus / total) * 100, 2),
        "high_risk_schools": high_risk_count if isinstance(high_risk_count, int) else 0,
    }

@router.get("/block-wise")
async def get_aadhaar_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise Aadhaar analytics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_enrolment": {"$sum": "$total_enrolment"},
                "aadhaar_passed": {"$sum": "$aadhaar_passed"},
                "aadhaar_failed": {"$sum": "$aadhaar_failed"},
                "aadhaar_pending": {"$sum": "$aadhaar_pending"},
                "aadhaar_not_provided": {"$sum": "$aadhaar_not_provided"},
                "name_match_verified": {"$sum": "$name_match_verified"},
                "mbu_pending_5_15": {"$sum": "$mbu_pending_5_15"},
                "mbu_pending_15_plus": {"$sum": "$mbu_pending_15_plus"},
            }
        },
        {"$sort": {"total_enrolment": -1}}
    ], scope_match)
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    blocks = []
    total_exceptions = 0
    
    # First pass to calculate total exceptions
    temp_list = await cursor.to_list(length=100)
    for item in temp_list:
        exceptions = item.get("aadhaar_failed", 0) + item.get("aadhaar_pending", 0) + item.get("aadhaar_not_provided", 0)
        total_exceptions += exceptions
    
    # Second pass to build response
    for item in temp_list:
        total = item.get("total_enrolment", 1) or 1
        passed = item.get("aadhaar_passed", 0)
        failed = item.get("aadhaar_failed", 0)
        pending = item.get("aadhaar_pending", 0)
        not_provided = item.get("aadhaar_not_provided", 0)
        name_match = item.get("name_match_verified", 0)
        mbu_5_15 = item.get("mbu_pending_5_15", 0)
        mbu_15_plus = item.get("mbu_pending_15_plus", 0)
        exceptions = failed + pending + not_provided
        
        # Calculate performance index: 0.5×Coverage + 0.3×NameMatch + 0.2×(100–MBU%)
        coverage_pct = (passed / total) * 100
        name_match_pct = (name_match / max(passed, 1)) * 100
        mbu_pct = ((mbu_5_15 + mbu_15_plus) / total) * 100
        performance_index = round(0.5 * coverage_pct + 0.3 * name_match_pct + 0.2 * (100 - mbu_pct), 1)
        
        blocks.append({
            "block_name": item["_id"] or "Unknown",
            "block_code": item.get("block_code", ""),
            "total_schools": item.get("total_schools", 0),
            "total_enrolment": total,
            "aadhaar_passed": passed,
            "aadhaar_coverage_pct": round(coverage_pct, 2),
            "aadhaar_failed": failed,
            "aadhaar_failed_pct": round((failed / total) * 100, 2),
            "aadhaar_pending": pending,
            "aadhaar_pending_pct": round((pending / total) * 100, 2),
            "aadhaar_not_provided": not_provided,
            "aadhaar_not_provided_pct": round((not_provided / total) * 100, 2),
            "aadhaar_exception_pct": round((exceptions / total) * 100, 2),
            "name_match_pct": round(name_match_pct, 2),
            "name_mismatch_pct": round(100 - name_match_pct, 2),
            "mbu_pending_pct": round(mbu_pct, 2),
            "exception_contribution_pct": round((exceptions / max(total_exceptions, 1)) * 100, 2),
            "performance_index": performance_index,
        })
    
    return blocks

@router.get("/status-distribution")
async def get_aadhaar_status_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Aadhaar status distribution for donut chart"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match(
        [
            {
                "$group": {
                    "_id": None,
                    "passed": {"$sum": "$aadhaar_passed"},
                    "failed": {"$sum": "$aadhaar_failed"},
                    "pending": {"$sum": "$aadhaar_pending"},
                    "not_provided": {"$sum": "$aadhaar_not_provided"},
                }
            }
        ],
        scope_match,
    )
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"distribution": []}
    
    data = result[0]
    total = data.get("passed", 0) + data.get("failed", 0) + data.get("pending", 0) + data.get("not_provided", 0)
    
    return {
        "distribution": [
            {"name": "Passed", "value": data.get("passed", 0), "percentage": round((data.get("passed", 0) / max(total, 1)) * 100, 1), "color": "#10b981"},
            {"name": "Failed", "value": data.get("failed", 0), "percentage": round((data.get("failed", 0) / max(total, 1)) * 100, 1), "color": "#ef4444"},
            {"name": "Pending", "value": data.get("pending", 0), "percentage": round((data.get("pending", 0) / max(total, 1)) * 100, 1), "color": "#f59e0b"},
            {"name": "Not Provided", "value": data.get("not_provided", 0), "percentage": round((data.get("not_provided", 0) / max(total, 1)) * 100, 1), "color": "#6b7280"},
        ],
        "total": total
    }

@router.get("/high-risk-schools")
async def get_high_risk_schools(
    limit: int = Query(20, description="Number of schools"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools with highest exception rates"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = [
        {"$match": scope_match} if scope_match else {"$match": {}},
        {
            "$addFields": {
                "exception_total": {"$add": ["$aadhaar_failed", "$aadhaar_pending", "$aadhaar_not_provided"]},
                "exception_rate": {
                    "$multiply": [
                        {"$divide": [
                            {"$add": ["$aadhaar_failed", "$aadhaar_pending", "$aadhaar_not_provided"]},
                            {"$max": ["$total_enrolment", 1]}
                        ]},
                        100
                    ]
                }
            }
        },
        {"$match": {"exception_rate": {"$gt": 0}}},
        {"$sort": {"exception_rate": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "block_code": 1,
                "district_code": 1,
                "district_name": 1,
                "total_enrolment": 1,
                "aadhaar_passed": 1,
                "aadhaar_failed": 1,
                "aadhaar_pending": 1,
                "aadhaar_not_provided": 1,
                "exception_total": 1,
                "exception_rate": {"$round": ["$exception_rate", 2]},
                "aadhaar_coverage_pct": {
                    "$round": [
                        {"$multiply": [
                            {"$divide": ["$aadhaar_passed", {"$max": ["$total_enrolment", 1]}]},
                            100
                        ]},
                        2
                    ]
                }
            }
        }
    ]
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    schools = await cursor.to_list(length=limit)
    return schools

@router.get("/bottom-blocks")
async def get_bottom_blocks(
    limit: int = Query(10, description="Number of blocks"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get blocks with lowest Aadhaar coverage"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_enrolment": {"$sum": "$total_enrolment"},
                "aadhaar_passed": {"$sum": "$aadhaar_passed"},
            }
        },
        {
            "$addFields": {
                "coverage_pct": {
                    "$round": [
                        {"$multiply": [
                            {"$divide": ["$aadhaar_passed", {"$max": ["$total_enrolment", 1]}]},
                            100
                        ]},
                        2
                    ]
                }
            }
        },
        {"$sort": {"coverage_pct": 1}},
        {"$limit": limit}
    ], scope_match)
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=limit)
    return [
        {
            "block_name": b["_id"] or "Unknown",
            "block_code": b.get("block_code", ""),
            "total_schools": b.get("total_schools", 0),
            "total_enrolment": b.get("total_enrolment", 0),
            "aadhaar_passed": b.get("aadhaar_passed", 0),
            "coverage_pct": b.get("coverage_pct", 0)
        }
        for b in blocks
    ]

@router.get("/pareto-analysis")
async def get_aadhaar_pareto(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get Pareto analysis - which blocks contribute most to exceptions"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "exceptions": {"$sum": {"$add": ["$aadhaar_failed", "$aadhaar_pending", "$aadhaar_not_provided"]}},
            }
        },
        {"$sort": {"exceptions": -1}}
    ], scope_match)
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    total_exceptions = sum(b["exceptions"] for b in blocks)
    cumulative = 0
    result = []
    
    for b in blocks:
        cumulative += b["exceptions"]
        result.append({
            "block_name": b["_id"] or "Unknown",
            "exceptions": b["exceptions"],
            "contribution_pct": round((b["exceptions"] / max(total_exceptions, 1)) * 100, 2),
            "cumulative_pct": round((cumulative / max(total_exceptions, 1)) * 100, 2)
        })
    
    return result

@router.get("/mbu-analysis")
async def get_mbu_analysis(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get MBU (Manual Backlog Update) analysis by block"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_enrolment": {"$sum": "$total_enrolment"},
                "mbu_5_15": {"$sum": "$mbu_pending_5_15"},
                "mbu_15_plus": {"$sum": "$mbu_pending_15_plus"},
            }
        },
        {"$sort": {"mbu_5_15": -1}}
    ], scope_match)
    
    cursor = db.aadhaar_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    return [
        {
            "block_name": b["_id"] or "Unknown",
            "total_enrolment": b.get("total_enrolment", 0),
            "mbu_5_15": b.get("mbu_5_15", 0),
            "mbu_15_plus": b.get("mbu_15_plus", 0),
            "mbu_total": b.get("mbu_5_15", 0) + b.get("mbu_15_plus", 0),
            "mbu_pct": round(((b.get("mbu_5_15", 0) + b.get("mbu_15_plus", 0)) / max(b.get("total_enrolment", 1), 1)) * 100, 2)
        }
        for b in blocks
    ]

@router.post("/import")
async def import_aadhaar_data(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL of the Aadhaar Excel file")
):
    """Import Aadhaar analytics data from Excel file"""
    import_id = str(uuid.uuid4())
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.get(url)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        
        file_path = UPLOADS_DIR / f"aadhaar_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        # Process in background
        background_tasks.add_task(process_aadhaar_file, str(file_path), filename, import_id)
        
        return {
            "import_id": import_id,
            "status": "processing",
            "message": "Aadhaar data import started"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")

async def process_aadhaar_file(file_path: str, filename: str, import_id: str):
    """Process Aadhaar Excel file and store in dedicated collection"""
    try:
        logger.info(f"Processing Aadhaar file: {filename}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
        
        logger.info(f"Aadhaar file columns: {list(df.columns)}")
        
        # Clear existing data
        await db.aadhaar_analytics.delete_many({})
        
        records_processed = 0
        
        for _, row in df.iterrows():
            try:
                # Find UDISE column
                udise_col = next((c for c in df.columns if 'udise' in c), None)
                if not udise_col:
                    continue
                    
                udise = str(row[udise_col]).strip() if pd.notna(row[udise_col]) else None
                if not udise or udise == 'nan':
                    continue
                
                if '.' in udise:
                    udise = udise.split('.')[0]
                
                # Extract all fields
                record = {
                    "udise_code": udise,
                    "district_name": safe_str_val(row, df.columns, ['district_name', 'district']),
                    "district_code": safe_str_val(row, df.columns, ['district_code']),
                    "block_name": safe_str_val(row, df.columns, ['block_name', 'block']),
                    "block_code": safe_str_val(row, df.columns, ['block_code']),
                    "school_name": safe_str_val(row, df.columns, ['school_name', 'school']),
                    "school_management": safe_str_val(row, df.columns, ['school_management', 'management']),
                    "school_category": safe_str_val(row, df.columns, ['school_category', 'category']),
                    "total_enrolment": safe_int_val(row, df.columns, ['total_enrolment', 'total_students']),
                    "transgender_enrolment": safe_int_val(row, df.columns, ['transgender_enrolment', 'transgender']),
                    "aadhaar_not_provided": safe_int_val(row, df.columns, ['aadhaar_not_provided', 'not_provided']),
                    "aadhaar_pending": safe_int_val(row, df.columns, ['pending_aadhaar_validation', 'aadhaar_pending', 'pending']),
                    "aadhaar_failed": safe_int_val(row, df.columns, ['failed_aadhaar_validation', 'aadhaar_failed', 'failed']),
                    "aadhaar_passed": safe_int_val(row, df.columns, ['passed_aadhaar_validation', 'aadhaar_passed', 'passed']),
                    "name_match": safe_int_val(row, df.columns, ['student_name_match_with_aadhaar_name', 'name_match']),
                    "name_match_verified": safe_int_val(row, df.columns, ['student_name_match_with_aadhaar_name_(verified_aadhaar_only)', 'name_match_verified', 'verified_name_match']),
                    "mbu_pending_5_15": safe_int_val(row, df.columns, ['mbu_pending_(5-15)', 'mbu_pending_5_15', 'mbu_5_15']),
                    "mbu_pending_15_plus": safe_int_val(row, df.columns, ['mbu_pending_(15+)', 'mbu_pending_15_plus', 'mbu_15_plus', 'mbu_15+']),
                    "mbu_not_applicable": safe_int_val(row, df.columns, ['mbu_not_applicable']),
                    "status_check_pending": safe_int_val(row, df.columns, ['status_check_to_be_done', 'status_check_pending']),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.aadhaar_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing row: {str(e)}")
                continue
        
        logger.info(f"Aadhaar import completed: {records_processed} records")
        
    except Exception as e:
        logger.error(f"Aadhaar import failed: {str(e)}")

def safe_str_val(row, columns, possible_names):
    """Safely get string value from row"""
    for name in possible_names:
        if name in columns:
            val = row.get(name)
            if pd.notna(val):
                return str(val).strip()
    return ""

def safe_int_val(row, columns, possible_names):
    """Safely get integer value from row"""
    for name in possible_names:
        if name in columns:
            val = row.get(name)
            if pd.notna(val):
                try:
                    return int(float(val))
                except:
                    pass
    return 0

