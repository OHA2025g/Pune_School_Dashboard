"""Data Entry Status Analytics Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import httpx
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/data-entry", tags=["Data Entry Status"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_data_entry_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Data Entry Status Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_students"},
                "total_students_py": {"$sum": "$total_students_py"},
                "total_completed": {"$sum": "$completed"},
                "total_in_progress": {"$sum": "$in_progress"},
                "total_not_started": {"$sum": "$not_started"},
                "total_repeaters": {"$sum": "$repeaters"},
                "certified_count": {"$sum": {"$cond": [{"$eq": ["$certified", "Yes"]}, 1, 0]}},
                "full_completion_schools": {"$sum": {"$cond": [{"$eq": ["$completion_pct", 100]}, 1, 0]}},
            }
        }
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "total_students": 0}
    
    data = result[0]
    total_schools = data.get("total_schools", 0) or 1
    total_students = data.get("total_students", 0) or 1
    total_completed = data.get("total_completed", 0) or 0
    total_in_progress = data.get("total_in_progress", 0) or 0
    total_not_started = data.get("total_not_started", 0) or 0
    total_repeaters = data.get("total_repeaters", 0) or 0
    certified_count = data.get("certified_count", 0) or 0
    full_completion_schools = data.get("full_completion_schools", 0) or 0
    
    # Calculate KPIs
    completion_rate = round((total_completed / total_students) * 100, 2) if total_students > 0 else 0
    pending_rate = round(((total_in_progress + total_not_started) / total_students) * 100, 2) if total_students > 0 else 0
    repeater_rate = round((total_repeaters / total_students) * 100, 2) if total_students > 0 else 0
    certification_rate = round((certified_count / total_schools) * 100, 1) if total_schools > 0 else 0
    full_completion_rate = round((full_completion_schools / total_schools) * 100, 1) if total_schools > 0 else 0
    avg_students_per_school = round(total_students / total_schools, 0)
    
    # Get block count
    block_count = await db.data_entry_analytics.distinct("block_name", scope_match or {})
    
    return {
        "total_schools": total_schools,
        "total_blocks": len(block_count),
        "total_students": total_students,
        "total_students_py": data.get("total_students_py", 0),
        "avg_students_per_school": avg_students_per_school,
        "total_completed": total_completed,
        "total_in_progress": total_in_progress,
        "total_not_started": total_not_started,
        "total_repeaters": total_repeaters,
        "completion_rate": completion_rate,
        "pending_rate": pending_rate,
        "in_progress_rate": round((total_in_progress / total_students) * 100, 2) if total_students > 0 else 0,
        "not_started_rate": round((total_not_started / total_students) * 100, 2) if total_students > 0 else 0,
        "repeater_rate": repeater_rate,
        "certified_schools": certified_count,
        "non_certified_schools": total_schools - certified_count,
        "certification_rate": certification_rate,
        "full_completion_schools": full_completion_schools,
        "full_completion_rate": full_completion_rate,
        "pending_students": total_in_progress + total_not_started
    }


@router.get("/block-wise")
async def get_data_entry_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise data entry status"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_students"},
                "total_completed": {"$sum": "$completed"},
                "total_in_progress": {"$sum": "$in_progress"},
                "total_not_started": {"$sum": "$not_started"},
                "total_repeaters": {"$sum": "$repeaters"},
                "certified_count": {"$sum": {"$cond": [{"$eq": ["$certified", "Yes"]}, 1, 0]}},
                "full_completion_schools": {"$sum": {"$cond": [{"$eq": ["$completion_pct", 100]}, 1, 0]}},
            }
        },
        {"$sort": {"total_students": -1}}
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    result = []
    for idx, block in enumerate(blocks):
        total_students = block.get("total_students", 0) or 1
        total_schools = block.get("total_schools", 0) or 1
        total_completed = block.get("total_completed", 0) or 0
        pending = block.get("total_in_progress", 0) + block.get("total_not_started", 0)
        
        completion_pct = round((total_completed / total_students) * 100, 2) if total_students > 0 else 0
        
        result.append({
            "rank": idx + 1,
            "block_name": block["_id"],
            "block_code": block.get("block_code", ""),
            "total_schools": block.get("total_schools", 0),
            "total_students": total_students,
            "total_completed": total_completed,
            "pending_students": pending,
            "in_progress": block.get("total_in_progress", 0),
            "not_started": block.get("total_not_started", 0),
            "completion_pct": completion_pct,
            "pending_pct": round((pending / total_students) * 100, 2) if total_students > 0 else 0,
            "total_repeaters": block.get("total_repeaters", 0),
            "repeater_rate": round((block.get("total_repeaters", 0) / total_students) * 100, 2) if total_students > 0 else 0,
            "certified_schools": block.get("certified_count", 0),
            "certification_rate": round((block.get("certified_count", 0) / total_schools) * 100, 1) if total_schools > 0 else 0,
            "full_completion_schools": block.get("full_completion_schools", 0),
            "avg_students_per_school": round(total_students / total_schools, 0)
        })
    
    return result


@router.get("/school-completion-bands")
async def get_school_completion_bands(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get distribution of schools by completion percentage bands"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$bucket": {
                "groupBy": "$completion_rate",
                "boundaries": [0, 90, 95, 99, 100, 101],
                "default": "Other",
                "output": {
                    "count": {"$sum": 1},
                    "schools": {"$push": {"name": "$school_name", "pct": "$completion_rate", "udise": "$udise_code"}}
                }
            }
        }
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    buckets = await cursor.to_list(length=10)
    
    bands = {
        "100%": 0,
        "95-99%": 0,
        "90-95%": 0,
        "<90%": 0,
        "critical_schools": []
    }
    
    for bucket in buckets:
        boundary = bucket.get("_id")
        count = bucket.get("count", 0)
        if boundary == 100:
            bands["100%"] = count
        elif boundary == 99:
            bands["95-99%"] = count
        elif boundary == 95:
            bands["90-95%"] = count
        elif boundary == 90:
            bands["<90%"] = count
        elif boundary == 0:
            bands["<90%"] += count
            # Add critical schools
            for school in bucket.get("schools", [])[:20]:
                bands["critical_schools"].append(school)
    
    return bands


@router.get("/certification-status")
async def get_certification_status(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get certification status distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$certified",
                "count": {"$sum": 1},
                "total_students": {"$sum": "$total_students"}
            }
        }
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=10)
    
    certified = {"schools": 0, "students": 0}
    non_certified = {"schools": 0, "students": 0}
    
    for r in results:
        if r["_id"] == "Yes":
            certified["schools"] = r["count"]
            certified["students"] = r["total_students"]
        else:
            non_certified["schools"] = r["count"]
            non_certified["students"] = r["total_students"]
    
    total_schools = certified["schools"] + non_certified["schools"]
    total_students = certified["students"] + non_certified["students"]
    
    return {
        "certified": certified,
        "non_certified": non_certified,
        "certification_rate": round((certified["schools"] / total_schools) * 100, 1) if total_schools > 0 else 0,
        "students_in_certified": round((certified["students"] / total_students) * 100, 1) if total_students > 0 else 0,
        "distribution": [
            {"name": "Certified", "value": certified["schools"], "color": "#10b981"},
            {"name": "Non-Certified", "value": non_certified["schools"], "color": "#ef4444"}
        ]
    }


@router.get("/repeater-analysis")
async def get_repeater_analysis(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get repeater analysis by block"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_students": {"$sum": "$total_students"},
                "total_repeaters": {"$sum": "$repeaters"},
                "schools_with_repeaters": {"$sum": {"$cond": [{"$gt": ["$repeaters", 0]}, 1, 0]}},
                "total_schools": {"$sum": 1}
            }
        },
        {"$sort": {"total_repeaters": -1}}
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    result = []
    for block in blocks:
        total_students = block.get("total_students", 0) or 1
        result.append({
            "block_name": block["_id"],
            "total_students": block.get("total_students", 0),
            "total_repeaters": block.get("total_repeaters", 0),
            "repeater_rate": round((block.get("total_repeaters", 0) / total_students) * 100, 2) if total_students > 0 else 0,
            "schools_with_repeaters": block.get("schools_with_repeaters", 0),
            "total_schools": block.get("total_schools", 0)
        })
    
    return result


@router.get("/critical-schools")
async def get_critical_schools(
    threshold: float = Query(95, description="Completion threshold"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools below completion threshold"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    cursor = db.data_entry_analytics.find(
        {**scope_match, "completion_pct": {"$lt": threshold}} if scope_match else {"completion_pct": {"$lt": threshold}},
        {"_id": 0, "udise_code": 1, "school_name": 1, "block_name": 1, 
         "total_students": 1, "total_completed": 1, "in_progress": 1, 
         "not_started": 1, "completion_pct": 1, "certified": 1}
    ).sort("completion_pct", 1).limit(50)
    
    schools = await cursor.to_list(length=50)
    return schools


@router.get("/high-repeater-schools")
async def get_high_repeater_schools(
    threshold: float = Query(5, description="Repeater rate threshold"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools with high repeater rates"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = [
        {"$match": scope_match} if scope_match else {"$match": {}},
        {
            "$addFields": {
                "repeater_rate": {
                    "$cond": [
                        {"$gt": ["$total_students", 0]},
                        {"$multiply": [{"$divide": ["$repeaters", "$total_students"]}, 100]},
                        0
                    ]
                }
            }
        },
        {"$match": {"repeater_rate": {"$gt": threshold}}},
        {"$sort": {"repeater_rate": -1}},
        {"$limit": 50},
        {
            "$project": {
                "_id": 0,
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "total_students": 1,
                "total_repeaters": 1,
                "repeater_rate": {"$round": ["$repeater_rate", 2]}
            }
        }
    ]
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    schools = await cursor.to_list(length=50)
    return schools


@router.get("/data-quality")
async def get_data_quality_metrics(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get data quality and consistency metrics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_students"},
                "total_completed": {"$sum": "$completed"},
                "total_in_progress": {"$sum": "$in_progress"},
                "total_not_started": {"$sum": "$not_started"},
                # Check for data consistency (completed + in_progress + not_started should equal total)
                "consistent_schools": {
                    "$sum": {
                        "$cond": [
                            {"$eq": [
                                {"$add": ["$completed", "$in_progress", "$not_started"]},
                                "$total_students"
                            ]},
                            1, 0
                        ]
                    }
                },
                "zero_entry_schools": {"$sum": {"$cond": [{"$eq": ["$completed", 0]}, 1, 0]}},
                "full_entry_schools": {"$sum": {"$cond": [{"$eq": ["$completion_pct", 100]}, 1, 0]}}
            }
        }
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"consistency_rate": 0}
    
    data = result[0]
    total_schools = data.get("total_schools", 0) or 1
    
    return {
        "total_schools": total_schools,
        "consistent_schools": data.get("consistent_schools", 0),
        "consistency_rate": round((data.get("consistent_schools", 0) / total_schools) * 100, 1),
        "zero_entry_schools": data.get("zero_entry_schools", 0),
        "full_entry_schools": data.get("full_entry_schools", 0),
        "mismatch_schools": total_schools - data.get("consistent_schools", 0)
    }


@router.get("/top-bottom-blocks")
async def get_top_bottom_blocks(
    n: int = Query(5, description="Number of blocks"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get top and bottom performing blocks"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    # Get all blocks sorted by completion rate
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_students": {"$sum": "$total_students"},
                "total_completed": {"$sum": "$completed"},
                "total_schools": {"$sum": 1}
            }
        },
        {
            "$addFields": {
                "completion_rate": {
                    "$cond": [
                        {"$gt": ["$total_students", 0]},
                        {"$multiply": [{"$divide": ["$total_completed", "$total_students"]}, 100]},
                        0
                    ]
                }
            }
        },
        {"$sort": {"completion_rate": -1}}
    ], scope_match)
    
    cursor = db.data_entry_analytics.aggregate(pipeline)
    all_blocks = await cursor.to_list(length=100)
    
    top_blocks = [
        {
            "block_name": b["_id"],
            "completion_rate": round(b.get("completion_rate", 0) or 0, 2),
            "total_students": b["total_students"],
            "total_schools": b["total_schools"]
        }
        for b in all_blocks[:n]
    ]
    
    bottom_blocks = [
        {
            "block_name": b["_id"],
            "completion_rate": round(b.get("completion_rate", 0) or 0, 2),
            "total_students": b["total_students"],
            "total_schools": b["total_schools"]
        }
        for b in all_blocks[-n:]
    ]
    bottom_blocks.reverse()
    
    return {
        "top_blocks": top_blocks,
        "bottom_blocks": bottom_blocks,
        "worst_block": bottom_blocks[0] if bottom_blocks else None
    }


@router.post("/import")
async def import_data_entry_status(
    background_tasks: BackgroundTasks,
    url: str = Query(None, description="URL of Excel file to import")
):
    """Import Data Entry Status data from Excel file"""
    if not url:
        url = "https://customer-assets.emergentagent.com/job_e600aca7-d1b5-4003-a850-c6b4b2f65c48/artifacts/bgxd8gox_6.%20Data%20Entry%20Status-%20School%20Wise-%20Real%20Time%20%28State%29%20MAHARASHTRA%20%283%29.xlsx"
    
    import_id = str(uuid.uuid4())[:8]
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True, timeout=60.0)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        file_path = UPLOADS_DIR / f"data_entry_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_data_entry_file, str(file_path), filename, import_id)
        
        return {"status": "processing", "import_id": import_id, "message": "Data Entry Status import started"}
    
    except Exception as e:
        logger.error(f"Data Entry Status import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_data_entry_file(file_path: str, filename: str, import_id: str):
    """Process Data Entry Status Excel file"""
    try:
        logger.info(f"Processing Data Entry Status file: {filename}")
        
        df = pd.read_excel(file_path)
        logger.info(f"Data Entry Status file loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Clear existing data
        await db.data_entry_analytics.delete_many({})
        
        records_processed = 0
        for idx, row in df.iterrows():
            try:
                udise = str(row.get('UDISE Code', '')).strip() if pd.notna(row.get('UDISE Code')) else ""
                if not udise:
                    continue
                
                total_students = int(row.get('Total Students', 0)) if pd.notna(row.get('Total Students')) else 0
                total_completed = int(row.get('Total Completed', 0)) if pd.notna(row.get('Total Completed')) else 0
                
                # Calculate completion percentage
                completion_pct = round((total_completed / total_students) * 100, 2) if total_students > 0 else 0
                
                record = {
                    "udise_code": udise,
                    "district_code": str(row.get('District Code', '')),
                    "district_name": str(row.get('District Name', '')).strip() if pd.notna(row.get('District Name')) else "",
                    "block_code": str(row.get('Block Code', '')),
                    "block_name": str(row.get('Block Name', '')).strip() if pd.notna(row.get('Block Name')) else "",
                    "school_name": str(row.get('School Name', '')).strip() if pd.notna(row.get('School Name')) else "",
                    "school_category": int(row.get('School Category', 0)) if pd.notna(row.get('School Category')) else 0,
                    "school_management": int(row.get('School Management', 0)) if pd.notna(row.get('School Management')) else 0,
                    "total_students_py": int(row.get('Total Students(Previous Year)', 0)) if pd.notna(row.get('Total Students(Previous Year)')) else 0,
                    "total_students": total_students,
                    "not_started": int(row.get('Not Started', 0)) if pd.notna(row.get('Not Started')) else 0,
                    "in_progress": int(row.get('In Progress', 0)) if pd.notna(row.get('In Progress')) else 0,
                    "total_completed": total_completed,
                    "total_repeaters": int(row.get('Total Repeaters', 0)) if pd.notna(row.get('Total Repeaters')) else 0,
                    "academic_year": str(row.get('Academic Year', '')).strip() if pd.notna(row.get('Academic Year')) else "",
                    "certified": str(row.get('Certified (Yes/No)', 'No')).strip() if pd.notna(row.get('Certified (Yes/No)')) else "No",
                    "completion_pct": completion_pct,
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.data_entry_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing data entry row: {str(e)}")
                continue
        
        logger.info(f"Data Entry Status import completed: {records_processed} records")
        
    except Exception as e:
        logger.error(f"Data Entry Status import failed: {str(e)}")


