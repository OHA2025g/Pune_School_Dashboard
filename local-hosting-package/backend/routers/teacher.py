"""Teacher Analytics Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import logging
import httpx
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/teacher", tags=["Teacher Analytics"])
logger = logging.getLogger(__name__)

# Helper functions
def safe_int_val(row, columns, possible_names):
    """Safely get integer value from row"""
    for name in possible_names:
        name_lower = name.lower().replace(' ', '_')
        for col in columns:
            if col.lower().replace(' ', '_') == name_lower:
                val = row.get(col)
                if pd.notna(val):
                    try:
                        return int(float(val))
                    except Exception:
                        pass
    return 0

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_teacher_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Teacher Analytics Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "teachers_cy": {"$sum": "$teacher_tot_cy"},
                "teachers_py": {"$sum": "$teacher_tot_py"},
                "deputation_cy": {"$sum": "$tot_teacher_deputation_cy"},
                "deputation_py": {"$sum": "$tot_teacher_deputation_py"},
                "other_school_cy": {"$sum": "$tot_teacher_teach_oth_sch_cy"},
                "other_school_py": {"$sum": "$tot_teacher_teach_oth_sch_py"},
                "cwsn_trained_cy": {"$sum": "$tot_teacher_tr_cwsn_cy"},
                "cwsn_trained_py": {"$sum": "$tot_teacher_tr_cwsn_py"},
                "computer_trained_cy": {"$sum": "$tot_teacher_tr_computers_cy"},
                "computer_trained_py": {"$sum": "$tot_teacher_tr_computers_py"},
                "ctet_cy": {"$sum": "$tot_teacher_tr_ctet_cy"},
                "ctet_py": {"$sum": "$tot_teacher_tr_ctet_py"},
                "below_grad_cy": {"$sum": "$tot_teacher_below_graduation_cy"},
                "below_grad_py": {"$sum": "$tot_teacher_below_graduation_py"},
            }
        }
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {
            "total_schools": 0,
            "teachers_cy": 0,
            "teachers_py": 0,
            "teacher_growth": 0,
            "teacher_growth_pct": 0.0,
            "avg_teachers_per_school": 0.0,
            "deputation_cy": 0,
            "deputation_ratio": 0.0,
            "other_school_cy": 0,
            "cross_school_ratio": 0.0,
            "cwsn_trained_cy": 0,
            "cwsn_coverage_pct": 0.0,
            "computer_trained_cy": 0,
            "digital_readiness_pct": 0.0,
            "ctet_cy": 0,
            "ctet_coverage_pct": 0.0,
            "below_grad_cy": 0,
            "below_grad_pct": 0.0,
            "teacher_quality_index": 0.0,
            "teacher_risk_index": 0.0
        }
    
    data = result[0]
    teachers_cy = data.get("teachers_cy", 0) or 0
    teachers_py = data.get("teachers_py", 0) or 0
    total_schools = data.get("total_schools", 0) or 0
    
    # Calculate derived metrics
    teacher_growth = teachers_cy - teachers_py
    teacher_growth_pct = round((teacher_growth / teachers_py * 100) if teachers_py > 0 else 0, 2)
    avg_teachers_per_school = round(teachers_cy / total_schools if total_schools > 0 else 0, 1)
    
    deputation_cy = data.get("deputation_cy", 0) or 0
    other_school_cy = data.get("other_school_cy", 0) or 0
    cwsn_trained_cy = data.get("cwsn_trained_cy", 0) or 0
    computer_trained_cy = data.get("computer_trained_cy", 0) or 0
    ctet_cy = data.get("ctet_cy", 0) or 0
    below_grad_cy = data.get("below_grad_cy", 0) or 0
    
    deputation_ratio = round((deputation_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    cross_school_ratio = round((other_school_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    cwsn_coverage_pct = round((cwsn_trained_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    digital_readiness_pct = round((computer_trained_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    ctet_coverage_pct = round((ctet_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    below_grad_pct = round((below_grad_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 2)
    
    # Composite indices
    teacher_quality_index = round((ctet_coverage_pct * 0.4) + (cwsn_coverage_pct * 0.3) + (digital_readiness_pct * 0.3), 1)
    teacher_risk_index = round((below_grad_pct * 0.4) + (deputation_ratio * 0.3) + (cross_school_ratio * 0.3), 1)
    
    return {
        "total_schools": total_schools,
        "teachers_cy": teachers_cy,
        "teachers_py": teachers_py,
        "teacher_growth": teacher_growth,
        "teacher_growth_pct": teacher_growth_pct,
        "avg_teachers_per_school": avg_teachers_per_school,
        "deputation_cy": deputation_cy,
        "deputation_py": data.get("deputation_py", 0) or 0,
        "deputation_ratio": deputation_ratio,
        "other_school_cy": other_school_cy,
        "cross_school_ratio": cross_school_ratio,
        "cwsn_trained_cy": cwsn_trained_cy,
        "cwsn_trained_py": data.get("cwsn_trained_py", 0) or 0,
        "cwsn_coverage_pct": cwsn_coverage_pct,
        "computer_trained_cy": computer_trained_cy,
        "computer_trained_py": data.get("computer_trained_py", 0) or 0,
        "digital_readiness_pct": digital_readiness_pct,
        "ctet_cy": ctet_cy,
        "ctet_py": data.get("ctet_py", 0) or 0,
        "ctet_coverage_pct": ctet_coverage_pct,
        "below_grad_cy": below_grad_cy,
        "below_grad_py": data.get("below_grad_py", 0) or 0,
        "below_grad_pct": below_grad_pct,
        "teacher_quality_index": teacher_quality_index,
        "teacher_risk_index": teacher_risk_index
    }


@router.get("/block-wise")
async def get_teacher_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise teacher analytics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "teachers_cy": {"$sum": "$teacher_tot_cy"},
                "teachers_py": {"$sum": "$teacher_tot_py"},
                "deputation_cy": {"$sum": "$tot_teacher_deputation_cy"},
                "other_school_cy": {"$sum": "$tot_teacher_teach_oth_sch_cy"},
                "cwsn_trained_cy": {"$sum": "$tot_teacher_tr_cwsn_cy"},
                "computer_trained_cy": {"$sum": "$tot_teacher_tr_computers_cy"},
                "ctet_cy": {"$sum": "$tot_teacher_tr_ctet_cy"},
                "below_grad_cy": {"$sum": "$tot_teacher_below_graduation_cy"},
            }
        },
        {"$sort": {"teachers_cy": -1}}
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    block_data = []
    for r in results:
        if not r["_id"]:
            continue
            
        teachers_cy = r.get("teachers_cy", 0) or 0
        teachers_py = r.get("teachers_py", 0) or 0
        total_schools = r.get("total_schools", 0) or 0
        
        growth = teachers_cy - teachers_py
        growth_pct = round((growth / teachers_py * 100) if teachers_py > 0 else 0, 1)
        avg_per_school = round(teachers_cy / total_schools if total_schools > 0 else 0, 1)
        
        deputation_cy = r.get("deputation_cy", 0) or 0
        other_school_cy = r.get("other_school_cy", 0) or 0
        cwsn_cy = r.get("cwsn_trained_cy", 0) or 0
        computer_cy = r.get("computer_trained_cy", 0) or 0
        ctet_cy = r.get("ctet_cy", 0) or 0
        below_grad_cy = r.get("below_grad_cy", 0) or 0
        
        deputation_pct = round((deputation_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        cross_school_pct = round((other_school_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        cwsn_pct = round((cwsn_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        computer_pct = round((computer_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        ctet_pct = round((ctet_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        below_grad_pct = round((below_grad_cy / teachers_cy * 100) if teachers_cy > 0 else 0, 1)
        
        # Composite indices
        quality_index = round((ctet_pct * 0.4) + (cwsn_pct * 0.3) + (computer_pct * 0.3), 1)
        risk_index = round((below_grad_pct * 0.4) + (deputation_pct * 0.3) + (cross_school_pct * 0.3), 1)
        
        block_data.append({
            "block_name": r["_id"],
            "block_code": r.get("block_code", ""),
            "total_schools": total_schools,
            "teachers_cy": teachers_cy,
            "teachers_py": teachers_py,
            "teacher_growth": growth,
            "teacher_growth_pct": growth_pct,
            "avg_teachers_per_school": avg_per_school,
            "deputation_cy": deputation_cy,
            "deputation_pct": deputation_pct,
            "cross_school_pct": cross_school_pct,
            "cwsn_pct": cwsn_pct,
            "computer_pct": computer_pct,
            "ctet_pct": ctet_pct,
            "below_grad_pct": below_grad_pct,
            "quality_index": quality_index,
            "risk_index": risk_index
        })
    
    return block_data


@router.get("/school-distribution")
async def get_teacher_school_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get school distribution by staffing change"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$project": {
                "change": {"$subtract": ["$teacher_tot_cy", "$teacher_tot_py"]}
            }
        },
        {
            "$group": {
                "_id": {
                    "$cond": [
                        {"$gt": ["$change", 0]}, "increased",
                        {"$cond": [{"$lt": ["$change", 0]}, "decreased", "no_change"]}
                    ]
                },
                "count": {"$sum": 1}
            }
        }
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=10)
    
    distribution = {"increased": 0, "decreased": 0, "no_change": 0}
    for r in results:
        if r["_id"] in distribution:
            distribution[r["_id"]] = r["count"]
    
    return [
        {"name": "Increased Staffing", "value": distribution["increased"], "color": "#10b981"},
        {"name": "Decreased Staffing", "value": distribution["decreased"], "color": "#ef4444"},
        {"name": "No Change", "value": distribution["no_change"], "color": "#6b7280"}
    ]


@router.get("/top-changes")
async def get_teacher_top_changes(
    change_type: str = Query("gain", description="gain or loss"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools with highest teacher gain or loss"""
    sort_order = -1 if change_type == "gain" else 1
    
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$project": {
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "teachers_cy": "$teacher_tot_cy",
                "teachers_py": "$teacher_tot_py",
                "change": {"$subtract": ["$teacher_tot_cy", "$teacher_tot_py"]}
            }
        },
        {"$sort": {"change": sort_order}},
        {"$limit": 20}
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    return [{
        "udise_code": r.get("udise_code", ""),
        "school_name": r.get("school_name", ""),
        "block_name": r.get("block_name", ""),
        "teachers_cy": r.get("teachers_cy", 0),
        "teachers_py": r.get("teachers_py", 0),
        "change": r.get("change", 0)
    } for r in results]


@router.get("/training-coverage")
async def get_teacher_training_coverage(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get training coverage breakdown"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_teachers": {"$sum": "$teacher_tot_cy"},
                "ctet_trained": {"$sum": "$tot_teacher_tr_ctet_cy"},
                "cwsn_trained": {"$sum": "$tot_teacher_tr_cwsn_cy"},
                "computer_trained": {"$sum": "$tot_teacher_tr_computers_cy"},
            }
        }
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    total = data.get("total_teachers", 0) or 1
    
    return [
        {
            "name": "CTET Qualified",
            "trained": data.get("ctet_trained", 0),
            "total": total,
            "percentage": round((data.get("ctet_trained", 0) / total) * 100, 1),
            "color": "#3b82f6"
        },
        {
            "name": "CWSN Trained",
            "trained": data.get("cwsn_trained", 0),
            "total": total,
            "percentage": round((data.get("cwsn_trained", 0) / total) * 100, 1),
            "color": "#8b5cf6"
        },
        {
            "name": "Computer Trained",
            "trained": data.get("computer_trained", 0),
            "total": total,
            "percentage": round((data.get("computer_trained", 0) / total) * 100, 1),
            "color": "#06b6d4"
        }
    ]


@router.get("/qualification-risk")
async def get_teacher_qualification_risk(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get blocks with high below-graduation teachers"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "teachers_cy": {"$sum": "$teacher_tot_cy"},
                "below_grad_cy": {"$sum": "$tot_teacher_below_graduation_cy"},
            }
        },
        {"$sort": {"below_grad_cy": -1}},
        {"$limit": 15}
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=15)
    
    return [{
        "block_name": r.get("_id", ""),
        "teachers_cy": r.get("teachers_cy", 0),
        "below_grad_cy": r.get("below_grad_cy", 0),
        "below_grad_pct": round((r.get("below_grad_cy", 0) / r.get("teachers_cy", 1)) * 100, 1) if r.get("teachers_cy", 0) > 0 else 0
    } for r in results if r.get("_id")]


@router.get("/deployment-risk")
async def get_teacher_deployment_risk(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get blocks with high deputation/cross-school teaching"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "teachers_cy": {"$sum": "$teacher_tot_cy"},
                "deputation_cy": {"$sum": "$tot_teacher_deputation_cy"},
                "other_school_cy": {"$sum": "$tot_teacher_teach_oth_sch_cy"},
            }
        },
        {"$sort": {"deputation_cy": -1}},
        {"$limit": 15}
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=15)
    
    return [{
        "block_name": r.get("_id", ""),
        "teachers_cy": r.get("teachers_cy", 0),
        "deputation_cy": r.get("deputation_cy", 0),
        "other_school_cy": r.get("other_school_cy", 0),
        "deputation_pct": round((r.get("deputation_cy", 0) / r.get("teachers_cy", 1)) * 100, 1) if r.get("teachers_cy", 0) > 0 else 0,
        "cross_school_pct": round((r.get("other_school_cy", 0) / r.get("teachers_cy", 1)) * 100, 1) if r.get("teachers_cy", 0) > 0 else 0
    } for r in results if r.get("_id")]


@router.get("/block-comparison")
async def get_teacher_block_comparison(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block comparison for PY vs CY"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "teachers_cy": {"$sum": "$teacher_tot_cy"},
                "teachers_py": {"$sum": "$teacher_tot_py"},
            }
        },
        {"$sort": {"teachers_cy": -1}}
    ], scope_match)
    
    cursor = db.teacher_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    return [{
        "block_name": r.get("_id", ""),
        "teachers_cy": r.get("teachers_cy", 0),
        "teachers_py": r.get("teachers_py", 0),
        "change": (r.get("teachers_cy", 0) or 0) - (r.get("teachers_py", 0) or 0)
    } for r in results if r.get("_id")]


@router.post("/import")
async def import_teacher_data(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL of the Teacher Excel file")
):
    """Import Teacher analytics data from Excel file"""
    import_id = str(uuid.uuid4())
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            response = await http_client.get(url)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        
        file_path = UPLOADS_DIR / f"teacher_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        # Process in background
        background_tasks.add_task(process_teacher_file, str(file_path), filename, import_id)
        
        return {
            "import_id": import_id,
            "status": "processing",
            "message": "Teacher data import started"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")


async def process_teacher_file(file_path: str, filename: str, import_id: str):
    """Process Teacher Excel file and store in dedicated collection"""
    try:
        logger.info(f"Processing Teacher file: {filename}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = [str(col).strip().lower().replace(' ', '_').replace('&', 'and') for col in df.columns]
        
        logger.info(f"Teacher file columns: {list(df.columns)}")
        
        # Clear existing data
        await db.teacher_analytics.delete_many({})
        
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
                
                # Extract district and block info
                district_col = next((c for c in df.columns if 'district' in c and 'name' in c), None)
                block_col = next((c for c in df.columns if 'block' in c and 'name' in c), None)
                
                district_raw = str(row[district_col]).strip() if district_col and pd.notna(row[district_col]) else ""
                block_raw = str(row[block_col]).strip() if block_col and pd.notna(row[block_col]) else ""
                
                # Parse district name and code
                district_name = district_raw.split('(')[0].strip() if '(' in district_raw else district_raw
                district_code = district_raw.split('(')[1].replace(')', '').strip() if '(' in district_raw else ""
                
                # Parse block name and code
                block_name = block_raw.split('(')[0].strip() if '(' in block_raw else block_raw
                block_code = block_raw.split('(')[1].replace(')', '').strip() if '(' in block_raw else ""
                
                # Get school name
                school_col = next((c for c in df.columns if 'school_name' in c), None)
                school_name = str(row[school_col]).strip() if school_col and pd.notna(row[school_col]) else ""
                
                # Build record with all teacher metrics
                record = {
                    "udise_code": udise,
                    "district_name": district_name,
                    "district_code": district_code,
                    "block_name": block_name,
                    "block_code": block_code,
                    "school_name": school_name,
                    "teacher_tot_py": safe_int_val(row, df.columns, ['teacher_tot_py']),
                    "teacher_tot_cy": safe_int_val(row, df.columns, ['teacher_tot_cy']),
                    "tot_teacher_deputation_py": safe_int_val(row, df.columns, ['tot_teacher_deputation_py']),
                    "tot_teacher_deputation_cy": safe_int_val(row, df.columns, ['tot_teacher_deputation_cy']),
                    "tot_teacher_teach_oth_sch_py": safe_int_val(row, df.columns, ['tot_teacher_teach_oth_sch_py']),
                    "tot_teacher_teach_oth_sch_cy": safe_int_val(row, df.columns, ['tot_teacher_teach_oth_sch_cy']),
                    "tot_teacher_tr_cwsn_py": safe_int_val(row, df.columns, ['tot_teacher_tr_cwsn_py']),
                    "tot_teacher_tr_cwsn_cy": safe_int_val(row, df.columns, ['tot_teacher_tr_cwsn_cy']),
                    "tot_teacher_tr_computers_py": safe_int_val(row, df.columns, ['tot_teacher__tr_computers_py', 'tot_teacher_tr_computers_py']),
                    "tot_teacher_tr_computers_cy": safe_int_val(row, df.columns, ['tot_teacher__tr_computers_cy', 'tot_teacher_tr_computers_cy']),
                    "tot_teacher_tr_ctet_py": safe_int_val(row, df.columns, ['tot_teacher_tr_ctet_py']),
                    "tot_teacher_tr_ctet_cy": safe_int_val(row, df.columns, ['tot_teacher_tr_ctet_cy']),
                    "tot_teacher_below_graduation_py": safe_int_val(row, df.columns, ['tot_teacher_below_graduation_py']),
                    "tot_teacher_below_graduation_cy": safe_int_val(row, df.columns, ['tot_teacher_below_graduation_cy']),
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.teacher_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing teacher row: {str(e)}")
                continue
        
        logger.info(f"Teacher import completed: {records_processed} records")
        
    except Exception as e:
        logger.error(f"Teacher import failed: {str(e)}")


