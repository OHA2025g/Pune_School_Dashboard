"""Dropbox Remarks Analytics Router"""
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import aiofiles
import uuid
from pathlib import Path
import httpx
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/dropbox", tags=["Dropbox Remarks"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

# Helper functions
def safe_str_val(row, columns, possible_names):
    """Safely get string value from row"""
    for name in possible_names:
        name_lower = name.lower().replace(' ', '_')
        for col in columns:
            if col.lower().replace(' ', '_') == name_lower:
                val = row.get(col)
                if pd.notna(val):
                    return str(val).strip()
    return ""

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
                    except:
                        pass
    return 0

@router.get("/overview")
async def get_dropbox_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Dropbox Remarks Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_schools": {"$sum": 1},
                "schools_with_remarks": {"$sum": {"$cond": [{"$gt": ["$total_remarks", 0]}, 1, 0]}},
                "dropout": {"$sum": "$dropout"},
                "active_import": {"$sum": "$active_import"},
                "migrated_domestic": {"$sum": "$migrated_domestic"},
                "migrated_country": {"$sum": "$migrated_country"},
                "iti_polytechnic": {"$sum": "$iti_polytechnic"},
                "non_regular": {"$sum": "$non_regular"},
                "open_schooling": {"$sum": "$open_schooling"},
                "wrong_entry": {"$sum": "$wrong_entry"},
                "due_to_death": {"$sum": "$due_to_death"},
                "class12_passed": {"$sum": "$class12_passed"},
                "total_remarks": {"$sum": "$total_remarks"},
            }
        }
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "total_remarks": 0}
    
    data = result[0]
    total_schools = data.get("total_schools", 0) or 1
    total_remarks = data.get("total_remarks", 0) or 1
    
    # Calculate derived metrics
    reporting_rate = round((data.get("schools_with_remarks", 0) / total_schools) * 100, 1)
    avg_remarks = round(total_remarks / total_schools, 1)
    
    # Data quality metrics
    wrong_entry = data.get("wrong_entry", 0) or 0
    active_import = data.get("active_import", 0) or 0
    data_accuracy = round((1 - (wrong_entry / total_remarks)) * 100, 1) if total_remarks > 0 else 100
    data_risk_index = round(((wrong_entry + active_import) / total_remarks) * 100, 1) if total_remarks > 0 else 0
    
    # Category percentages
    dropout = data.get("dropout", 0) or 0
    dropout_pct = round((dropout / total_remarks) * 100, 2) if total_remarks > 0 else 0
    
    return {
        "total_schools": total_schools,
        "schools_with_remarks": data.get("schools_with_remarks", 0),
        "reporting_rate": reporting_rate,
        "total_remarks": total_remarks,
        "avg_remarks_per_school": avg_remarks,
        # Categories
        "dropout": dropout,
        "dropout_pct": dropout_pct,
        "active_import": active_import,
        "migrated_domestic": data.get("migrated_domestic", 0),
        "migrated_country": data.get("migrated_country", 0),
        "iti_polytechnic": data.get("iti_polytechnic", 0),
        "non_regular": data.get("non_regular", 0),
        "open_schooling": data.get("open_schooling", 0),
        "wrong_entry": wrong_entry,
        "due_to_death": data.get("due_to_death", 0),
        "class12_passed": data.get("class12_passed", 0),
        # Quality metrics
        "data_accuracy": data_accuracy,
        "data_risk_index": data_risk_index,
        "clean_data_ratio": data_accuracy
    }


@router.get("/category-distribution")
async def get_dropbox_category_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get remark category volume distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "dropout": {"$sum": "$dropout"},
                "active_import": {"$sum": "$active_import"},
                "migrated_domestic": {"$sum": "$migrated_domestic"},
                "migrated_country": {"$sum": "$migrated_country"},
                "iti_polytechnic": {"$sum": "$iti_polytechnic"},
                "non_regular": {"$sum": "$non_regular"},
                "open_schooling": {"$sum": "$open_schooling"},
                "wrong_entry": {"$sum": "$wrong_entry"},
                "due_to_death": {"$sum": "$due_to_death"},
                "class12_passed": {"$sum": "$class12_passed"},
            }
        }
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    
    categories = [
        {"name": "Class 12 Passed", "key": "class12_passed", "color": "#10b981", "type": "positive"},
        {"name": "Active for Import", "key": "active_import", "color": "#f59e0b", "type": "pending"},
        {"name": "Migration (Domestic)", "key": "migrated_domestic", "color": "#3b82f6", "type": "transition"},
        {"name": "ITI/Polytechnic", "key": "iti_polytechnic", "color": "#8b5cf6", "type": "transition"},
        {"name": "Non-Regular Mode", "key": "non_regular", "color": "#06b6d4", "type": "transition"},
        {"name": "Open Schooling", "key": "open_schooling", "color": "#ec4899", "type": "transition"},
        {"name": "Drop Out", "key": "dropout", "color": "#ef4444", "type": "critical"},
        {"name": "Wrong Entry/Duplicate", "key": "wrong_entry", "color": "#f97316", "type": "error"},
        {"name": "Migration (Country)", "key": "migrated_country", "color": "#6366f1", "type": "transition"},
        {"name": "Due to Death", "key": "due_to_death", "color": "#64748b", "type": "critical"},
    ]
    
    return [{
        "name": cat["name"],
        "value": data.get(cat["key"], 0) or 0,
        "color": cat["color"],
        "type": cat["type"]
    } for cat in categories]


@router.get("/block-wise")
async def get_dropbox_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise dropbox analytics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_remarks": {"$sum": "$total_remarks"},
                "dropout": {"$sum": "$dropout"},
                "wrong_entry": {"$sum": "$wrong_entry"},
                "active_import": {"$sum": "$active_import"},
                "class12_passed": {"$sum": "$class12_passed"},
                "migrated_domestic": {"$sum": "$migrated_domestic"},
                "iti_polytechnic": {"$sum": "$iti_polytechnic"},
            }
        },
        {"$sort": {"total_remarks": -1}}
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    block_data = []
    for r in results:
        if not r["_id"]:
            continue
        
        total = r.get("total_remarks", 0) or 1
        schools = r.get("total_schools", 0) or 1
        
        avg_load = round(total / schools, 1)
        dropout_pct = round((r.get("dropout", 0) / total) * 100, 1) if total > 0 else 0
        error_pct = round((r.get("wrong_entry", 0) / total) * 100, 1) if total > 0 else 0
        
        block_data.append({
            "block_name": r["_id"],
            "block_code": r.get("block_code", ""),
            "total_schools": schools,
            "total_remarks": total,
            "avg_remarks_per_school": avg_load,
            "dropout": r.get("dropout", 0),
            "dropout_pct": dropout_pct,
            "wrong_entry": r.get("wrong_entry", 0),
            "error_pct": error_pct,
            "active_import": r.get("active_import", 0),
            "class12_passed": r.get("class12_passed", 0),
            "migrated_domestic": r.get("migrated_domestic", 0),
            "iti_polytechnic": r.get("iti_polytechnic", 0)
        })
    
    return block_data


@router.get("/top-schools")
async def get_dropbox_top_schools(
    order: str = Query("desc", description="desc for highest, asc for lowest"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get schools with highest/lowest remarks"""
    sort_order = -1 if order == "desc" else 1
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    
    pipeline = [
        {"$match": {**scope_match, "total_remarks": {"$gt": 0}}} if scope_match else {"$match": {"total_remarks": {"$gt": 0}}},
        {
            "$project": {
                "udise_code": 1,
                "school_name": 1,
                "block_name": 1,
                "block_code": 1,
                "district_code": 1,
                "district_name": 1,
                "management": 1,
                "total_remarks": 1,
                "dropout": 1,
                "wrong_entry": 1,
                "class12_passed": 1,
            }
        },
        {"$sort": {"total_remarks": sort_order}},
        {"$limit": 20}
    ]
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=20)
    
    return [{
        "udise_code": r.get("udise_code", ""),
        "school_name": r.get("school_name", ""),
        "block_name": r.get("block_name", ""),
        "management": r.get("management", ""),
        "total_remarks": r.get("total_remarks", 0),
        "dropout": r.get("dropout", 0),
        "wrong_entry": r.get("wrong_entry", 0),
        "class12_passed": r.get("class12_passed", 0)
    } for r in results]


@router.get("/data-quality")
async def get_dropbox_data_quality(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get data quality metrics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_remarks": {"$sum": "$total_remarks"},
                "wrong_entry": {"$sum": "$wrong_entry"},
                "active_import": {"$sum": "$active_import"},
                "class12_passed": {"$sum": "$class12_passed"},
                "dropout": {"$sum": "$dropout"},
                "migrated_domestic": {"$sum": "$migrated_domestic"},
                "migrated_country": {"$sum": "$migrated_country"},
                "iti_polytechnic": {"$sum": "$iti_polytechnic"},
                "non_regular": {"$sum": "$non_regular"},
                "open_schooling": {"$sum": "$open_schooling"},
                "due_to_death": {"$sum": "$due_to_death"},
            }
        }
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"valid_pct": 100, "error_pct": 0}
    
    data = result[0]
    total = data.get("total_remarks", 0) or 1
    wrong = data.get("wrong_entry", 0) or 0
    pending = data.get("active_import", 0) or 0
    
    valid_count = total - wrong - pending
    
    return {
        "total_remarks": total,
        "valid_count": max(0, valid_count),
        "valid_pct": round((valid_count / total) * 100, 1) if total > 0 else 100,
        "error_count": wrong,
        "error_pct": round((wrong / total) * 100, 1) if total > 0 else 0,
        "pending_count": pending,
        "pending_pct": round((pending / total) * 100, 1) if total > 0 else 0,
        "quality_distribution": [
            {"name": "Valid Data", "value": max(0, valid_count), "color": "#10b981"},
            {"name": "Wrong Entry/Duplicate", "value": wrong, "color": "#ef4444"},
            {"name": "Pending Import", "value": pending, "color": "#f59e0b"}
        ]
    }


@router.get("/transition-analysis")
async def get_dropbox_transition_analysis(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get student transition/flow analysis"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "class12_passed": {"$sum": "$class12_passed"},
                "migrated_domestic": {"$sum": "$migrated_domestic"},
                "migrated_country": {"$sum": "$migrated_country"},
                "iti_polytechnic": {"$sum": "$iti_polytechnic"},
                "non_regular": {"$sum": "$non_regular"},
                "open_schooling": {"$sum": "$open_schooling"},
                "dropout": {"$sum": "$dropout"},
                "due_to_death": {"$sum": "$due_to_death"},
            }
        }
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return []
    
    data = result[0]
    
    transitions = [
        {"name": "Academic Completion", "value": data.get("class12_passed", 0), "color": "#10b981", "category": "Positive"},
        {"name": "Skill Education (ITI/Poly)", "value": data.get("iti_polytechnic", 0), "color": "#8b5cf6", "category": "Transition"},
        {"name": "Migration (Domestic)", "value": data.get("migrated_domestic", 0), "color": "#3b82f6", "category": "Mobility"},
        {"name": "Migration (International)", "value": data.get("migrated_country", 0), "color": "#6366f1", "category": "Mobility"},
        {"name": "Non-Regular Mode", "value": data.get("non_regular", 0), "color": "#06b6d4", "category": "Alternative"},
        {"name": "Open Schooling", "value": data.get("open_schooling", 0), "color": "#ec4899", "category": "Alternative"},
        {"name": "Drop Out", "value": data.get("dropout", 0), "color": "#ef4444", "category": "Critical"},
        {"name": "Due to Death", "value": data.get("due_to_death", 0), "color": "#64748b", "category": "Critical"},
    ]
    
    return transitions


@router.get("/dropout-hotspots")
async def get_dropbox_dropout_hotspots(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get blocks with highest dropout density"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "total_schools": {"$sum": 1},
                "dropout": {"$sum": "$dropout"},
                "due_to_death": {"$sum": "$due_to_death"},
                "total_remarks": {"$sum": "$total_remarks"},
            }
        },
        {"$sort": {"dropout": -1}},
        {"$limit": 15}
    ], scope_match)
    
    cursor = db.dropbox_analytics.aggregate(pipeline)
    results = await cursor.to_list(length=15)
    
    return [{
        "block_name": r.get("_id", ""),
        "total_schools": r.get("total_schools", 0),
        "dropout": r.get("dropout", 0),
        "due_to_death": r.get("due_to_death", 0),
        "dropout_density": round(r.get("dropout", 0) / r.get("total_schools", 1), 1),
        "severity_score": r.get("dropout", 0) + (r.get("due_to_death", 0) * 5)  # Death weighted higher
    } for r in results if r.get("_id")]


@router.post("/import")
async def import_dropbox_data(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL of the Dropbox Remarks Excel file")
):
    """Import Dropbox Remarks data from Excel file"""
    import_id = str(uuid.uuid4())
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            response = await http_client.get(url)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        
        file_path = UPLOADS_DIR / f"dropbox_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_dropbox_file, str(file_path), filename, import_id)
        
        return {
            "import_id": import_id,
            "status": "processing",
            "message": "Dropbox Remarks data import started"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to import: {str(e)}")


async def process_dropbox_file(file_path: str, filename: str, import_id: str):
    """Process Dropbox Remarks Excel file and store in dedicated collection"""
    try:
        logger.info(f"Processing Dropbox file: {filename}")
        
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = [str(col).strip().lower().replace(' ', '_').replace('/', '_').replace('-', '_') for col in df.columns]
        
        logger.info(f"Dropbox file columns: {list(df.columns)}")
        
        # Clear existing data
        await db.dropbox_analytics.delete_many({})
        
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
                district_col = next((c for c in df.columns if 'district_name' in c), None)
                block_col = next((c for c in df.columns if 'block_name' in c), None)
                
                district_name = str(row[district_col]).strip() if district_col and pd.notna(row[district_col]) else ""
                block_name = str(row[block_col]).strip() if block_col and pd.notna(row[block_col]) else ""
                
                block_code_col = next((c for c in df.columns if 'block_code' in c), None)
                block_code = str(row[block_code_col]).strip() if block_code_col and pd.notna(row[block_code_col]) else ""
                
                school_col = next((c for c in df.columns if 'school_name' in c), None)
                school_name = str(row[school_col]).strip() if school_col and pd.notna(row[school_col]) else ""
                
                mgmt_col = next((c for c in df.columns if 'management' in c), None)
                management = str(row[mgmt_col]).strip() if mgmt_col and pd.notna(row[mgmt_col]) else ""
                
                def safe_int(val):
                    if pd.isna(val):
                        return 0
                    try:
                        return int(float(val))
                    except:
                        return 0
                
                # Find remark columns
                dropout_col = next((c for c in df.columns if c == 'drop_out'), None)
                active_col = next((c for c in df.columns if 'active_for_import' in c or 'status_not_known' in c), None)
                domestic_col = next((c for c in df.columns if 'migrated_to_other_block' in c), None)
                country_col = next((c for c in df.columns if 'migrated_to_other_country' in c), None)
                iti_col = next((c for c in df.columns if 'iti' in c or 'polytechnic' in c), None)
                nonreg_col = next((c for c in df.columns if 'non_regular' in c), None)
                open_col = next((c for c in df.columns if 'open_schooling' in c or 'un_recognized' in c), None)
                wrong_col = next((c for c in df.columns if 'wrong_entry' in c or 'duplicate' in c), None)
                death_col = next((c for c in df.columns if 'due_to_death' in c), None)
                class12_col = next((c for c in df.columns if 'class_12' in c or 'passed_out' in c), None)
                
                dropout = safe_int(row.get(dropout_col, 0)) if dropout_col else 0
                active_import = safe_int(row.get(active_col, 0)) if active_col else 0
                migrated_domestic = safe_int(row.get(domestic_col, 0)) if domestic_col else 0
                migrated_country = safe_int(row.get(country_col, 0)) if country_col else 0
                iti_polytechnic = safe_int(row.get(iti_col, 0)) if iti_col else 0
                non_regular = safe_int(row.get(nonreg_col, 0)) if nonreg_col else 0
                open_schooling = safe_int(row.get(open_col, 0)) if open_col else 0
                wrong_entry = safe_int(row.get(wrong_col, 0)) if wrong_col else 0
                due_to_death = safe_int(row.get(death_col, 0)) if death_col else 0
                class12_passed = safe_int(row.get(class12_col, 0)) if class12_col else 0
                
                total_remarks = (dropout + active_import + migrated_domestic + migrated_country + 
                               iti_polytechnic + non_regular + open_schooling + wrong_entry + 
                               due_to_death + class12_passed)
                
                record = {
                    "udise_code": udise,
                    "district_name": district_name,
                    "block_name": block_name,
                    "block_code": block_code,
                    "school_name": school_name,
                    "management": management,
                    "dropout": dropout,
                    "active_import": active_import,
                    "migrated_domestic": migrated_domestic,
                    "migrated_country": migrated_country,
                    "iti_polytechnic": iti_polytechnic,
                    "non_regular": non_regular,
                    "open_schooling": open_schooling,
                    "wrong_entry": wrong_entry,
                    "due_to_death": due_to_death,
                    "class12_passed": class12_passed,
                    "total_remarks": total_remarks,
                    "updated_at": datetime.now(timezone.utc)
                }
                
                await db.dropbox_analytics.update_one(
                    {"udise_code": udise},
                    {"$set": record},
                    upsert=True
                )
                records_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing dropbox row: {str(e)}")
                continue
        
        logger.info(f"Dropbox import completed: {records_processed} records")
        
    except Exception as e:
        logger.error(f"Dropbox import failed: {str(e)}")


