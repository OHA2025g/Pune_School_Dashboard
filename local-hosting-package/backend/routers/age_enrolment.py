"""Age-wise Enrolment Analytics Router"""
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

router = APIRouter(prefix="/age-enrolment", tags=["Age-wise Enrolment"])

# Database will be injected
db = None
UPLOADS_DIR = None

def init_db(database, uploads_dir):
    global db, UPLOADS_DIR
    db = database
    UPLOADS_DIR = uploads_dir

@router.get("/overview")
async def get_age_enrolment_overview(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get executive overview KPIs for Age-wise Enrolment Dashboard"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_records": {"$sum": 1},
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"}
            }
        }
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_schools": 0, "total_students": 0}
    
    data = result[0]
    total_boys = data.get("boys", 0) or 0
    total_girls = data.get("girls", 0) or 0
    total_enrolment = data.get("total_students", 0) or total_boys + total_girls
    
    # Get unique schools and blocks with scope filter
    schools_query = scope_match if scope_match else {}
    schools = await db.age_enrolment.distinct("udise_code", schools_query)
    blocks = await db.age_enrolment.distinct("block_name", schools_query)
    
    # Get age-wise data for calculations
    age_pipeline = prepend_match([
        {
            "$group": {
                "_id": "$age",
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total": {"$sum": "$total"}
            }
        },
        {"$sort": {"total": -1}}
    ], scope_match)
    cursor = db.age_enrolment.aggregate(age_pipeline)
    age_data = await cursor.to_list(length=100)
    
    # Find peak age
    peak_age = age_data[0] if age_data else {"_id": "N/A", "total": 0}
    
    # Calculate early age (4-6) and secondary retention - age is integer
    early_age_total = sum(a["total"] for a in age_data if a["_id"] in [4, 5, 6])
    primary_total = sum(a["total"] for a in age_data if a["_id"] in [10, 11, 12, 13])
    secondary_total = sum(a["total"] for a in age_data if a["_id"] in [14, 15, 16, 17, 18])
    
    gpi = round(total_girls / total_boys, 3) if total_boys > 0 else 0
    girls_pct = round((total_girls / total_enrolment) * 100, 1) if total_enrolment > 0 else 0
    early_age_pct = round((early_age_total / total_enrolment) * 100, 1) if total_enrolment > 0 else 0
    secondary_retention = round(secondary_total / primary_total, 2) if primary_total > 0 else 0
    
    return {
        "total_schools": len(schools),
        "total_blocks": len(blocks),
        "total_students": total_enrolment,
        "total_enrolment": total_enrolment,
        "total_boys": total_boys,
        "total_girls": total_girls,
        "boys": total_boys,
        "girls": total_girls,
        "girls_pct": girls_pct,
        "gender_parity_index": gpi,
        "avg_students_per_school": round(total_enrolment / len(schools), 0) if schools else 0,
        "peak_enrolment_age": peak_age["_id"],
        "peak_enrolment_count": peak_age["total"],
        "early_age_total": early_age_total,
        "early_age_pct": early_age_pct,
        "primary_total": primary_total,
        "secondary_total": secondary_total,
        "secondary_retention_index": secondary_retention
    }


@router.get("/age-wise")
async def get_age_wise_enrolment(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get age-wise enrolment distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$age",
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total": {"$sum": "$total"},
                "schools": {"$addToSet": "$udise_code"}
            }
        }
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    # Sort by age (numeric)
    def parse_age(age_str):
        try:
            return int(age_str)
        except:
            if 'to' in str(age_str):
                return int(str(age_str).split(' to ')[0])
            return 99
    
    results.sort(key=lambda x: parse_age(x["_id"]))
    
    age_data = []
    for r in results:
        boys = r.get("boys", 0) or 0
        girls = r.get("girls", 0) or 0
        total = r.get("total", 0) or boys + girls
        gpi = round(girls / boys, 3) if boys > 0 else 0
        
        age_data.append({
            "age": r["_id"],
            "boys": boys,
            "girls": girls,
            "total": total,
            "gpi": gpi,
            "schools_count": len(r.get("schools", []))
        })
    
    return age_data


@router.get("/block-wise")
async def get_age_enrolment_block_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get block-wise enrolment with gender parity"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$block_name",
                "block_code": {"$first": "$block_code"},
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"},
                "schools": {"$addToSet": "$udise_code"}
            }
        },
        {"$sort": {"total_students": -1}}
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    blocks = await cursor.to_list(length=100)
    
    result = []
    for idx, block in enumerate(blocks):
        boys = block.get("boys", 0) or 0
        girls = block.get("girls", 0) or 0
        total = block.get("total_students", 0) or boys + girls
        schools = len(block.get("schools", []))
        
        gpi = round(girls / boys, 3) if boys > 0 else 0
        girls_pct = round((girls / total) * 100, 1) if total > 0 else 0
        
        result.append({
            "rank": idx + 1,
            "block_name": block["_id"],
            "block_code": block.get("block_code", ""),
            "total_schools": schools,
            "boys": boys,
            "girls": girls,
            "total_students": total,
            "gender_parity_index": gpi,
            "girls_pct": girls_pct,
            "avg_per_school": round(total / schools, 0) if schools > 0 else 0
        })
    
    return result


@router.get("/management-wise")
async def get_age_enrolment_management_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get enrolment by school management type"""
    # Management code mapping (string keys since data stores as strings)
    mgmt_names = {
        "1": "Dept. of Education", "2": "Tribal/Social Welfare Dept", "4": "Local Body",
        "5": "Pvt Aided", "6": "Pvt Unaided", "7": "Govt Aided", "10": "Social Welfare Dept",
        "11": "Ministry of Labour", "12": "Kendriya Vidyalaya", "13": "Navodaya Vidyalaya",
        "14": "Sainik School", "16": "Other Central Govt", "17": "State Govt Schools",
        "18": "Railway Schools", "19": "Other State Govt", "25": "Madrasa Recognized",
        "27": "Pvt Unaided (Recognized)", "29": "Madrasa Unrecognized", "42": "Central Govt",
        "48": "Unrecognized", "49": "Other Management", "50": "Pre-Primary Only",
        "53": "Other Govt", "54": "Municipal", "55": "Zilla Parishad"
    }
    
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$school_management",
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"},
                "schools": {"$addToSet": "$udise_code"}
            }
        },
        {"$sort": {"total_students": -1}}
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    mgmt_data = []
    for r in results:
        boys = r.get("boys", 0) or 0
        girls = r.get("girls", 0) or 0
        total = r.get("total_students", 0) or boys + girls
        schools = len(r.get("schools", []))
        gpi = round(girls / boys, 3) if boys > 0 else 0
        
        mgmt_code = str(r["_id"]) if r["_id"] else "Unknown"
        mgmt_name = mgmt_names.get(mgmt_code, f"Management {mgmt_code}")
        
        mgmt_data.append({
            "management_code": mgmt_code,
            "management_name": mgmt_name,
            "total_schools": schools,
            "boys": boys,
            "girls": girls,
            "total_boys": boys,
            "total_girls": girls,
            "total_students": total,
            "total_enrolment": total,
            "gender_parity_index": gpi,
            "avg_school_size": round(total / schools, 0) if schools > 0 else 0
        })
    
    return mgmt_data


@router.get("/category-wise")
async def get_age_enrolment_category_wise(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get enrolment by school category"""
    # Category code mapping (string keys since data stores as strings)
    category_names = {
        "1": "Primary Only", "2": "Primary with Upper Primary", "3": "Primary to Secondary",
        "4": "Primary to Higher Secondary", "5": "Upper Primary Only", 
        "6": "Upper Primary with Secondary", "7": "Upper Primary to Higher Secondary",
        "8": "Secondary Only", "10": "Secondary with Higher Secondary",
        "11": "Higher Secondary Only"
    }
    
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$school_category",
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"},
                "schools": {"$addToSet": "$udise_code"}
            }
        },
        {"$sort": {"total_students": -1}}
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    cat_data = []
    for r in results:
        boys = r.get("boys", 0) or 0
        girls = r.get("girls", 0) or 0
        total = r.get("total_students", 0) or boys + girls
        schools = len(r.get("schools", []))
        gpi = round(girls / boys, 3) if boys > 0 else 0
        
        cat_code = str(r["_id"]) if r["_id"] else "Unknown"
        cat_name = category_names.get(cat_code, f"Category {cat_code}")
        
        cat_data.append({
            "category_code": cat_code,
            "category_name": cat_name,
            "total_schools": schools,
            "boys": boys,
            "girls": girls,
            "total_boys": boys,
            "total_girls": girls,
            "total_students": total,
            "total_enrolment": total,
            "gender_parity_index": gpi,
            "avg_school_size": round(total / schools, 0) if schools > 0 else 0
        })
    
    return cat_data


@router.get("/top-schools")
async def get_top_schools_by_enrolment(
    n: int = Query(20, description="Number of schools"),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get top schools by enrolment"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$udise_code",
                "school_name": {"$first": "$school_name"},
                "block_name": {"$first": "$block_name"},
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"}
            }
        },
        {"$sort": {"total_students": -1}},
        {"$limit": n}
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    schools = await cursor.to_list(length=n)
    
    result = []
    for idx, school in enumerate(schools):
        boys = school.get("boys", 0) or 0
        girls = school.get("girls", 0) or 0
        total = school.get("total_students", 0) or boys + girls
        gpi = round(girls / boys, 3) if boys > 0 else 0
        
        result.append({
            "rank": idx + 1,
            "udise_code": school["_id"],
            "school_name": school.get("school_name", ""),
            "block_name": school.get("block_name", ""),
            "boys": boys,
            "girls": girls,
            "total_students": total,
            "gender_parity_index": gpi
        })
    
    return result


@router.get("/school-size-distribution")
async def get_school_size_distribution(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get school size distribution"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$udise_code",
                "total_students": {"$sum": "$total"}
            }
        }
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    schools = await cursor.to_list(length=10000)
    
    # Create size bands
    bands = {
        "0-50": 0, "51-100": 0, "101-200": 0, "201-500": 0,
        "501-1000": 0, "1001-2000": 0, "2000+": 0
    }
    
    enrolments = [s["total_students"] for s in schools if s["total_students"]]
    
    for e in enrolments:
        if e <= 50:
            bands["0-50"] += 1
        elif e <= 100:
            bands["51-100"] += 1
        elif e <= 200:
            bands["101-200"] += 1
        elif e <= 500:
            bands["201-500"] += 1
        elif e <= 1000:
            bands["501-1000"] += 1
        elif e <= 2000:
            bands["1001-2000"] += 1
        else:
            bands["2000+"] += 1
    
    import statistics
    median_size = statistics.median(enrolments) if enrolments else 0
    avg_size = sum(enrolments) / len(enrolments) if enrolments else 0
    
    return {
        "distribution": [{"band": k, "count": v} for k, v in bands.items()],
        "median_size": round(median_size, 0),
        "avg_size": round(avg_size, 0),
        "total_schools": len(enrolments),
        "min_size": min(enrolments) if enrolments else 0,
        "max_size": max(enrolments) if enrolments else 0
    }


@router.get("/gender-by-age")
async def get_gender_by_age(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get gender distribution by age for visualization"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    pipeline = prepend_match([
        {
            "$group": {
                "_id": "$age",
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"}
            }
        }
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    results = await cursor.to_list(length=100)
    
    # Sort by age
    def parse_age(age_str):
        try:
            return int(age_str)
        except:
            if 'to' in str(age_str):
                return int(str(age_str).split(' to ')[0])
            return 99
    
    results.sort(key=lambda x: parse_age(x["_id"]))
    
    # Filter to main ages (2-22)
    filtered = [r for r in results if parse_age(r["_id"]) <= 22]
    
    return [
        {
            "age": r["_id"],
            "boys": r.get("boys", 0),
            "girls": r.get("girls", 0),
            "gpi": round(r.get("girls", 0) / r.get("boys", 1), 3) if r.get("boys", 0) > 0 else 0
        }
        for r in filtered
    ]


@router.get("/data-quality")
async def get_age_enrolment_data_quality(
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
):
    """Get data quality metrics"""
    scope_match = build_scope_match(district_code=district_code, block_code=block_code, udise_code=udise_code)
    # Check for inconsistencies
    pipeline = prepend_match([
        {
            "$group": {
                "_id": None,
                "total_records": {"$sum": 1},
                "boys": {"$sum": "$boys"},
                "girls": {"$sum": "$girls"},
                "total_students": {"$sum": "$total"}
            }
        }
    ], scope_match)
    
    cursor = db.age_enrolment.aggregate(pipeline)
    result = await cursor.to_list(length=1)
    
    if not result:
        return {"total_records": 0}
    
    data = result[0]
    total_boys = data.get("boys", 0) or 0
    total_girls = data.get("girls", 0) or 0
    calc_total = total_boys + total_girls
    stored_total = data.get("total_students", 0) or calc_total
    
    # Get school count with scope filter
    schools_query = scope_match if scope_match else {}
    schools = await db.age_enrolment.distinct("udise_code", schools_query)
    
    # Check for zero enrolment with scope filter
    zero_query = {"total_students": 0}
    if scope_match:
        zero_query.update(scope_match)
    zero_cursor = db.age_enrolment.find(zero_query)
    zero_records = await zero_cursor.to_list(length=10000)
    
    return {
        "total_records": data.get("total_records", 0),
        "total_schools": len(schools),
        "boys": total_boys,
        "girls": total_girls,
        "calculated_total": calc_total,
        "stored_total": stored_total,
        "data_consistent": calc_total == stored_total,
        "zero_enrolment_records": len(zero_records),
        "completeness_score": round((1 - len(zero_records) / data.get("total_records", 1)) * 100, 1)
    }


@router.post("/import")
async def import_age_enrolment(
    background_tasks: BackgroundTasks,
    url: str = Query(None, description="URL of Excel file to import")
):
    """Import Age-wise Enrolment data from Excel file"""
    if not url:
        url = "https://customer-assets.emergentagent.com/job_e600aca7-d1b5-4003-a850-c6b4b2f65c48/artifacts/jp05ej1k_7.%20Age%20Wise%20-%202025-26.xlsx"
    
    import_id = str(uuid.uuid4())[:8]
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True, timeout=120.0)
            response.raise_for_status()
        
        filename = url.split('/')[-1]
        file_path = UPLOADS_DIR / f"age_enrolment_{import_id}_{filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(response.content)
        
        background_tasks.add_task(process_age_enrolment_file, str(file_path), filename, import_id)
        
        return {"status": "processing", "import_id": import_id, "message": "Age-wise Enrolment import started"}
    
    except Exception as e:
        logging.error(f"Age-wise Enrolment import error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_age_enrolment_file(file_path: str, filename: str, import_id: str):
    """Process Age-wise Enrolment Excel file"""
    try:
        logging.info(f"Processing Age-wise Enrolment file: {filename}")
        
        df = pd.read_excel(file_path)
        logging.info(f"Age-wise Enrolment file loaded: {len(df)} rows, {len(df.columns)} columns")
        
        # Clear existing data
        await db.age_enrolment.delete_many({})
        
        records_processed = 0
        for idx, row in df.iterrows():
            try:
                udise = str(row.get('UDISE Code', '')).strip() if pd.notna(row.get('UDISE Code')) else ""
                if not udise:
                    continue
                
                age = str(row.get('Age Wise', '')).strip() if pd.notna(row.get('Age Wise')) else ""
                
                # Calculate totals from class-wise data
                total_boys = 0
                total_girls = 0
                for col in df.columns:
                    if '(Boys)' in col:
                        val = row.get(col, 0)
                        total_boys += int(val) if pd.notna(val) else 0
                    elif '(Girls)' in col:
                        val = row.get(col, 0)
                        total_girls += int(val) if pd.notna(val) else 0
                
                record = {
                    "udise_code": udise,
                    "district_code": str(row.get('District Code', '')),
                    "district_name": str(row.get('District Name', '')).strip() if pd.notna(row.get('District Name')) else "",
                    "block_code": str(row.get('Block Code', '')),
                    "block_name": str(row.get('Block Name', '')).strip() if pd.notna(row.get('Block Name')) else "",
                    "school_name": str(row.get('School Name', '')).strip() if pd.notna(row.get('School Name')) else "",
                    "school_management": int(row.get('School Management', 0)) if pd.notna(row.get('School Management')) else 0,
                    "school_category": int(row.get('School Category', 0)) if pd.notna(row.get('School Category')) else 0,
                    "age": age,
                    "boys": total_boys,
                    "girls": total_girls,
                    "total_students": total_boys + total_girls,
                    "updated_at": datetime.now(timezone.utc)
                }
                
                # Store each age record separately for age-wise analysis
                await db.age_enrolment.insert_one(record)
                records_processed += 1
                
            except Exception as e:
                logging.error(f"Error processing age enrolment row {idx}: {str(e)}")
                continue
        
        logging.info(f"Age-wise Enrolment import completed: {records_processed} records")
        
    except Exception as e:
        logging.error(f"Age-wise Enrolment import failed: {str(e)}")


