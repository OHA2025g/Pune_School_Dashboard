from fastapi import FastAPI, APIRouter, HTTPException, Query, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone
import pandas as pd
import io
import aiofiles
import hashlib
import httpx

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')
# Optional local overrides (do not commit secrets)
load_dotenv(ROOT_DIR / ".env.local", override=True)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app
app = FastAPI(title="Maharashtra Education Dashboard API")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Uploads directory
UPLOADS_DIR = ROOT_DIR / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)

# ============= MODELS =============

class KPIStats(BaseModel):
    total_schools: int = 0
    total_students: int = 0
    aadhaar_percentage: float = 0.0
    apaar_percentage: float = 0.0
    water_availability_percentage: float = 0.0
    avg_ptr: float = 0.0
    data_entry_percentage: float = 0.0
    avg_shi: float = 0.0

class DistrictSummary(BaseModel):
    district_code: str
    district_name: str
    total_schools: int = 0
    total_students: int = 0
    aadhaar_percentage: float = 0.0
    apaar_percentage: float = 0.0
    water_percentage: float = 0.0
    toilet_percentage: float = 0.0
    avg_ptr: float = 0.0
    data_entry_percentage: float = 0.0
    shi_score: float = 0.0
    rag_status: str = "green"

class BlockSummary(BaseModel):
    block_code: str
    block_name: str
    district_code: str
    district_name: str
    total_schools: int = 0
    total_students: int = 0
    aadhaar_percentage: float = 0.0
    apaar_percentage: float = 0.0
    shi_score: float = 0.0
    rag_status: str = "green"

class SchoolDetail(BaseModel):
    udise_code: str
    school_name: str
    district_code: str
    district_name: str
    block_code: str
    block_name: str
    school_category: Optional[str] = None
    school_management: Optional[str] = None
    total_students: int = 0
    total_teachers: int = 0
    ptr: float = 0.0
    aadhaar_percentage: float = 0.0
    apaar_percentage: float = 0.0
    water_available: bool = True
    toilets_available: bool = True
    classrooms: int = 0
    students_per_classroom: float = 0.0
    data_entry_status: str = "pending"
    certified: bool = False
    shi_score: float = 0.0
    rag_status: str = "green"

class ImportStatus(BaseModel):
    import_id: str
    status: str
    dataset_type: Optional[str] = None
    filename: str
    records_processed: int = 0
    errors: List[str] = []
    created_at: datetime
    completed_at: Optional[datetime] = None

# ============= MAHARASHTRA DISTRICT DATA =============

MAHARASHTRA_DISTRICTS = [
    {"code": "2701", "name": "AHMEDNAGAR"},
    {"code": "2702", "name": "AKOLA"},
    {"code": "2703", "name": "AMRAVATI"},
    {"code": "2704", "name": "AURANGABAD"},
    {"code": "2705", "name": "BEED"},
    {"code": "2706", "name": "BHANDARA"},
    {"code": "2707", "name": "BULDHANA"},
    {"code": "2708", "name": "CHANDRAPUR"},
    {"code": "2709", "name": "DHULE"},
    {"code": "2710", "name": "GADCHIROLI"},
    {"code": "2711", "name": "GONDIA"},
    {"code": "2712", "name": "HINGOLI"},
    {"code": "2713", "name": "JALGAON"},
    {"code": "2714", "name": "JALNA"},
    {"code": "2715", "name": "KOLHAPUR"},
    {"code": "2716", "name": "LATUR"},
    {"code": "2717", "name": "MUMBAI CITY"},
    {"code": "2718", "name": "MUMBAI SUBURBAN"},
    {"code": "2719", "name": "NAGPUR"},
    {"code": "2720", "name": "NANDED"},
    {"code": "2721", "name": "NANDURBAR"},
    {"code": "2722", "name": "NASHIK"},
    {"code": "2723", "name": "OSMANABAD"},
    {"code": "2724", "name": "PALGHAR"},
    {"code": "2725", "name": "PUNE"},
    {"code": "2726", "name": "RAIGAD"},
    {"code": "2727", "name": "RATNAGIRI"},
    {"code": "2728", "name": "SANGLI"},
    {"code": "2729", "name": "SATARA"},
    {"code": "2730", "name": "SINDHUDURG"},
    {"code": "2731", "name": "SOLAPUR"},
    {"code": "2732", "name": "THANE"},
    {"code": "2733", "name": "WARDHA"},
    {"code": "2734", "name": "WASHIM"},
    {"code": "2735", "name": "YAVATMAL"},
    {"code": "2736", "name": "PARBHANI"},
]

# District name to code mapping
DISTRICT_NAME_TO_CODE = {d["name"]: d["code"] for d in MAHARASHTRA_DISTRICTS}
DISTRICT_CODE_TO_NAME = {d["code"]: d["name"] for d in MAHARASHTRA_DISTRICTS}

# ============= HELPER FUNCTIONS =============

def calculate_shi(school_data: dict) -> float:
    """Calculate School Health Index (0-100)"""
    # Identity Score (25%)
    aadhaar_pct = school_data.get("aadhaar_percentage", 0)
    apaar_pct = school_data.get("apaar_percentage", 0)
    identity_score = (aadhaar_pct * 0.5 + apaar_pct * 0.3 + 100 * 0.2)
    
    # Infrastructure Score (25%)
    water = 100 if school_data.get("water_available", True) else 0
    toilet = 100 if school_data.get("toilets_available", True) else 0
    classroom_ratio = school_data.get("students_per_classroom", 40)
    classroom_score = min(100, (40 / max(classroom_ratio, 1)) * 100)
    infra_score = (water * 0.4 + toilet * 0.3 + classroom_score * 0.3)
    
    # Teacher Score (20%)
    ptr = school_data.get("ptr", 30)
    teacher_score = min(100, (30 / max(ptr, 1)) * 100)
    
    # Operational Score (20%)
    data_entry = 100 if school_data.get("certified", False) else 50
    operational_score = data_entry
    
    # Age Integrity Score (10%)
    age_integrity = 90
    
    # Final SHI
    shi = (
        identity_score * 0.25 +
        infra_score * 0.25 +
        teacher_score * 0.20 +
        operational_score * 0.20 +
        age_integrity * 0.10
    )
    return round(min(100, max(0, shi)), 1)

def get_rag_status(shi_score: float) -> str:
    """Get RAG status based on SHI score"""
    if shi_score >= 85:
        return "green"
    elif shi_score >= 70:
        return "amber"
    elif shi_score >= 50:
        return "amber"
    else:
        return "red"

def generate_district_code(district_name: str) -> str:
    """Generate or lookup district code from name"""
    district_upper = district_name.upper().strip()
    if district_upper in DISTRICT_NAME_TO_CODE:
        return DISTRICT_NAME_TO_CODE[district_upper]
    # Generate a hash-based code for unknown districts
    hash_val = hashlib.md5(district_upper.encode()).hexdigest()[:4]
    return f"27{hash_val[:2].upper()}"

def generate_block_code(district_code: str, block_name: str) -> str:
    """Generate block code from district code and block name"""
    hash_val = hashlib.md5(block_name.upper().encode()).hexdigest()[:2]
    return f"{district_code}{hash_val}"

# ============= DATA ACCESS FUNCTIONS =============

async def get_data_from_db() -> bool:
    """Check if we have imported data in the database"""
    count = await db.schools.count_documents({})
    return count > 0

async def get_districts_from_db() -> List[DistrictSummary]:
    """Get district data from MongoDB"""
    pipeline = [
        {
            "$group": {
                "_id": "$district_name",
                "district_code": {"$first": "$district_code"},
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_students"},
                "total_teachers": {"$sum": "$total_teachers"},
                "aadhaar_sum": {"$sum": "$aadhaar_percentage"},
                "apaar_sum": {"$sum": "$apaar_percentage"},
                "water_count": {"$sum": {"$cond": ["$water_available", 1, 0]}},
                "toilet_count": {"$sum": {"$cond": ["$toilets_available", 1, 0]}},
                "certified_count": {"$sum": {"$cond": ["$certified", 1, 0]}},
                "school_count": {"$sum": 1}
            }
        }
    ]
    
    cursor = db.schools.aggregate(pipeline)
    districts = []
    
    async for doc in cursor:
        district_name = doc["_id"]
        if not district_name:
            continue
            
        total_schools = doc["total_schools"]
        
        aadhaar_pct = round(doc["aadhaar_sum"] / total_schools, 1) if total_schools > 0 else 0.0
        apaar_pct = round(doc["apaar_sum"] / total_schools, 1) if total_schools > 0 else 0.0
        water_pct = round((doc["water_count"] / total_schools) * 100, 1) if total_schools > 0 else 0.0
        toilet_pct = round((doc["toilet_count"] / total_schools) * 100, 1) if total_schools > 0 else 0.0
        data_entry_pct = round((doc["certified_count"] / total_schools) * 100, 1) if total_schools > 0 else 0.0
        
        total_teachers = doc["total_teachers"]
        avg_ptr = round(doc["total_students"] / total_teachers, 1) if total_teachers > 0 else 30.0
        
        shi = calculate_shi({
            "aadhaar_percentage": aadhaar_pct,
            "apaar_percentage": apaar_pct,
            "water_available": water_pct > 90,
            "toilets_available": toilet_pct > 90,
            "students_per_classroom": 35,
            "ptr": avg_ptr,
            "certified": data_entry_pct > 80
        })
        
        districts.append(DistrictSummary(
            district_code=doc.get("district_code", generate_district_code(district_name)),
            district_name=district_name,
            total_schools=total_schools,
            total_students=doc["total_students"],
            aadhaar_percentage=aadhaar_pct,
            apaar_percentage=apaar_pct,
            water_percentage=water_pct,
            toilet_percentage=toilet_pct,
            avg_ptr=avg_ptr,
            data_entry_percentage=data_entry_pct,
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return districts

async def get_blocks_from_db(district_code: str = None, district_name: str = None) -> List[BlockSummary]:
    """Get block data from MongoDB"""
    match_stage = {}
    if district_code:
        match_stage["district_code"] = district_code
    if district_name:
        match_stage["district_name"] = district_name
    
    pipeline = [
        {"$match": match_stage} if match_stage else {"$match": {}},
        {
            "$group": {
                "_id": {"district_name": "$district_name", "block_name": "$block_name"},
                "district_code": {"$first": "$district_code"},
                "block_code": {"$first": "$block_code"},
                "total_schools": {"$sum": 1},
                "total_students": {"$sum": "$total_students"},
                "aadhaar_sum": {"$sum": "$aadhaar_percentage"},
                "apaar_sum": {"$sum": "$apaar_percentage"},
            }
        }
    ]
    
    cursor = db.schools.aggregate(pipeline)
    blocks = []
    
    async for doc in cursor:
        block_name = doc["_id"]["block_name"]
        district_name_val = doc["_id"]["district_name"]
        
        if not block_name or not district_name_val:
            continue
        
        total_schools = doc["total_schools"]
        aadhaar_pct = round(doc["aadhaar_sum"] / total_schools, 1) if total_schools > 0 else 0.0
        apaar_pct = round(doc["apaar_sum"] / total_schools, 1) if total_schools > 0 else 0.0
        
        shi = calculate_shi({
            "aadhaar_percentage": aadhaar_pct,
            "apaar_percentage": apaar_pct,
            "water_available": True,
            "toilets_available": True,
            "students_per_classroom": 35,
            "ptr": 30,
            "certified": True
        })
        
        d_code = doc.get("district_code", generate_district_code(district_name_val))
        
        blocks.append(BlockSummary(
            block_code=doc.get("block_code", generate_block_code(d_code, block_name)),
            block_name=block_name,
            district_code=d_code,
            district_name=district_name_val,
            total_schools=total_schools,
            total_students=doc["total_students"],
            aadhaar_percentage=aadhaar_pct,
            apaar_percentage=apaar_pct,
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return blocks

async def get_schools_from_db(block_code: str = None, block_name: str = None, district_name: str = None, limit: int = 100) -> List[SchoolDetail]:
    """Get school data from MongoDB"""
    query = {}
    if block_code:
        query["block_code"] = block_code
    if block_name:
        query["block_name"] = block_name
    if district_name:
        query["district_name"] = district_name
    
    cursor = db.schools.find(query, {"_id": 0}).limit(limit)
    schools = []
    
    async for doc in cursor:
        total_students = doc.get("total_students", 0)
        total_teachers = doc.get("total_teachers", 1)
        classrooms = doc.get("classrooms", 1)
        
        ptr = round(total_students / max(total_teachers, 1), 1)
        students_per_classroom = round(total_students / max(classrooms, 1), 1)
        
        shi = calculate_shi({
            "aadhaar_percentage": doc.get("aadhaar_percentage", 0),
            "apaar_percentage": doc.get("apaar_percentage", 0),
            "water_available": doc.get("water_available", True),
            "toilets_available": doc.get("toilets_available", True),
            "students_per_classroom": students_per_classroom,
            "ptr": ptr,
            "certified": doc.get("certified", False)
        })
        
        schools.append(SchoolDetail(
            udise_code=doc.get("udise_code", ""),
            school_name=doc.get("school_name", "Unknown School"),
            district_code=doc.get("district_code", ""),
            district_name=doc.get("district_name", ""),
            block_code=doc.get("block_code", ""),
            block_name=doc.get("block_name", ""),
            school_category=doc.get("school_category"),
            school_management=doc.get("school_management"),
            total_students=total_students,
            total_teachers=total_teachers,
            ptr=ptr,
            aadhaar_percentage=doc.get("aadhaar_percentage", 0.0),
            apaar_percentage=doc.get("apaar_percentage", 0.0),
            water_available=doc.get("water_available", True),
            toilets_available=doc.get("toilets_available", True),
            classrooms=classrooms,
            students_per_classroom=students_per_classroom,
            data_entry_status=doc.get("data_entry_status", "pending"),
            certified=doc.get("certified", False),
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return schools

# ============= MOCK DATA FUNCTIONS (Fallback) =============

import random

def generate_mock_district_data() -> List[DistrictSummary]:
    """Generate realistic mock data for districts"""
    districts = []
    random.seed(42)
    
    for d in MAHARASHTRA_DISTRICTS:
        total_schools = random.randint(800, 5000)
        total_students = total_schools * random.randint(80, 200)
        aadhaar_pct = round(random.uniform(75, 98), 1)
        apaar_pct = round(random.uniform(60, 95), 1)
        water_pct = round(random.uniform(85, 99), 1)
        toilet_pct = round(random.uniform(90, 99), 1)
        avg_ptr = round(random.uniform(20, 40), 1)
        data_entry_pct = round(random.uniform(70, 98), 1)
        
        shi = calculate_shi({
            "aadhaar_percentage": aadhaar_pct,
            "apaar_percentage": apaar_pct,
            "water_available": water_pct > 90,
            "toilets_available": toilet_pct > 90,
            "students_per_classroom": 35,
            "ptr": avg_ptr,
            "certified": data_entry_pct > 80
        })
        
        districts.append(DistrictSummary(
            district_code=d["code"],
            district_name=d["name"],
            total_schools=total_schools,
            total_students=total_students,
            aadhaar_percentage=aadhaar_pct,
            apaar_percentage=apaar_pct,
            water_percentage=water_pct,
            toilet_percentage=toilet_pct,
            avg_ptr=avg_ptr,
            data_entry_percentage=data_entry_pct,
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return districts

def generate_mock_block_data(district_code: str) -> List[BlockSummary]:
    """Generate mock data for blocks in a district"""
    blocks = []
    random.seed(int(district_code))
    district_name = DISTRICT_CODE_TO_NAME.get(district_code, "UNKNOWN")
    
    for i in range(1, 11):
        block_code = f"{district_code}{str(i).zfill(2)}"
        total_schools = random.randint(100, 500)
        total_students = total_schools * random.randint(80, 200)
        aadhaar_pct = round(random.uniform(70, 98), 1)
        apaar_pct = round(random.uniform(55, 95), 1)
        
        shi = calculate_shi({
            "aadhaar_percentage": aadhaar_pct,
            "apaar_percentage": apaar_pct,
            "water_available": random.random() > 0.1,
            "toilets_available": random.random() > 0.05,
            "students_per_classroom": random.randint(25, 50),
            "ptr": random.uniform(20, 45),
            "certified": random.random() > 0.3
        })
        
        blocks.append(BlockSummary(
            block_code=block_code,
            block_name=f"BLOCK {i}",
            district_code=district_code,
            district_name=district_name,
            total_schools=total_schools,
            total_students=total_students,
            aadhaar_percentage=aadhaar_pct,
            apaar_percentage=apaar_pct,
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return blocks

def generate_mock_schools(block_code: str, limit: int = 50) -> List[SchoolDetail]:
    """Generate mock school data for a block"""
    schools = []
    random.seed(int(block_code) if block_code.isdigit() else hash(block_code))
    
    district_code = block_code[:4]
    district_name = DISTRICT_CODE_TO_NAME.get(district_code, "UNKNOWN")
    block_name = f"BLOCK {block_code[-2:]}"
    
    school_types = ["Primary", "Upper Primary", "Secondary", "Higher Secondary"]
    management_types = ["Government", "Private Aided", "Private Unaided"]
    
    for i in range(limit):
        udise_code = f"{block_code}{str(i+1).zfill(5)}"
        total_students = random.randint(50, 1500)
        total_teachers = max(2, total_students // random.randint(25, 40))
        ptr = round(total_students / max(total_teachers, 1), 1)
        classrooms = max(3, total_students // random.randint(30, 50))
        
        aadhaar_pct = round(random.uniform(70, 100), 1)
        apaar_pct = round(random.uniform(50, 100), 1)
        water_available = random.random() > 0.1
        toilets_available = random.random() > 0.05
        certified = random.random() > 0.3
        
        shi = calculate_shi({
            "aadhaar_percentage": aadhaar_pct,
            "apaar_percentage": apaar_pct,
            "water_available": water_available,
            "toilets_available": toilets_available,
            "students_per_classroom": total_students / max(classrooms, 1),
            "ptr": ptr,
            "certified": certified
        })
        
        schools.append(SchoolDetail(
            udise_code=udise_code,
            school_name=f"SCHOOL {i+1}",
            district_code=district_code,
            district_name=district_name,
            block_code=block_code,
            block_name=block_name,
            school_category=random.choice(school_types),
            school_management=random.choice(management_types),
            total_students=total_students,
            total_teachers=total_teachers,
            ptr=ptr,
            aadhaar_percentage=aadhaar_pct,
            apaar_percentage=apaar_pct,
            water_available=water_available,
            toilets_available=toilets_available,
            classrooms=classrooms,
            students_per_classroom=round(total_students / max(classrooms, 1), 1),
            data_entry_status="completed" if certified else "pending",
            certified=certified,
            shi_score=shi,
            rag_status=get_rag_status(shi)
        ))
    
    return schools

# ============= API ROUTES =============

@api_router.get("/")
async def root():
    return {"message": "Maharashtra Education Dashboard API", "version": "1.0.0"}

@api_router.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

# State Overview
@api_router.get("/state/overview", response_model=KPIStats)
async def get_state_overview():
    """Get state-level KPI statistics"""
    has_data = await get_data_from_db()
    
    if has_data:
        districts = await get_districts_from_db()
    else:
        districts = generate_mock_district_data()
    
    if not districts:
        return KPIStats()
    
    total_schools = sum(d.total_schools for d in districts)
    total_students = sum(d.total_students for d in districts)
    avg_aadhaar = sum(d.aadhaar_percentage for d in districts) / len(districts)
    avg_apaar = sum(d.apaar_percentage for d in districts) / len(districts)
    avg_water = sum(d.water_percentage for d in districts) / len(districts)
    avg_ptr = sum(d.avg_ptr for d in districts) / len(districts)
    avg_data_entry = sum(d.data_entry_percentage for d in districts) / len(districts)
    avg_shi = sum(d.shi_score for d in districts) / len(districts)
    
    return KPIStats(
        total_schools=total_schools,
        total_students=total_students,
        aadhaar_percentage=round(avg_aadhaar, 1),
        apaar_percentage=round(avg_apaar, 1),
        water_availability_percentage=round(avg_water, 1),
        avg_ptr=round(avg_ptr, 1),
        data_entry_percentage=round(avg_data_entry, 1),
        avg_shi=round(avg_shi, 1)
    )

@api_router.get("/districts", response_model=List[DistrictSummary])
async def get_districts(
    sort_by: str = Query("shi_score", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    rag_filter: Optional[str] = Query(None, description="Filter by RAG status")
):
    """Get all districts with summary statistics"""
    has_data = await get_data_from_db()
    
    if has_data:
        districts = await get_districts_from_db()
    else:
        districts = generate_mock_district_data()
    
    # Apply RAG filter
    if rag_filter:
        districts = [d for d in districts if d.rag_status == rag_filter]
    
    # Sort
    reverse = sort_order == "desc"
    if districts and hasattr(districts[0], sort_by):
        districts.sort(key=lambda x: getattr(x, sort_by), reverse=reverse)
    
    return districts

@api_router.get("/districts/{district_code}", response_model=DistrictSummary)
async def get_district_detail(district_code: str):
    """Get detailed information for a specific district"""
    has_data = await get_data_from_db()
    
    if has_data:
        districts = await get_districts_from_db()
    else:
        districts = generate_mock_district_data()
    
    district = next((d for d in districts if d.district_code == district_code), None)
    
    if not district:
        raise HTTPException(status_code=404, detail="District not found")
    
    return district

@api_router.get("/districts/{district_code}/blocks", response_model=List[BlockSummary])
async def get_blocks(
    district_code: str,
    sort_by: str = Query("shi_score", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order")
):
    """Get all blocks in a district"""
    has_data = await get_data_from_db()
    
    if has_data:
        district_name = DISTRICT_CODE_TO_NAME.get(district_code)
        blocks = await get_blocks_from_db(district_code=district_code, district_name=district_name)
    else:
        blocks = generate_mock_block_data(district_code)
    
    reverse = sort_order == "desc"
    if blocks and hasattr(blocks[0], sort_by):
        blocks.sort(key=lambda x: getattr(x, sort_by), reverse=reverse)
    
    return blocks

@api_router.get("/blocks/{block_code}", response_model=BlockSummary)
async def get_block_detail(block_code: str):
    """Get detailed information for a specific block"""
    district_code = block_code[:4]
    
    has_data = await get_data_from_db()
    
    if has_data:
        district_name = DISTRICT_CODE_TO_NAME.get(district_code)
        blocks = await get_blocks_from_db(district_code=district_code, district_name=district_name)
    else:
        blocks = generate_mock_block_data(district_code)
    
    block = next((b for b in blocks if b.block_code == block_code), None)
    
    if not block:
        raise HTTPException(status_code=404, detail="Block not found")
    
    return block

@api_router.get("/blocks/{block_code}/schools", response_model=List[SchoolDetail])
async def get_schools(
    block_code: str,
    limit: int = Query(50, description="Number of schools to return"),
    sort_by: str = Query("shi_score", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order"),
    rag_filter: Optional[str] = Query(None, description="Filter by RAG status")
):
    """Get all schools in a block"""
    has_data = await get_data_from_db()
    
    if has_data:
        schools = await get_schools_from_db(block_code=block_code, limit=limit)
    else:
        schools = generate_mock_schools(block_code, limit)
    
    # Apply RAG filter
    if rag_filter:
        schools = [s for s in schools if s.rag_status == rag_filter]
    
    # Sort
    reverse = sort_order == "desc"
    if schools and hasattr(schools[0], sort_by):
        schools.sort(key=lambda x: getattr(x, sort_by), reverse=reverse)
    
    return schools

@api_router.get("/schools/{udise_code}", response_model=SchoolDetail)
async def get_school_detail(udise_code: str):
    """Get detailed information for a specific school"""
    has_data = await get_data_from_db()
    
    if has_data:
        doc = await db.schools.find_one({"udise_code": udise_code}, {"_id": 0})
        if doc:
            total_students = doc.get("total_students", 0)
            total_teachers = doc.get("total_teachers", 1)
            classrooms = doc.get("classrooms", 1)
            ptr = round(total_students / max(total_teachers, 1), 1)
            students_per_classroom = round(total_students / max(classrooms, 1), 1)
            
            shi = calculate_shi({
                "aadhaar_percentage": doc.get("aadhaar_percentage", 0),
                "apaar_percentage": doc.get("apaar_percentage", 0),
                "water_available": doc.get("water_available", True),
                "toilets_available": doc.get("toilets_available", True),
                "students_per_classroom": students_per_classroom,
                "ptr": ptr,
                "certified": doc.get("certified", False)
            })
            
            return SchoolDetail(
                udise_code=doc.get("udise_code", udise_code),
                school_name=doc.get("school_name", "Unknown School"),
                district_code=doc.get("district_code", ""),
                district_name=doc.get("district_name", ""),
                block_code=doc.get("block_code", ""),
                block_name=doc.get("block_name", ""),
                school_category=doc.get("school_category"),
                school_management=doc.get("school_management"),
                total_students=total_students,
                total_teachers=total_teachers,
                ptr=ptr,
                aadhaar_percentage=doc.get("aadhaar_percentage", 0.0),
                apaar_percentage=doc.get("apaar_percentage", 0.0),
                water_available=doc.get("water_available", True),
                toilets_available=doc.get("toilets_available", True),
                classrooms=classrooms,
                students_per_classroom=students_per_classroom,
                data_entry_status=doc.get("data_entry_status", "pending"),
                certified=doc.get("certified", False),
                shi_score=shi,
                rag_status=get_rag_status(shi)
            )
    
    # Fallback to mock
    block_code = udise_code[:6]
    schools = generate_mock_schools(block_code, 100)
    school = next((s for s in schools if s.udise_code == udise_code), None)
    
    if not school:
        school = generate_mock_schools(block_code, 1)[0]
        school.udise_code = udise_code
    
    return school

# Rankings
@api_router.get("/rankings/districts/top", response_model=List[DistrictSummary])
async def get_top_districts(limit: int = Query(10)):
    """Get top performing districts by SHI score"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    districts.sort(key=lambda x: x.shi_score, reverse=True)
    return districts[:limit]

@api_router.get("/rankings/districts/bottom", response_model=List[DistrictSummary])
async def get_bottom_districts(limit: int = Query(10)):
    """Get lowest performing districts by SHI score"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    districts.sort(key=lambda x: x.shi_score)
    return districts[:limit]

# Analytics endpoints
@api_router.get("/analytics/identity-compliance")
async def get_identity_compliance():
    """Get identity compliance analytics (Aadhaar/APAAR)"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    
    return {
        "total_districts": len(districts),
        "avg_aadhaar_compliance": round(sum(d.aadhaar_percentage for d in districts) / len(districts), 1),
        "avg_apaar_compliance": round(sum(d.apaar_percentage for d in districts) / len(districts), 1),
        "districts_above_90_aadhaar": len([d for d in districts if d.aadhaar_percentage >= 90]),
        "districts_below_80_aadhaar": len([d for d in districts if d.aadhaar_percentage < 80]),
        "district_wise": [
            {
                "district_name": d.district_name,
                "aadhaar_percentage": d.aadhaar_percentage,
                "apaar_percentage": d.apaar_percentage
            } for d in sorted(districts, key=lambda x: x.aadhaar_percentage, reverse=True)
        ]
    }

@api_router.get("/analytics/infrastructure")
async def get_infrastructure_analytics():
    """Get infrastructure analytics"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    
    return {
        "total_districts": len(districts),
        "avg_water_availability": round(sum(d.water_percentage for d in districts) / len(districts), 1),
        "avg_toilet_availability": round(sum(d.toilet_percentage for d in districts) / len(districts), 1),
        "districts_with_full_water": len([d for d in districts if d.water_percentage >= 95]),
        "districts_needing_attention": len([d for d in districts if d.water_percentage < 85]),
        "district_wise": [
            {
                "district_name": d.district_name,
                "water_percentage": d.water_percentage,
                "toilet_percentage": d.toilet_percentage
            } for d in sorted(districts, key=lambda x: x.water_percentage, reverse=True)
        ]
    }

@api_router.get("/analytics/teachers")
async def get_teacher_analytics():
    """Get teacher staffing analytics"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    
    return {
        "total_districts": len(districts),
        "state_avg_ptr": round(sum(d.avg_ptr for d in districts) / len(districts), 1),
        "districts_optimal_ptr": len([d for d in districts if d.avg_ptr <= 30]),
        "districts_high_ptr": len([d for d in districts if d.avg_ptr > 35]),
        "district_wise": [
            {
                "district_name": d.district_name,
                "avg_ptr": d.avg_ptr,
                "total_schools": d.total_schools,
                "total_students": d.total_students
            } for d in sorted(districts, key=lambda x: x.avg_ptr)
        ]
    }

@api_router.get("/analytics/data-quality")
async def get_data_quality_analytics():
    """Get data entry and quality analytics"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    
    return {
        "total_districts": len(districts),
        "avg_data_entry_completion": round(sum(d.data_entry_percentage for d in districts) / len(districts), 1),
        "districts_fully_updated": len([d for d in districts if d.data_entry_percentage >= 95]),
        "districts_pending": len([d for d in districts if d.data_entry_percentage < 80]),
        "district_wise": [
            {
                "district_name": d.district_name,
                "data_entry_percentage": d.data_entry_percentage,
                "total_schools": d.total_schools
            } for d in sorted(districts, key=lambda x: x.data_entry_percentage, reverse=True)
        ]
    }

@api_router.get("/analytics/shi-distribution")
async def get_shi_distribution():
    """Get SHI score distribution"""
    has_data = await get_data_from_db()
    districts = await get_districts_from_db() if has_data else generate_mock_district_data()
    
    excellent = [d for d in districts if d.shi_score >= 85]
    good = [d for d in districts if 70 <= d.shi_score < 85]
    at_risk = [d for d in districts if 50 <= d.shi_score < 70]
    critical = [d for d in districts if d.shi_score < 50]
    
    return {
        "distribution": {
            "excellent": len(excellent),
            "good": len(good),
            "at_risk": len(at_risk),
            "critical": len(critical)
        },
        "avg_shi": round(sum(d.shi_score for d in districts) / len(districts), 1),
        "max_shi": max(d.shi_score for d in districts),
        "min_shi": min(d.shi_score for d in districts),
        "district_scores": [
            {
                "district_name": d.district_name,
                "shi_score": d.shi_score,
                "rag_status": d.rag_status
            } for d in sorted(districts, key=lambda x: x.shi_score, reverse=True)
        ]
    }


# Include the base router
app.include_router(api_router)

# Import all domain routers
from routers.auth import router as auth_router, init_db as init_auth_db, create_default_admin
from routers.export import router as export_router, init_db as init_export_db
from routers.analytics import router as analytics_router, init_db as init_analytics_db
from routers.aadhaar import router as aadhaar_router, init_db as init_aadhaar_db
from routers.apaar import router as apaar_router, init_db as init_apaar_db
from routers.dropbox import router as dropbox_router, init_db as init_dropbox_db
from routers.enrolment import router as enrolment_router, init_db as init_enrolment_db
from routers.infrastructure import router as infrastructure_router, init_db as init_infrastructure_db
from routers.teacher import router as teacher_router, init_db as init_teacher_db
from routers.data_entry import router as data_entry_router, init_db as init_data_entry_db
from routers.age_enrolment import router as age_enrolment_router, init_db as init_age_enrolment_db
from routers.ctteacher import router as ctteacher_router, init_db as init_ctteacher_db
from routers.classrooms_toilets import router as classrooms_toilets_router, init_db as init_classrooms_toilets_db
from routers.executive import router as executive_router, init_db as init_executive_db
from routers.scope import router as scope_router, init_db as init_scope_db

# Initialize all routers with database
init_auth_db(db)
init_export_db(db)
init_analytics_db(db)
init_aadhaar_db(db, UPLOADS_DIR)
init_apaar_db(db, UPLOADS_DIR)
init_dropbox_db(db, UPLOADS_DIR)
init_enrolment_db(db, UPLOADS_DIR)
init_infrastructure_db(db, UPLOADS_DIR)
init_teacher_db(db, UPLOADS_DIR)
init_data_entry_db(db, UPLOADS_DIR)
init_age_enrolment_db(db, UPLOADS_DIR)
init_ctteacher_db(db, UPLOADS_DIR)
init_classrooms_toilets_db(db, UPLOADS_DIR)
init_executive_db(db)
init_scope_db(db)

# Register all routers with /api prefix
app.include_router(auth_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(analytics_router, prefix="/api")
app.include_router(aadhaar_router, prefix="/api")
app.include_router(apaar_router, prefix="/api")
app.include_router(dropbox_router, prefix="/api")
app.include_router(enrolment_router, prefix="/api")
app.include_router(infrastructure_router, prefix="/api")
app.include_router(teacher_router, prefix="/api")
app.include_router(data_entry_router, prefix="/api")
app.include_router(age_enrolment_router, prefix="/api")
app.include_router(ctteacher_router, prefix="/api")
app.include_router(classrooms_toilets_router, prefix="/api")
app.include_router(executive_router, prefix="/api")
app.include_router(scope_router, prefix="/api")

# CORS middleware
def _as_bool(v: str) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "on")

allow_origins = [o.strip() for o in os.environ.get("CORS_ORIGINS", "*").split(",") if o.strip()]
if not allow_origins:
    allow_origins = ["*"]

allow_credentials = _as_bool(os.environ.get("CORS_ALLOW_CREDENTIALS", "false"))

# If credentials are not required, allow any origin (useful for LAN testing where the
# frontend origin might be http://192.168.x.x:3005 etc). If you need credentials,
# set explicit allow_origins and CORS_ALLOW_CREDENTIALS=true.
allow_origin_regex = None
if not allow_credentials and "*" not in allow_origins:
    allow_origin_regex = ".*"

app.add_middleware(
    CORSMiddleware,
    # Default to False so wildcard origins work cleanly for local/LAN testing.
    # If you need cookies/credentials, set CORS_ALLOW_CREDENTIALS=true and specify explicit origins.
    allow_credentials=allow_credentials,
    allow_origins=allow_origins,
    allow_origin_regex=allow_origin_regex,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    # Create default admin user
    await create_default_admin(db)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
