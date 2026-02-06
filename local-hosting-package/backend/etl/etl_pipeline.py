"""
ETL Pipeline for Maharashtra Education Dashboard
Extracts data from 10 Excel files, transforms and loads into MongoDB
"""
import asyncio
import pandas as pd
import numpy as np
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
from pathlib import Path
import os
from dotenv import load_dotenv
from passlib.context import CryptContext

load_dotenv()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# File paths for 10 Excel datasets
EXCEL_FILES = {
    "aadhaar": "/app/backend/uploads/04f513ce-e611-4640-8cc6-16ec4fa4367b_c2cbg58x_1. Exceptional Data - AADHAAR Status - School Wise - 2025-26 - (State) MAHARASHTRA.xlsx",
    "apaar": "/app/backend/uploads/271ef91a-3d08-43d0-abd5-b4a54f448158_f3kgdk0n_9. APAAR Entry Status - School Wise (Only operational) - ( State ) MAHARASHTRA (1).xlsx",
    "teacher": "/app/backend/uploads/4cdc587e-dc8c-47d2-85ef-53f849ec0bc6_t1xxcumu_2. MAHARASHTRA_School_Wise_Comparison_AY_2025-26.xlsx",
    "water_infra": "/app/backend/uploads/4bf3018e-b91d-49f9-8095-5e72c1c455d2_14rppk9i_3. Drinking_Water_Other_Details_AY_25-26.xlsx",
    "enrolment": "/app/backend/uploads/40361386-97d9-466f-aac0-3f27a6f522be_vozfr1n6_4. Enrolment_Class_Wise_All_Student  - 2025-26 - (State ) MAHARASHTRA.xlsx",
    "dropbox": "/app/backend/uploads/2ad7b328-14e9-4e24-b354-aa78ac1164fb_c3rjolb2_5. Dropbox Remarks Statistics - School Wise - Real Time (State) MAHARASHTRA.xlsx",
    "data_entry": "/app/backend/uploads/afb4e18a-11cf-46b6-9a99-5e87db363cea_nlx0g3rk_6. Data Entry Status- School Wise- Real Time (State) MAHARASHTRA (3).xlsx",
    "age_wise": "/app/backend/uploads/adf19619-4921-4213-98b0-fb0cf3dbddcc_5fblef9o_7. Age Wise - 2025-26.xlsx",
    "ctteacher": "/app/backend/uploads/b39a660d-fd54-4f1b-8f79-29c665f5066e_dvxtnaor_8. CTTeacher Data 2025-26.xlsx",
    "classrooms_toilets": "/app/backend/uploads/6f24c297-fe35-4771-9efc-974e10e899ff_t6x4irms_10. Classrooms_&_Toilet_Details_AY_25-26.xlsx",
}


def safe_int(val):
    """Safely convert to integer"""
    try:
        if pd.isna(val):
            return 0
        # Handle "1-Yes", "2-No" format
        if isinstance(val, str):
            if val.startswith("1-") or val.lower() == "yes":
                return 1
            if val.startswith("2-") or val.lower() == "no":
                return 0
            # Skip placeholder values like (1), (2), etc
            if val.startswith("(") and val.endswith(")"):
                return 0
            # Try to extract leading number
            import re
            match = re.match(r'^(\d+)', val)
            if match:
                return int(match.group(1))
        return int(float(val))
    except:
        return 0


def is_placeholder_row(row):
    """Check if a row contains placeholder values like (1), (2), etc."""
    for val in row.values[:5]:
        if isinstance(val, str) and val.startswith("(") and val.endswith(")"):
            return True
    return False


def read_excel_skip_placeholders(file_path):
    """Read Excel file and skip placeholder rows"""
    df = pd.read_excel(file_path)
    # Filter out placeholder rows
    df = df[~df.apply(is_placeholder_row, axis=1)]
    return df


def safe_float(val):
    """Safely convert to float"""
    try:
        if pd.isna(val):
            return 0.0
        return float(val)
    except:
        return 0.0


def safe_str(val):
    """Safely convert to string"""
    if pd.isna(val):
        return ""
    return str(val).strip()


def extract_block_name(val):
    """Extract block name from 'BlockName & Code' or 'BlockName (Code)' format"""
    if pd.isna(val):
        return ""
    val = str(val)
    # Handle format: "BLOCKNAME (123456)" or "BLOCKNAME & 123456"
    if "(" in val:
        return val.split("(")[0].strip()
    if "&" in val:
        return val.split("&")[0].strip()
    return val.strip()


def extract_district_name(val):
    """Extract district name from 'DistrictName & Code' or 'DistrictName (Code)' format"""
    if pd.isna(val):
        return ""
    val = str(val)
    # Handle format: "DISTRICTNAME (1234)" or "DISTRICTNAME & 1234"
    if "(" in val:
        return val.split("(")[0].strip()
    if "&" in val:
        return val.split("&")[0].strip()
    return val.strip()


class ETLPipeline:
    def __init__(self, mongo_url: str, db_name: str):
        self.client = AsyncIOMotorClient(mongo_url)
        self.db = self.client[db_name]
        self.stats = {}
    
    async def run_full_etl(self):
        """Run complete ETL pipeline for all 10 Excel files"""
        print("=" * 60)
        print("MAHARASHTRA EDUCATION DASHBOARD - ETL PIPELINE")
        print("=" * 60)
        print(f"Started at: {datetime.now()}")
        print()
        
        # Clear existing data
        await self.clear_collections()
        
        # Create admin user
        await self.create_admin_user()
        
        # Process each dataset
        await self.etl_aadhaar()
        await self.etl_apaar()
        await self.etl_teacher()
        await self.etl_water_infrastructure()
        await self.etl_enrolment()
        await self.etl_dropbox()
        await self.etl_data_entry()
        await self.etl_age_wise()
        await self.etl_ctteacher()
        await self.etl_classrooms_toilets()
        
        # Create aggregate collections
        await self.create_districts_summary()
        await self.create_blocks_summary()
        
        # Print summary
        self.print_summary()
        
        print()
        print(f"Completed at: {datetime.now()}")
        print("=" * 60)
    
    async def clear_collections(self):
        """Clear all data collections"""
        collections = [
            "users", "districts", "blocks", "schools",
            "aadhaar_analytics", "apaar_analytics", "teacher_analytics",
            "infrastructure_analytics", "enrolment_analytics", "dropbox_analytics",
            "data_entry_analytics", "age_enrolment", "ctteacher_analytics",
            "classrooms_toilets"
        ]
        print("Clearing existing collections...")
        for coll in collections:
            await self.db[coll].delete_many({})
        print("✓ Collections cleared")
    
    async def create_admin_user(self):
        """Create default admin user"""
        admin = {
            "id": "admin-001",
            "email": "admin@mahaedume.gov.in",
            "full_name": "System Administrator",
            "role": "admin",
            "is_active": True,
            "hashed_password": pwd_context.hash("admin123"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        await self.db.users.insert_one(admin)
        print("✓ Admin user created (admin@mahaedume.gov.in / admin123)")
    
    async def etl_aadhaar(self):
        """ETL for Aadhaar Status data"""
        print("\n[1/10] Processing AADHAAR Status...")
        df = read_excel_skip_placeholders(EXCEL_FILES["aadhaar"])
        
        records = []
        for _, row in df.iterrows():
            total_enrolment = safe_int(row.get("Total Enrolment"))
            aadhaar_passed = safe_int(row.get("Passed Aadhaar validation"))
            aadhaar_failed = safe_int(row.get("Failed Aadhaar validation"))
            aadhaar_pending = safe_int(row.get("Pending Aadhaar validation"))
            aadhaar_not_provided = safe_int(row.get("Aadhaar not provided"))
            name_match = safe_int(row.get("Student name match with Aadhaar name"))
            name_match_verified = safe_int(row.get("Student name match with Aadhaar name (Verified AADHAAR Only)"))
            
            records.append({
                "district_name": safe_str(row.get("District Name")),
                "district_code": safe_str(row.get("District Code")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "total_enrolment": total_enrolment,
                "aadhaar_passed": aadhaar_passed,
                "aadhaar_failed": aadhaar_failed,
                "aadhaar_pending": aadhaar_pending,
                "aadhaar_not_provided": aadhaar_not_provided,
                "name_match": name_match,
                "name_match_verified": name_match_verified,
                "mbu_pending_5_15": safe_int(row.get("MBU Pending (Age 5-15)")),
                "mbu_pending_15_above": safe_int(row.get("MBU Pending (Age 15 and above)")),
                "mbu_not_required": safe_int(row.get("MBU Not Required")),
                "transgender_enrolment": safe_int(row.get("Transgender Enrolment")),
                "exception_rate": round((aadhaar_not_provided / total_enrolment * 100) if total_enrolment > 0 else 0, 2),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.aadhaar_analytics.insert_many(records)
        self.stats["aadhaar"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_apaar(self):
        """ETL for APAAR Entry Status data"""
        print("\n[2/10] Processing APAAR Entry Status...")
        df = read_excel_skip_placeholders(EXCEL_FILES["apaar"])
        
        records = []
        for _, row in df.iterrows():
            total_students = safe_int(row.get("Total Student"))
            total_generated = safe_int(row.get("Total Generated"))
            total_requested = safe_int(row.get("Total Requested"))
            total_failed = safe_int(row.get("Total Failed"))
            total_not_applied = safe_int(row.get("Total Not Applied"))
            generation_rate = (total_generated / total_students * 100) if total_students > 0 else 0
            
            record = {
                "district_name": safe_str(row.get("District Name")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "year": safe_str(row.get("Year")),
                "total_student": total_students,
                "total_generated": total_generated,
                "total_requested": total_requested,
                "total_failed": total_failed,
                "total_not_applied": total_not_applied,
                "generation_rate": round(generation_rate, 2),
                "pending": total_students - total_generated,
            }
            
            # Add class-wise data
            for cls in ['PP3', 'PP2', 'PP1', 'Class1', 'Class2', 'Class3', 'Class4', 'Class5',
                       'Class6', 'Class7', 'Class8', 'Class9', 'Class10', 'Class11', 'Class12']:
                record[f"{cls.lower()}_total_student"] = safe_int(row.get(f"{cls} Total Student"))
                record[f"{cls.lower()}_total_generated"] = safe_int(row.get(f"{cls} Total APAAR Generated"))
                record[f"{cls.lower()}_not_applied"] = safe_int(row.get(f"{cls} APAAR Not Applied"))
            
            record["created_at"] = datetime.now(timezone.utc)
            records.append(record)
        
        if records:
            await self.db.apaar_analytics.insert_many(records)
        self.stats["apaar"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_teacher(self):
        """ETL for Teacher Comparison data"""
        print("\n[3/10] Processing Teacher Analytics...")
        df = read_excel_skip_placeholders(EXCEL_FILES["teacher"])
        
        records = []
        for _, row in df.iterrows():
            records.append({
                "udise_code": safe_str(row.get("UDISE_CODE")),
                "district_name": extract_district_name(row.get("District_Name_&_Code")),
                "block_name": extract_block_name(row.get("BlockName_&_Code")),
                "school_name": safe_str(row.get("School_Name")),
                "school_management_code": safe_str(row.get("School_Management_Code")),
                "school_category_code": safe_str(row.get("School_Category_Code")),
                # Use lowercase field names to match router expectations
                "teacher_tot_py": safe_int(row.get("Teacher_Tot_PY")),
                "teacher_tot_cy": safe_int(row.get("Teacher_Tot_CY")),
                "tot_teacher_deputation_py": safe_int(row.get("Tot_Teacher_Deputation_PY")),
                "tot_teacher_deputation_cy": safe_int(row.get("Tot_Teacher_Deputation_CY")),
                "tot_teacher_teach_oth_sch_py": safe_int(row.get("Tot_Teacher_Teach_Oth_Sch_PY")),
                "tot_teacher_teach_oth_sch_cy": safe_int(row.get("Tot_Teacher_Teach_Oth_Sch_CY")),
                "tot_teacher_tr_cwsn_py": safe_int(row.get("Tot_Teacher_Tr_CWSN_PY")),
                "tot_teacher_tr_cwsn_cy": safe_int(row.get("Tot_Teacher_Tr_CWSN_CY")),
                "tot_teacher_tr_computers_py": safe_int(row.get("Tot_Teacher _Tr_Computers_PY")),
                "tot_teacher_tr_computers_cy": safe_int(row.get("Tot_Teacher _Tr_Computers_CY")),
                "tot_teacher_tr_ctet_py": safe_int(row.get("Tot_Teacher_TR_CTET_PY")),
                "tot_teacher_tr_ctet_cy": safe_int(row.get("Tot_Teacher_TR_CTET_CY")),
                "tot_teacher_below_graduation_py": safe_int(row.get("Tot_Teacher_Below_Graduation_PY")),
                "tot_teacher_below_graduation_cy": safe_int(row.get("Tot_Teacher_Below_Graduation_CY")),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.teacher_analytics.insert_many(records)
        self.stats["teacher"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_water_infrastructure(self):
        """ETL for Drinking Water & Infrastructure data"""
        print("\n[4/10] Processing Water & Infrastructure...")
        df = read_excel_skip_placeholders(EXCEL_FILES["water_infra"])
        
        records = []
        for _, row in df.iterrows():
            records.append({
                "udise_code": safe_str(row.get("UDISE_Code")),
                "overall_status": safe_str(row.get("Overall_Status")),
                "school_name": safe_str(row.get("School_Name")),
                "district_name": extract_district_name(row.get("District_Name_&_Code")),
                "block_name": extract_block_name(row.get("Block_Name_&_Code")),
                "drinking_water_available": 1 if (safe_int(row.get("TapWater_Avail")) > 0 or 
                                                   safe_int(row.get("HandPump_Avail")) > 0 or 
                                                   safe_int(row.get("ProtWell_Avail")) > 0) else 0,
                "tap_water": safe_int(row.get("TapWater_Avail")),
                "hand_pump": safe_int(row.get("HandPump_Avail")),
                "water_purifier": safe_int(row.get("WaterPurf/RO")),
                "water_quality_tested": safe_int(row.get("WaterQltyTesting")),
                "rain_water_harvesting": safe_int(row.get("RainWaterHarv")),
                "library_available": safe_int(row.get("Library")),
                "library_books": safe_int(row.get("Lib_Books")),
                "playground": safe_int(row.get("Playgrnd_Fac")),
                "medical_checkup": safe_int(row.get("MdlCheckup _LstYr")),
                "first_aid": safe_int(row.get("Firstaid_avail")),
                "life_saving": safe_int(row.get("Life_saving_avail")),
                "ramp_available": safe_int(row.get("RampAvail")),
                "special_educator": safe_int(row.get("Spcl_Educator_Avail")),
                "kitchen_garden": safe_int(row.get("Kitc_Gard_Avail")),
                "kitchen_shed": safe_int(row.get("Kitchen_shed")),
                "classroom_dustbin": safe_int(row.get("EachClsRms_Dustbin")),
                "toilet_dustbin": safe_int(row.get("Toilet_Dustbin")),
                "kitchen_dustbin": safe_int(row.get("Kitchen_Dustbin")),
                "furniture_available": safe_int(row.get("Furniture_avail")),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.infrastructure_analytics.insert_many(records)
        self.stats["infrastructure"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_enrolment(self):
        """ETL for Enrolment Class Wise data"""
        print("\n[5/10] Processing Enrolment Data...")
        df = read_excel_skip_placeholders(EXCEL_FILES["enrolment"])
        
        records = []
        for _, row in df.iterrows():
            # Sum up all class-wise enrolment
            boys_total = 0
            girls_total = 0
            trans_total = 0
            
            # PP3, PP2, PP1, Class 1-12
            for col in df.columns:
                if "(Boys)" in col:
                    boys_total += safe_int(row.get(col))
                elif "(Girls)" in col:
                    girls_total += safe_int(row.get(col))
                elif "(Trans)" in col:
                    trans_total += safe_int(row.get(col))
            
            total = boys_total + girls_total + trans_total
            
            records.append({
                "district_name": safe_str(row.get("District Name")),
                "district_code": safe_str(row.get("District Code")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "boys_enrolment": boys_total,
                "girls_enrolment": girls_total,
                "trans_enrolment": trans_total,
                "total_enrolment": total,
                # Pre-Primary class-wise
                "pp3_boys": safe_int(row.get("PP3(Boys)")),
                "pp3_girls": safe_int(row.get("PP3(Girls)")),
                "pp2_boys": safe_int(row.get("PP2(Boys)")),
                "pp2_girls": safe_int(row.get("PP2(Girls)")),
                "pp1_boys": safe_int(row.get("PP1(Boys)")),
                "pp1_girls": safe_int(row.get("PP1(Girls)")),
                # Primary class-wise
                "class1_boys": safe_int(row.get("Class 1(Boys)")),
                "class1_girls": safe_int(row.get("Class 1(Girls)")),
                "class2_boys": safe_int(row.get("Class 2(Boys)")),
                "class2_girls": safe_int(row.get("Class 2(Girls)")),
                "class3_boys": safe_int(row.get("Class 3(Boys)")),
                "class3_girls": safe_int(row.get("Class 3(Girls)")),
                "class4_boys": safe_int(row.get("Class 4(Boys)")),
                "class4_girls": safe_int(row.get("Class 4(Girls)")),
                "class5_boys": safe_int(row.get("Class 5(Boys)")),
                "class5_girls": safe_int(row.get("Class 5(Girls)")),
                # Upper Primary class-wise
                "class6_boys": safe_int(row.get("Class 6(Boys)")),
                "class6_girls": safe_int(row.get("Class 6(Girls)")),
                "class7_boys": safe_int(row.get("Class 7(Boys)")),
                "class7_girls": safe_int(row.get("Class 7(Girls)")),
                "class8_boys": safe_int(row.get("Class 8(Boys)")),
                "class8_girls": safe_int(row.get("Class 8(Girls)")),
                # Secondary class-wise
                "class9_boys": safe_int(row.get("Class 9(Boys)")),
                "class9_girls": safe_int(row.get("Class 9(Girls)")),
                "class10_boys": safe_int(row.get("Class 10(Boys)")),
                "class10_girls": safe_int(row.get("Class 10(Girls)")),
                # Higher Secondary class-wise
                "class11_boys": safe_int(row.get("Class 11(Boys)")),
                "class11_girls": safe_int(row.get("Class 11(Girls)")),
                "class12_boys": safe_int(row.get("Class 12(Boys)")),
                "class12_girls": safe_int(row.get("Class 12(Girls)")),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.enrolment_analytics.insert_many(records)
        self.stats["enrolment"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_dropbox(self):
        """ETL for Dropbox Remarks Statistics"""
        print("\n[6/10] Processing Dropbox Remarks...")
        df = read_excel_skip_placeholders(EXCEL_FILES["dropbox"])
        
        records = []
        for _, row in df.iterrows():
            dropout = safe_int(row.get("Drop Out"))
            death = safe_int(row.get("Due to Death"))
            migrated_domestic = safe_int(row.get("Migrated To Other Block/District/State"))
            migrated_country = safe_int(row.get("Migrated To Other Country"))
            iti_poly = safe_int(row.get("Gone for ITI/PolyTechnic/Other Mode"))
            non_regular = safe_int(row.get(" Gone for Study in Non-Regular Mode"))
            open_school = safe_int(row.get(" Gone for Study in Open Schooling/Un-Recognized Schools"))
            duplicate = safe_int(row.get("Wrong Entry/Duplicate"))
            active_import = safe_int(row.get("Active for Import/Status Not Known "))
            passed_out = safe_int(row.get("Class 12 - Passed Out"))
            
            total_remarks = dropout + death + migrated_domestic + migrated_country + iti_poly + non_regular + open_school + duplicate
            
            records.append({
                "district_name": safe_str(row.get("District Name")),
                "district_code": safe_str(row.get("District Code")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "dropout": dropout,
                "death": death,
                "migrated_domestic": migrated_domestic,
                "migrated_country": migrated_country,
                "iti_poly": iti_poly,
                "non_regular": non_regular,
                "open_school": open_school,
                "duplicate": duplicate,
                "active_import": active_import,
                "passed_out": passed_out,
                "total_remarks": total_remarks,
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.dropbox_analytics.insert_many(records)
        self.stats["dropbox"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_data_entry(self):
        """ETL for Data Entry Status"""
        print("\n[7/10] Processing Data Entry Status...")
        df = read_excel_skip_placeholders(EXCEL_FILES["data_entry"])
        
        records = []
        for _, row in df.iterrows():
            total = safe_int(row.get("Total Students"))
            completed = safe_int(row.get("Total Completed"))
            not_started = safe_int(row.get("Not Started"))
            in_progress = safe_int(row.get("In Progress"))
            
            records.append({
                "district_name": safe_str(row.get("District Name")),
                "district_code": safe_str(row.get("District Code")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "total_students_py": safe_int(row.get("Total Students(Previous Year)")),
                "total_students": total,
                "not_started": not_started,
                "in_progress": in_progress,
                "completed": completed,
                "repeaters": safe_int(row.get("Total Repeaters")),
                "certified": safe_str(row.get("Certified (Yes/No)")),
                "completion_rate": round((completed / total * 100) if total > 0 else 0, 2),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.data_entry_analytics.insert_many(records)
        self.stats["data_entry"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_age_wise(self):
        """ETL for Age Wise Enrolment"""
        print("\n[8/10] Processing Age-Wise Enrolment...")
        df = read_excel_skip_placeholders(EXCEL_FILES["age_wise"])
        
        records = []
        for _, row in df.iterrows():
            # Sum up all class-wise enrolment for boys and girls
            boys_total = 0
            girls_total = 0
            
            for col in df.columns:
                if "(Boys)" in col and "Class" in col:
                    boys_total += safe_int(row.get(col))
                elif "(Girls)" in col and "Class" in col:
                    girls_total += safe_int(row.get(col))
            
            age = safe_int(row.get("Age Wise"))
            
            records.append({
                "district_name": safe_str(row.get("District Name")),
                "district_code": safe_str(row.get("District Code")),
                "block_name": safe_str(row.get("Block Name")),
                "block_code": safe_str(row.get("Block Code")),
                "school_name": safe_str(row.get("School Name")),
                "udise_code": safe_str(row.get("UDISE Code")),
                "school_management": safe_str(row.get("School Management")),
                "school_category": safe_str(row.get("School Category")),
                "age": age,
                "boys": boys_total,
                "girls": girls_total,
                "total": boys_total + girls_total,
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            # Insert in batches due to large size
            batch_size = 10000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                await self.db.age_enrolment.insert_many(batch)
        
        self.stats["age_wise"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_ctteacher(self):
        """ETL for CT Teacher Data"""
        print("\n[9/10] Processing CT Teacher Data...")
        df = read_excel_skip_placeholders(EXCEL_FILES["ctteacher"])
        
        records = []
        for _, row in df.iterrows():
            gender = safe_str(row.get("Gender"))
            ctet_qualified = safe_int(row.get("Ctet Qualified"))
            aadhaar_verified = safe_str(row.get("AADHAAR Verified"))
            
            records.append({
                "udise_code": safe_str(row.get("Udise Code")),
                "school_name": safe_str(row.get("School Name")),
                "district_name": extract_district_name(row.get("District Name & Code")),
                "block_name": extract_block_name(row.get("Block Name & Code")),
                "teacher_name": safe_str(row.get("Teaching Staff Name")),
                "teacher_code": safe_str(row.get("Teaching Staff Code")),
                "gender": gender,
                "dob": safe_str(row.get("DOB")),
                "social_category": safe_str(row.get("Social Category")),
                "academic_qualification": safe_str(row.get("Academic Qualification")),
                "professional_qualification": safe_str(row.get("Professional Qualification")),
                "appointment_type": safe_str(row.get("Nature of Appointment")),
                "staff_type": safe_str(row.get("Staff Type")),
                "classes_taught": safe_str(row.get("Class Taught")),
                "main_subject": safe_str(row.get("Sub Taught_1")),
                "ctet_qualified": ctet_qualified,
                "trained_cwsn": safe_int(row.get("Trained Cwsn")),
                "trained_comp": safe_int(row.get("Trained Comp")),
                "training_nishtha": safe_int(row.get("Training NISHTHA")),
                "aadhaar_verified": 1 if "Verified" in aadhaar_verified else 0,
                "completion_status": safe_str(row.get("Completion Status")),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            # Insert in batches due to large size
            batch_size = 10000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                await self.db.ctteacher_analytics.insert_many(batch)
        
        self.stats["ctteacher"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def etl_classrooms_toilets(self):
        """ETL for Classrooms & Toilets Details"""
        print("\n[10/10] Processing Classrooms & Toilets...")
        df = read_excel_skip_placeholders(EXCEL_FILES["classrooms_toilets"])
        
        records = []
        for _, row in df.iterrows():
            records.append({
                "udise_code": safe_str(row.get("UDISE_Code")),
                "overall_status": safe_str(row.get("Overall_Status")),
                "school_name": safe_str(row.get("School_Name")),
                "district_name": extract_district_name(row.get("District_Name_&_Code")),
                "block_name": extract_block_name(row.get("Block_Name_&_Code")),
                "total_building_blocks": safe_int(row.get("No_Bldg_Blks_Sch_Tot")),
                "classrooms_instructional": safe_int(row.get("Clsrm_UsedforInstPurp")),
                "pucca_good": safe_int(row.get("Pucca_GudCond")),
                "pucca_minor": safe_int(row.get("Pucca_MinRep")),
                "pucca_major": safe_int(row.get("Pucca_MajRep")),
                "part_pucca_good": safe_int(row.get("PartPucca_GudCond")),
                "part_pucca_minor": safe_int(row.get("PartPucca_MinRep")),
                "part_pucca_major": safe_int(row.get("PartPucca_MajRep")),
                # Boys toilets (excluding CWSN)
                "boys_toilets_total": safe_int(row.get("Toilet_ExclCWSN_B_Tot")),
                "boys_toilets_functional": safe_int(row.get("Toilet_ExclCWSN_B_Func")),
                "boys_toilets_water": safe_int(row.get("Toilet_ExclCWSN_RunWat_B")),
                # Girls toilets (excluding CWSN)
                "girls_toilets_total": safe_int(row.get("Toilet_ExclCWSN_G_Tot")),
                "girls_toilets_functional": safe_int(row.get("Toilet_ExclCWSN_G_Func")),
                "girls_toilets_water": safe_int(row.get("Toilet_ExclCWSN_RunWat_G")),
                # CWSN toilets
                "cwsn_boys_total": safe_int(row.get("Toilet_CWSN_B_Tot")),
                "cwsn_boys_functional": safe_int(row.get("Toilet_CWSN_B_Func")),
                "cwsn_boys_water": safe_int(row.get("Toilet_CWSN_RunWat_B")),
                "cwsn_girls_total": safe_int(row.get("Toilet_CWSN_G_Tot")),
                "cwsn_girls_functional": safe_int(row.get("Toilet_CWSN_G_Func")),
                "cwsn_girls_water": safe_int(row.get("Toilet_CWSN_RunWat_G")),
                # Urinals
                "urinals_boys": safe_int(row.get("Urnl_B_Tot")),
                "urinals_girls": safe_int(row.get("Urnl_G_Tot")),
                # Hygiene facilities
                "handwash_toilet": safe_int(row.get("HandwashFac_Toilet/Urnl")),
                "sanitary_pad": safe_int(row.get("Sanitary_Pad")),
                "handwash_facility": safe_int(row.get("Handwash_Facility")),
                "handwash_points": safe_int(row.get("Handwash_Points")),
                # Other
                "classrooms_dilapidated": safe_int(row.get("Clsrm_DilapCond")),
                "electricity": safe_int(row.get("Electricity")),
                "library_room": safe_int(row.get("Library_room")),
                "computer_labs": safe_int(row.get("Computer_Labs")),
                "created_at": datetime.now(timezone.utc)
            })
        
        if records:
            await self.db.classrooms_toilets.insert_many(records)
        self.stats["classrooms_toilets"] = len(records)
        print(f"  ✓ Loaded {len(records)} records")
    
    async def create_districts_summary(self):
        """Create aggregated district summary"""
        print("\n[Aggregation] Creating District Summary...")
        
        # Get unique districts from aadhaar data
        districts = await self.db.aadhaar_analytics.distinct("district_name")
        
        for district in districts:
            if not district:
                continue
            
            # Aggregate data
            aadhaar = await self.db.aadhaar_analytics.aggregate([
                {"$match": {"district_name": district}},
                {"$group": {
                    "_id": None,
                    "total_schools": {"$sum": 1},
                    "total_enrolment": {"$sum": "$total_enrolment"},
                    "aadhaar_available": {"$sum": "$aadhaar_available"}
                }}
            ]).to_list(1)
            
            apaar = await self.db.apaar_analytics.aggregate([
                {"$match": {"district_name": district}},
                {"$group": {
                    "_id": None,
                    "total_students": {"$sum": "$total_student"},
                    "apaar_generated": {"$sum": "$total_generated"}
                }}
            ]).to_list(1)
            
            teacher = await self.db.teacher_analytics.aggregate([
                {"$match": {"district_name": district}},
                {"$group": {
                    "_id": None,
                    "total_teachers": {"$sum": "$teachers_cy"},
                    "ctet_passed": {"$sum": "$ctet_passed"}
                }}
            ]).to_list(1)
            
            # Calculate SHI score
            total_schools = aadhaar[0]["total_schools"] if aadhaar else 0
            aadhaar_pct = (aadhaar[0]["aadhaar_available"] / aadhaar[0]["total_enrolment"] * 100) if aadhaar and aadhaar[0]["total_enrolment"] > 0 else 0
            apaar_pct = (apaar[0]["apaar_generated"] / apaar[0]["total_students"] * 100) if apaar and apaar[0]["total_students"] > 0 else 0
            teacher_quality = (teacher[0]["ctet_passed"] / teacher[0]["total_teachers"] * 100) if teacher and teacher[0]["total_teachers"] > 0 else 0
            
            shi_score = round((aadhaar_pct * 0.3 + apaar_pct * 0.3 + teacher_quality * 0.4), 1)
            rag_status = "green" if shi_score >= 75 else "amber" if shi_score >= 50 else "red"
            
            district_doc = {
                "district_name": district,
                "district_code": district[:3].upper(),
                "total_schools": total_schools,
                "total_students": apaar[0]["total_students"] if apaar else 0,
                "total_teachers": teacher[0]["total_teachers"] if teacher else 0,
                "aadhaar_coverage": round(aadhaar_pct, 2),
                "apaar_coverage": round(apaar_pct, 2),
                "teacher_quality": round(teacher_quality, 2),
                "shi_score": shi_score,
                "rag_status": rag_status,
                "created_at": datetime.now(timezone.utc)
            }
            
            await self.db.districts.insert_one(district_doc)
        
        print(f"  ✓ Created {len(districts)} district summaries")
    
    async def create_blocks_summary(self):
        """Create aggregated block summary"""
        print("\n[Aggregation] Creating Block Summary...")
        
        # Get unique blocks
        pipeline = [
            {"$group": {"_id": {"district": "$district_name", "block": "$block_name"}}},
            {"$project": {"district_name": "$_id.district", "block_name": "$_id.block"}}
        ]
        blocks = await self.db.aadhaar_analytics.aggregate(pipeline).to_list(1000)
        
        count = 0
        for block in blocks:
            district = block.get("district_name")
            block_name = block.get("block_name")
            
            if not block_name:
                continue
            
            # Aggregate by block
            aadhaar = await self.db.aadhaar_analytics.aggregate([
                {"$match": {"block_name": block_name, "district_name": district}},
                {"$group": {
                    "_id": None,
                    "total_schools": {"$sum": 1},
                    "total_enrolment": {"$sum": "$total_enrolment"},
                    "aadhaar_available": {"$sum": "$aadhaar_available"}
                }}
            ]).to_list(1)
            
            apaar = await self.db.apaar_analytics.aggregate([
                {"$match": {"block_name": block_name}},
                {"$group": {
                    "_id": None,
                    "total_students": {"$sum": "$total_student"},
                    "apaar_generated": {"$sum": "$total_generated"}
                }}
            ]).to_list(1)
            
            teacher = await self.db.teacher_analytics.aggregate([
                {"$match": {"block_name": block_name}},
                {"$group": {
                    "_id": None,
                    "total_teachers": {"$sum": "$teachers_cy"},
                    "ctet_passed": {"$sum": "$ctet_passed"}
                }}
            ]).to_list(1)
            
            total_schools = aadhaar[0]["total_schools"] if aadhaar else 0
            aadhaar_pct = (aadhaar[0]["aadhaar_available"] / aadhaar[0]["total_enrolment"] * 100) if aadhaar and aadhaar[0]["total_enrolment"] > 0 else 0
            apaar_pct = (apaar[0]["apaar_generated"] / apaar[0]["total_students"] * 100) if apaar and apaar[0]["total_students"] > 0 else 0
            teacher_quality = (teacher[0]["ctet_passed"] / teacher[0]["total_teachers"] * 100) if teacher and teacher[0]["total_teachers"] > 0 else 0
            
            shi_score = round((aadhaar_pct * 0.3 + apaar_pct * 0.3 + teacher_quality * 0.4), 1)
            rag_status = "green" if shi_score >= 75 else "amber" if shi_score >= 50 else "red"
            
            block_doc = {
                "district_name": district,
                "block_name": block_name,
                "block_code": block_name[:3].upper() if block_name else "",
                "total_schools": total_schools,
                "total_students": apaar[0]["total_students"] if apaar else 0,
                "total_teachers": teacher[0]["total_teachers"] if teacher else 0,
                "aadhaar_coverage": round(aadhaar_pct, 2),
                "apaar_coverage": round(apaar_pct, 2),
                "teacher_quality": round(teacher_quality, 2),
                "shi_score": shi_score,
                "rag_status": rag_status,
                "created_at": datetime.now(timezone.utc)
            }
            
            await self.db.blocks.insert_one(block_doc)
            count += 1
        
        print(f"  ✓ Created {count} block summaries")
    
    def print_summary(self):
        """Print ETL summary"""
        print("\n" + "=" * 60)
        print("ETL SUMMARY")
        print("=" * 60)
        total = 0
        for name, count in self.stats.items():
            print(f"  {name.ljust(20)}: {count:,} records")
            total += count
        print("-" * 60)
        print(f"  {'TOTAL'.ljust(20)}: {total:,} records")


async def main():
    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.environ.get("DB_NAME", "maharashtra_edu")
    
    pipeline = ETLPipeline(mongo_url, db_name)
    await pipeline.run_full_etl()


if __name__ == "__main__":
    asyncio.run(main())
