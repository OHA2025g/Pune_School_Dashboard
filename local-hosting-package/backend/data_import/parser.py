"""
Excel Data Import Module for Maharashtra Education Dashboard
Handles parsing and processing of all 10 datasets
"""

import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import re

logger = logging.getLogger(__name__)

# Dataset type identifiers
DATASET_TYPES = {
    "aadhaar": ["AADHAAR", "Aadhaar Status"],
    "apaar": ["APAAR", "APAAR Entry"],
    "comparison": ["Comparison", "School_Wise_Comparison"],
    "water": ["Drinking_Water", "Water_Details"],
    "enrolment": ["Enrolment", "Class_Wise"],
    "remarks": ["Dropbox Remarks", "Remarks Statistics"],
    "data_entry": ["Data Entry Status"],
    "age": ["Age Wise", "Age_Wise"],
    "teacher": ["Teacher", "CTTeacher"],
    "classroom": ["Classroom", "Toilet_Details"]
}


def identify_dataset_type(filename: str) -> Optional[str]:
    """Identify dataset type from filename"""
    filename_lower = filename.lower()
    for dtype, keywords in DATASET_TYPES.items():
        for keyword in keywords:
            if keyword.lower() in filename_lower:
                return dtype
    return None


def clean_column_name(col: str) -> str:
    """Clean and standardize column names"""
    if not isinstance(col, str):
        return str(col)
    # Remove special characters and standardize
    col = re.sub(r'[^\w\s]', '', col)
    col = col.strip().lower().replace(' ', '_')
    return col


def safe_int(val) -> int:
    """Safely convert to integer"""
    try:
        if pd.isna(val):
            return 0
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def safe_float(val) -> float:
    """Safely convert to float"""
    try:
        if pd.isna(val):
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def safe_str(val) -> str:
    """Safely convert to string"""
    if pd.isna(val):
        return ""
    return str(val).strip()


class DatasetParser:
    """Parser for Maharashtra Education datasets"""
    
    def __init__(self):
        self.schools_data = {}  # UDISE -> school data
        self.districts_data = {}  # District code -> district aggregated data
        self.blocks_data = {}  # Block code -> block aggregated data
    
    def parse_excel(self, file_path: str, filename: str) -> Dict[str, Any]:
        """Parse an Excel file and return structured data"""
        try:
            # Read Excel file
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # Identify dataset type
            dataset_type = identify_dataset_type(filename)
            
            if not dataset_type:
                logger.warning(f"Unknown dataset type for file: {filename}")
                return {"success": False, "error": "Unknown dataset type"}
            
            # Clean column names
            df.columns = [clean_column_name(col) for col in df.columns]
            
            logger.info(f"Parsing {dataset_type} dataset with {len(df)} rows")
            logger.info(f"Columns: {list(df.columns)}")
            
            # Parse based on dataset type
            parser_method = getattr(self, f"parse_{dataset_type}", None)
            if parser_method:
                return parser_method(df, filename)
            else:
                return {"success": False, "error": f"No parser for {dataset_type}"}
                
        except Exception as e:
            logger.error(f"Error parsing {filename}: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def parse_aadhaar(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Aadhaar Status dataset"""
        records = []
        
        # Find relevant columns
        udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c or 'schoolcode' in c]
        district_cols = [c for c in df.columns if 'district' in c]
        block_cols = [c for c in df.columns if 'block' in c]
        
        for _, row in df.iterrows():
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            record = {
                "udise_code": udise,
                "district_code": safe_str(row.get(district_cols[0], "")) if district_cols else "",
                "district_name": safe_str(row.get([c for c in df.columns if 'district_name' in c or 'districtname' in c][0], "")) if [c for c in df.columns if 'district_name' in c or 'districtname' in c] else "",
                "block_name": safe_str(row.get(block_cols[0], "")) if block_cols else "",
                "school_name": safe_str(row.get([c for c in df.columns if 'school_name' in c or 'schoolname' in c][0], "")) if [c for c in df.columns if 'school_name' in c or 'schoolname' in c] else "",
                "total_students": safe_int(row.get('total_student', row.get('total_students', row.get('totalstudent', 0)))),
                "aadhaar_authenticated": safe_int(row.get('aadhaar_authenticated', row.get('authenticated', 0))),
                "aadhaar_pending": safe_int(row.get('aadhaar_pending', row.get('pending', 0))),
            }
            
            # Calculate percentage
            if record["total_students"] > 0:
                record["aadhaar_percentage"] = round(
                    (record["aadhaar_authenticated"] / record["total_students"]) * 100, 1
                )
            else:
                record["aadhaar_percentage"] = 0.0
            
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update({
                "udise_code": udise,
                "school_name": record["school_name"],
                "district_name": record["district_name"],
                "block_name": record["block_name"],
                "aadhaar_percentage": record["aadhaar_percentage"],
                "total_students_aadhaar": record["total_students"]
            })
        
        return {
            "success": True,
            "dataset_type": "aadhaar",
            "records_count": len(records),
            "records": records
        }
    
    def parse_apaar(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse APAAR Entry Status dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            total_col = [c for c in df.columns if 'total' in c and 'student' in c]
            apaar_col = [c for c in df.columns if 'apaar' in c and ('generat' in c or 'creat' in c or 'complet' in c)]
            
            total_students = safe_int(row.get(total_col[0], 0)) if total_col else 0
            apaar_done = safe_int(row.get(apaar_col[0], 0)) if apaar_col else 0
            
            apaar_pct = round((apaar_done / total_students) * 100, 1) if total_students > 0 else 0.0
            
            record = {
                "udise_code": udise,
                "total_students": total_students,
                "apaar_completed": apaar_done,
                "apaar_percentage": apaar_pct
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update({
                "apaar_percentage": apaar_pct,
                "total_students_apaar": total_students
            })
        
        return {
            "success": True,
            "dataset_type": "apaar",
            "records_count": len(records),
            "records": records
        }
    
    def parse_comparison(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse School Wise Comparison dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            record = {
                "udise_code": udise,
                "school_name": safe_str(row.get([c for c in df.columns if 'school_name' in c][0], "")) if [c for c in df.columns if 'school_name' in c] else "",
                "district_name": safe_str(row.get([c for c in df.columns if 'district' in c][0], "")) if [c for c in df.columns if 'district' in c] else "",
                "block_name": safe_str(row.get([c for c in df.columns if 'block' in c][0], "")) if [c for c in df.columns if 'block' in c] else "",
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "comparison",
            "records_count": len(records),
            "records": records
        }
    
    def parse_water(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Drinking Water dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Check for water availability columns
            water_cols = [c for c in df.columns if 'water' in c or 'drinking' in c]
            water_available = True
            for col in water_cols:
                val = safe_str(row.get(col, "")).lower()
                if val in ['no', 'not available', '0', 'false']:
                    water_available = False
                    break
            
            record = {
                "udise_code": udise,
                "water_available": water_available,
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "water",
            "records_count": len(records),
            "records": records
        }
    
    def parse_enrolment(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Enrolment Class Wise dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Sum up class-wise enrolment
            total_students = 0
            boys = 0
            girls = 0
            
            for col in df.columns:
                if 'class' in col or 'grade' in col:
                    total_students += safe_int(row.get(col, 0))
                if 'boy' in col:
                    boys += safe_int(row.get(col, 0))
                if 'girl' in col:
                    girls += safe_int(row.get(col, 0))
            
            # If no class columns, try total columns
            if total_students == 0:
                total_cols = [c for c in df.columns if 'total' in c and ('student' in c or 'enrol' in c)]
                if total_cols:
                    total_students = safe_int(row.get(total_cols[0], 0))
            
            record = {
                "udise_code": udise,
                "total_students": total_students,
                "boys": boys,
                "girls": girls
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update({
                "total_students": total_students,
                "boys": boys,
                "girls": girls
            })
        
        return {
            "success": True,
            "dataset_type": "enrolment",
            "records_count": len(records),
            "records": records
        }
    
    def parse_remarks(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Dropbox Remarks Statistics dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            remarks_cols = [c for c in df.columns if 'remark' in c or 'issue' in c or 'exception' in c]
            has_remarks = False
            for col in remarks_cols:
                if pd.notna(row.get(col)) and safe_str(row.get(col)):
                    has_remarks = True
                    break
            
            record = {
                "udise_code": udise,
                "has_remarks": has_remarks
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "remarks",
            "records_count": len(records),
            "records": records
        }
    
    def parse_data_entry(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Data Entry Status dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Check for status/certified columns
            status_cols = [c for c in df.columns if 'status' in c or 'certif' in c or 'complet' in c]
            certified = False
            data_entry_status = "pending"
            
            for col in status_cols:
                val = safe_str(row.get(col, "")).lower()
                if val in ['yes', 'certified', 'completed', '1', 'true', 'done']:
                    certified = True
                    data_entry_status = "completed"
                    break
            
            record = {
                "udise_code": udise,
                "certified": certified,
                "data_entry_status": data_entry_status
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "data_entry",
            "records_count": len(records),
            "records": records
        }
    
    def parse_age(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Age Wise dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Count age distribution
            age_distribution = {}
            for col in df.columns:
                if 'age' in col or col.isdigit() or re.match(r'\d+', col):
                    age_distribution[col] = safe_int(row.get(col, 0))
            
            record = {
                "udise_code": udise,
                "age_distribution": age_distribution
            }
            records.append(record)
        
        return {
            "success": True,
            "dataset_type": "age",
            "records_count": len(records),
            "records": records
        }
    
    def parse_teacher(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse CT Teacher Data dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Count teachers
            teacher_cols = [c for c in df.columns if 'teacher' in c or 'staff' in c]
            total_teachers = 0
            
            for col in teacher_cols:
                if 'total' in col or 'count' in col:
                    total_teachers = safe_int(row.get(col, 0))
                    break
            
            if total_teachers == 0:
                # Try to count from individual columns
                for col in teacher_cols:
                    total_teachers += safe_int(row.get(col, 0))
            
            record = {
                "udise_code": udise,
                "total_teachers": total_teachers
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "teacher",
            "records_count": len(records),
            "records": records
        }
    
    def parse_classroom(self, df: pd.DataFrame, filename: str) -> Dict[str, Any]:
        """Parse Classrooms & Toilet Details dataset"""
        records = []
        
        for _, row in df.iterrows():
            udise_cols = [c for c in df.columns if 'udise' in c or 'school_code' in c]
            udise = None
            for col in udise_cols:
                if pd.notna(row.get(col)):
                    udise = safe_str(row.get(col))
                    break
            
            if not udise:
                continue
            
            # Find classroom columns
            classroom_cols = [c for c in df.columns if 'classroom' in c or 'room' in c]
            classrooms = 0
            for col in classroom_cols:
                if 'total' in col or 'count' in col or 'number' in col:
                    classrooms = safe_int(row.get(col, 0))
                    break
            
            # Find toilet columns
            toilet_cols = [c for c in df.columns if 'toilet' in c or 'lavator' in c]
            toilets_available = True
            boys_toilets = 0
            girls_toilets = 0
            
            for col in toilet_cols:
                val = row.get(col)
                if 'boy' in col:
                    boys_toilets = safe_int(val)
                elif 'girl' in col:
                    girls_toilets = safe_int(val)
                elif pd.notna(val):
                    val_str = safe_str(val).lower()
                    if val_str in ['no', 'not available', '0', 'false']:
                        toilets_available = False
            
            record = {
                "udise_code": udise,
                "classrooms": classrooms,
                "toilets_available": toilets_available,
                "boys_toilets": boys_toilets,
                "girls_toilets": girls_toilets
            }
            records.append(record)
            
            # Update school data
            if udise not in self.schools_data:
                self.schools_data[udise] = {}
            self.schools_data[udise].update(record)
        
        return {
            "success": True,
            "dataset_type": "classroom",
            "records_count": len(records),
            "records": records
        }
    
    def get_aggregated_data(self) -> Dict[str, Any]:
        """Get aggregated data at district and block levels"""
        districts = {}
        blocks = {}
        
        for udise, school in self.schools_data.items():
            district_name = school.get("district_name", "Unknown")
            block_name = school.get("block_name", "Unknown")
            
            # Skip if no district info
            if not district_name or district_name == "Unknown":
                continue
            
            # Initialize district
            if district_name not in districts:
                districts[district_name] = {
                    "district_name": district_name,
                    "total_schools": 0,
                    "total_students": 0,
                    "total_teachers": 0,
                    "aadhaar_sum": 0,
                    "apaar_sum": 0,
                    "water_count": 0,
                    "toilet_count": 0,
                    "certified_count": 0,
                    "schools_with_data": 0
                }
            
            # Initialize block
            block_key = f"{district_name}|{block_name}"
            if block_key not in blocks:
                blocks[block_key] = {
                    "district_name": district_name,
                    "block_name": block_name,
                    "total_schools": 0,
                    "total_students": 0,
                    "aadhaar_sum": 0,
                    "apaar_sum": 0,
                    "schools_with_data": 0
                }
            
            # Aggregate
            districts[district_name]["total_schools"] += 1
            districts[district_name]["total_students"] += school.get("total_students", 0)
            districts[district_name]["total_teachers"] += school.get("total_teachers", 0)
            
            if school.get("aadhaar_percentage"):
                districts[district_name]["aadhaar_sum"] += school["aadhaar_percentage"]
                districts[district_name]["schools_with_data"] += 1
            
            if school.get("apaar_percentage"):
                districts[district_name]["apaar_sum"] += school["apaar_percentage"]
            
            if school.get("water_available"):
                districts[district_name]["water_count"] += 1
            
            if school.get("toilets_available"):
                districts[district_name]["toilet_count"] += 1
            
            if school.get("certified"):
                districts[district_name]["certified_count"] += 1
            
            # Block aggregation
            blocks[block_key]["total_schools"] += 1
            blocks[block_key]["total_students"] += school.get("total_students", 0)
            
            if school.get("aadhaar_percentage"):
                blocks[block_key]["aadhaar_sum"] += school["aadhaar_percentage"]
                blocks[block_key]["schools_with_data"] += 1
            
            if school.get("apaar_percentage"):
                blocks[block_key]["apaar_sum"] += school["apaar_percentage"]
        
        # Calculate averages
        for d in districts.values():
            if d["schools_with_data"] > 0:
                d["aadhaar_percentage"] = round(d["aadhaar_sum"] / d["schools_with_data"], 1)
                d["apaar_percentage"] = round(d["apaar_sum"] / d["schools_with_data"], 1)
            else:
                d["aadhaar_percentage"] = 0.0
                d["apaar_percentage"] = 0.0
            
            if d["total_schools"] > 0:
                d["water_percentage"] = round((d["water_count"] / d["total_schools"]) * 100, 1)
                d["toilet_percentage"] = round((d["toilet_count"] / d["total_schools"]) * 100, 1)
                d["data_entry_percentage"] = round((d["certified_count"] / d["total_schools"]) * 100, 1)
            else:
                d["water_percentage"] = 0.0
                d["toilet_percentage"] = 0.0
                d["data_entry_percentage"] = 0.0
            
            if d["total_teachers"] > 0:
                d["avg_ptr"] = round(d["total_students"] / d["total_teachers"], 1)
            else:
                d["avg_ptr"] = 0.0
        
        for b in blocks.values():
            if b["schools_with_data"] > 0:
                b["aadhaar_percentage"] = round(b["aadhaar_sum"] / b["schools_with_data"], 1)
                b["apaar_percentage"] = round(b["apaar_sum"] / b["schools_with_data"], 1)
            else:
                b["aadhaar_percentage"] = 0.0
                b["apaar_percentage"] = 0.0
        
        return {
            "schools": list(self.schools_data.values()),
            "districts": list(districts.values()),
            "blocks": list(blocks.values())
        }
