from typing import Any, Dict, Optional, List


def _code_variants(code: Optional[str]) -> List[Any]:
    if not code:
        return []
    vals: List[Any] = [code]
    if isinstance(code, str) and code.isdigit() and not code.startswith("0"):
        try:
            vals.append(int(code))
        except Exception:
            pass
    return vals


def build_scope_match(
    district_code: Optional[str] = None,
    block_code: Optional[str] = None,
    udise_code: Optional[str] = None,
    district_name: Optional[str] = None,
    block_name: Optional[str] = None,
    school_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a MongoDB match dict for the common drilldown scope:
    District -> Block -> School.

    Note: Collections are expected to store these fields as:
    - district_code
    - block_code
    - udise_code
    """
    conditions: List[Dict[str, Any]] = []

    if district_code or district_name:
        code_vals = _code_variants(district_code)
        if code_vals and district_name:
            conditions.append({"$or": [{"district_code": {"$in": code_vals}}, {"district_name": district_name}]})
        elif code_vals:
            conditions.append({"district_code": {"$in": code_vals}})
        elif district_name:
            conditions.append({"district_name": district_name})

    if block_code or block_name:
        code_vals = _code_variants(block_code)
        if code_vals and block_name:
            conditions.append({"$or": [{"block_code": {"$in": code_vals}}, {"block_name": block_name}]})
        elif code_vals:
            conditions.append({"block_code": {"$in": code_vals}})
        elif block_name:
            conditions.append({"block_name": block_name})

    if udise_code or school_name:
        code_vals = _code_variants(udise_code)
        if code_vals and school_name:
            conditions.append({"$or": [{"udise_code": {"$in": code_vals}}, {"school_name": school_name}]})
        elif code_vals:
            conditions.append({"udise_code": {"$in": code_vals}})
        elif school_name:
            conditions.append({"school_name": school_name})

    if not conditions:
        return {}
    if len(conditions) == 1:
        return conditions[0]
    return {"$and": conditions}


def prepend_match(pipeline: list, match: Dict[str, Any]) -> list:
    """Prepend a $match stage when match is non-empty."""
    if not match:
        return pipeline
    return [{"$match": match}, *pipeline]


