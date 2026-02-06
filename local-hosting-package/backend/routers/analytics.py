"""Advanced Analytics and AI-Powered Predictions"""
from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple
import os
import json
import uuid
from pathlib import Path
from dotenv import load_dotenv
import numpy as np
from datetime import date

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")
# Optional local overrides (do not commit secrets)
load_dotenv(ROOT_DIR / ".env.local", override=True)

from utils.auth import get_current_user
from utils.scope import build_scope_match, prepend_match

router = APIRouter(prefix="/analytics", tags=["Analytics"])

# Database will be injected
db = None

def init_db(database):
    global db
    db = database

SYSTEM_PROMPT = """You are an expert education data analyst for the Maharashtra Education Department.
        Analyze school metrics data and provide actionable insights, predictions, and recommendations.
        Be specific with numbers, percentages, and block names when available.
        Format responses with clear sections using markdown headers.
        Focus on practical, implementable recommendations."""

@router.get("/ai/status")
async def ai_status(current_user: dict = Depends(get_current_user)):
    """Return whether insights generation is enabled (without exposing secrets)."""
    return {
        "provider": os.environ.get("INSIGHTS_PROVIDER", "local"),
        # local mode is always enabled; openai mode depends on key
        "enabled": os.environ.get("INSIGHTS_PROVIDER", "local") != "openai"
        or bool(os.environ.get("OPENAI_API_KEY")),
        "model": os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
    }

def _format_ai_exception(e: Exception) -> str:
    msg = str(e)
    return f"AI analysis unavailable: {msg}"

def _scope_level(district_code: Optional[str], block_code: Optional[str], udise_code: Optional[str]) -> str:
    if udise_code:
        return "school"
    if block_code:
        return "block"
    if district_code:
        return "district"
    return "state"


def _scope_prefix_md(district_code: Optional[str], block_code: Optional[str], udise_code: Optional[str]) -> str:
    level = _scope_level(district_code, block_code, udise_code)
    lines = []
    lines.append("## Scope")
    lines.append(f"- Level: **{level}**")
    if district_code:
        lines.append(f"- District code: **{district_code}**")
    if block_code:
        lines.append(f"- Block code: **{block_code}**")
    if udise_code:
        lines.append(f"- UDISE code: **{udise_code}**")
    return "\n".join(lines)


def _safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.array(values, dtype=float), p))


def _z_scores(values: List[float]) -> List[float]:
    if not values:
        return []
    arr = np.array(values, dtype=float)
    std = float(arr.std()) if len(arr) > 1 else 0.0
    if std == 0.0:
        return [0.0 for _ in values]
    mean = float(arr.mean())
    return [float((x - mean) / std) for x in arr.tolist()]


def _top(items: List[Dict[str, Any]], key: str, n: int = 5) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda x: float(x.get(key, 0) or 0), reverse=True)[:n]


def _bottom(items: List[Dict[str, Any]], key: str, n: int = 5) -> List[Dict[str, Any]]:
    return sorted(items, key=lambda x: float(x.get(key, 0) or 0))[:n]


def _local_dropout_insights(risk_data: Any) -> str:
    # Backwards compatible: accept list OR payload dict
    payload: Dict[str, Any] = risk_data if isinstance(risk_data, dict) else {"risk_data": risk_data}
    risk_data = payload.get("risk_data", []) or []
    entity_label = payload.get("entity_label") or "blocks"

    if not risk_data:
        return "No data available to generate insights."

    avg_dropout = round(sum(r["dropout_rate"] for r in risk_data) / max(len(risk_data), 1), 2)
    high = [r for r in risk_data if r.get("risk_level") == "High"]
    top5 = _top(risk_data, "risk_score", 5)

    # ML-style: z-score outliers on risk_score
    z = _z_scores([float(r.get("risk_score", 0) or 0) for r in risk_data])
    outliers = [
        {**risk_data[i], "z": round(z[i], 2)}
        for i in range(len(risk_data))
        if z[i] >= 1.5
    ][:5]

    lines = []
    lines.append("## Key Findings")
    lines.append(f"- Total {entity_label} analyzed: **{len(risk_data)}**")
    lines.append(f"- Average dropout rate: **{avg_dropout}%**")
    lines.append(f"- High-risk {entity_label}: **{len(high)}**")
    if top5:
        lines.append(
            f"- Highest risk: **{top5[0]['block']}** (risk score **{top5[0]['risk_score']}**, dropout rate **{top5[0]['dropout_rate']}%**)"
        )

    lines.append(f"\n## High Risk {entity_label.title()} (Top 5)")
    for r in top5:
        lines.append(
            f"- **{r['block']}**: dropout **{r['dropout_count']}**, migration **{r['migration_count']}**, "
            f"dropout rate **{r['dropout_rate']}%**, risk score **{r['risk_score']}**"
        )

    lines.append("\n## Root Cause Signals (data-driven)")
    lines.append("- **High migration share** often indicates seasonal movement or documentation issues; prioritize verification + tracking.")
    lines.append("- **High dropout with low migration** suggests retention/attendance challenges; prioritize counselling + attendance monitoring.")
    if outliers:
        lines.append(f"- **Statistical outliers** (z-score ≥ 1.5) indicate {entity_label} disproportionately driving risk:")
        for o in outliers:
            lines.append(f"  - **{o['block']}** (risk score {o['risk_score']}, z={o['z']})")

    lines.append("\n## Recommendations (no LLM)")
    lines.append(f"- **Targeted outreach** in top-risk {entity_label}: identify at-risk students weekly and follow up with parents.")
    lines.append("- **Migration workflow**: create a standard checklist for transfer/migration cases to reduce misclassification.")
    lines.append("- **Attendance early-warning**: flag students with repeated absences and intervene within 7 days.")
    lines.append(f"- **Monthly review**: review dropout/migration counts with {entity_label} owners and cluster officers.")

    lines.append("\n## Priority Action Items (next 30 days)")
    lines.append("- **Week 1**: publish top-10 at-risk schools per high-risk block; assign case owners.")
    lines.append("- **Week 2**: run parent counselling drives in top 2 blocks; track attendance improvements weekly.")
    lines.append("- **Week 3–4**: audit migration entries for top 2 blocks; correct data + update SOP.")

    return "\n".join(lines)


def _local_infra_insights(forecast_data: Any) -> str:
    payload: Dict[str, Any] = forecast_data if isinstance(forecast_data, dict) else {"forecast_data": forecast_data}
    forecast_data = payload.get("forecast_data", []) or []
    entity_label = payload.get("entity_label") or "blocks"

    if not forecast_data:
        return "No data available to generate insights."

    total_classrooms = sum(int(f.get("total_classrooms", 0) or 0) for f in forecast_data)
    repair_needed = sum(int(f.get("current_repair_needed", 0) or 0) for f in forecast_data)
    dilapidated = sum(int(f.get("dilapidated", 0) or 0) for f in forecast_data)
    repair_rate = round(_safe_div(repair_needed, total_classrooms) * 100, 2) if total_classrooms else 0.0

    # Priority ranking: use estimated budget + dilapidated + repair
    scored = []
    for f in forecast_data:
        tc = int(f.get("total_classrooms", 0) or 0)
        rn = int(f.get("current_repair_needed", 0) or 0)
        dil = int(f.get("dilapidated", 0) or 0)
        budget = float(f.get("estimated_budget_lakhs", 0) or 0)
        score = (10 * _safe_div(rn, max(tc, 1))) + (20 * _safe_div(dil, max(tc, 1))) + (0.02 * budget)
        scored.append({**f, "_score": round(score, 3)})
    top5 = _top(scored, "_score", 5)
    top3 = top5[:3]
    # Use medians to flag outliers (robust to extremes)
    med_repair = _percentile([float(f.get("repair_rate", 0) or 0) for f in forecast_data], 50)
    med_dil = _percentile([float(f.get("dilapidated", 0) or 0) for f in forecast_data], 50)

    lines = []
    lines.append("## Infrastructure Health Summary")
    lines.append(f"- Total classrooms: **{total_classrooms:,}**")
    lines.append(f"- Current repair needed: **{repair_needed:,}** (**{repair_rate}%** of classrooms)")
    lines.append(f"- Dilapidated classrooms: **{dilapidated:,}**")

    lines.append(f"\n## Critical {entity_label.title()} (Top 5)")
    for f in top5:
        lines.append(
            f"- **{f['block']}**: repair needed **{f['current_repair_needed']}** / **{f['total_classrooms']}**, "
            f"dilapidated **{f['dilapidated']}**, est. budget **₹{round(float(f.get('estimated_budget_lakhs',0) or 0),1)}L**"
        )

    # These headings are required by the frontend accordion buckets.
    lines.append("\n## Root Cause Signals (data-driven)")
    lines.append(
        f"- Median repair rate across {entity_label}: **{round(med_repair,1)}%**; units above this are likely facing backlog + maintenance gaps."
    )
    lines.append(
        f"- Median dilapidated classrooms across {entity_label}: **{int(round(med_dil))}**; units above this indicate safety-critical risk."
    )
    if top3:
        lines.append(f"- Highest composite risk {entity_label} (repair share + dilapidation + budget pressure):")
        for f in top3:
            lines.append(
                f"  - **{f['block']}**: repair rate **{f.get('repair_rate', 0)}%**, "
                f"dilapidated **{f.get('dilapidated', 0)}**, est. **₹{round(float(f.get('estimated_budget_lakhs',0) or 0),1)}L**"
            )

    lines.append("\n## Recommendations")
    lines.append("- **Safety-first sequencing**: address dilapidated classrooms + major repairs before minor repairs.")
    lines.append("- **Preventive maintenance cadence**: quarterly condition checks + minor repairs to reduce next-year major repairs.")
    lines.append("- **Contractor pipeline**: pre-empanel contractors + standard BOQs to shorten cycle time.")
    lines.append(f"- **Budget governance**: track spend vs. repaired classrooms (output KPI) per {entity_label[:-1] if entity_label.endswith('s') else entity_label} monthly.")

    lines.append("\n## Priority Action Items (next 30–60 days)")
    lines.append(f"- **Week 1–2**: verify and freeze the repair list (especially dilapidated) for the top 3 risk {entity_label}.")
    lines.append(f"- **Week 2–4**: begin major repairs in top 2 {entity_label}; publish a weekly completion tracker (classrooms repaired).")
    lines.append(f"- **Week 4–8**: execute minor repairs across remaining high-score {entity_label}; re-run condition survey end of month 2.")

    lines.append("\n## Budget Allocation (data-driven)")
    lines.append("- Prioritize blocks with **high dilapidated share** first (safety risk), then high repair share (preventive maintenance).")
    lines.append("- Split budget into **60% safety-critical repairs**, **30% preventive repairs**, **10% contingency**.")

    lines.append("\n## Maintenance Schedule")
    lines.append("- **0–3 months**: address dilapidated classrooms + major repairs in top 2 blocks.")
    lines.append("- **3–6 months**: complete minor repairs across remaining high-score blocks.")
    lines.append("- **Quarterly**: re-run condition survey and update forecast.")

    lines.append("\n## Long-term Planning (3-year)")
    lines.append("- Create an annual **block-wise capex plan** and track condition improvements as a KPI.")
    lines.append("- Standardize procurement + contractor empanelment to reduce repair cycle time.")
    return "\n".join(lines)


def _local_teacher_insights(shortage_data: Any) -> str:
    payload: Dict[str, Any] = shortage_data if isinstance(shortage_data, dict) else {"shortage_data": shortage_data}
    shortage_data = payload.get("shortage_data", []) or []
    age_distribution = payload.get("age_distribution", {}) or {}
    entity_label = payload.get("entity_label") or "blocks"

    if not shortage_data:
        return "No data available to generate insights."

    total_teachers = sum(int(s.get("total_teachers", 0) or 0) for s in shortage_data)
    retiring_5 = sum(int(s.get("retiring_in_5_years", 0) or 0) for s in shortage_data)
    avg_ctet = round(sum(float(s.get("ctet_rate", 0) or 0) for s in shortage_data) / max(len(shortage_data), 1), 1)
    high_risk = [s for s in shortage_data if s.get("risk_level") == "High"]
    top5 = _top(shortage_data, "retirement_risk_pct", 5)
    top3 = top5[:3]
    ctet_median = _percentile([float(s.get("ctet_rate", 0) or 0) for s in shortage_data], 50)
    net_shortage_pos = [s for s in shortage_data if int(s.get("forecast_shortage_5yr", 0) or 0) > 0]

    lines = []
    lines.append("## Workforce Health Analysis")
    lines.append(f"- Total teachers analyzed: **{total_teachers:,}**")
    lines.append(f"- Retiring in 5 years: **{retiring_5:,}** (**{round(_safe_div(retiring_5, total_teachers)*100,1)}%**)")
    lines.append(f"- Average CTET qualification rate: **{avg_ctet}%**")
    lines.append(f"- High retirement-risk {entity_label}: **{len(high_risk)}**")

    lines.append(f"\n## Retirement Wave Impact (Top 5 {entity_label})")
    for s in top5:
        lines.append(
            f"- **{s['block']}**: retiring(5y) **{s['retiring_in_5_years']}** / **{s['total_teachers']}** "
            f"({s['retirement_risk_pct']}%), CTET **{s['ctet_rate']}%**, net shortage(5y) **{s['forecast_shortage_5yr']}**"
        )

    # These headings are required by the frontend accordion buckets.
    lines.append("\n## Root Cause Signals (data-driven)")
    lines.append(f"- Median CTET rate across {entity_label}: **{round(ctet_median,1)}%**; units below this likely need targeted certification support.")
    lines.append(f"- {entity_label.title()} with **high retirement risk** plus **positive net shortage** will see staffing stress first.")
    if top3:
        lines.append(f"- Most urgent {entity_label} (highest retirement risk):")
        for s in top3:
            lines.append(
                f"  - **{s['block']}**: retirement risk **{s.get('retirement_risk_pct', 0)}%**, "
                f"net shortage(5y) **{s.get('forecast_shortage_5yr', 0)}**, CTET **{s.get('ctet_rate', 0)}%**"
            )
    if net_shortage_pos:
        worst = _top(net_shortage_pos, "forecast_shortage_5yr", 3)
        lines.append("- Net-shortage hotspots (retirements > new entrants):")
        for s in worst:
            lines.append(f"  - **{s['block']}**: net shortage(5y) **{s.get('forecast_shortage_5yr', 0)}**")

    lines.append("\n## Recommendations")
    lines.append("- **Hiring plan**: prioritize recruitment/transfer into blocks with high retirement risk and net shortage.")
    lines.append("- **CTET improvement**: run block-wise CTET prep cohorts for blocks below median; track monthly pass/registration.")
    lines.append("- **Succession planning**: assign deputies for key roles in blocks with high 55+ share; start handover now.")
    lines.append("- **Mid-year buffer**: maintain a small pool for emergency redeployment to blocks with high 3-year retirements.")

    lines.append("\n## Priority Action Items (next 30–60 days)")
    lines.append(f"- **Week 1**: publish unit-wise retirement + vacancy forecast (top 5 {entity_label}) and assign HR owners.")
    lines.append(f"- **Week 2–3**: initiate transfers/attachments to top 2 shortage {entity_label}; create a substitute roster.")
    lines.append(f"- **Week 3–8**: run CTET upskilling camps in {entity_label} below median; monitor weekly attendance + completion.")

    lines.append("\n## Hiring & Deployment Requirements (data-driven)")
    lines.append("- Prioritize recruitment/transfer into blocks with **high retirement risk** and **negative 5-year net**.")
    lines.append("- Maintain a buffer pool for **mid-year vacancies** in blocks with high 3-year retirement counts.")

    lines.append("\n## Training Priorities (CTET / capacity)")
    lines.append("- Focus CTET training in blocks with **CTET rate below district median**.")
    lines.append("- Pair senior teachers (55+) with new entrants for **handover of key roles** before retirement.")

    lines.append("\n## Succession Planning (5-year)")
    lines.append("- Create a **block-wise replacement plan** (retirements minus new entrants) updated quarterly.")
    lines.append("- Track KPIs: CTET rate, vacancy fill time, teacher-to-student ratio (PTR) where available.")

    lines.append("\n## Age Distribution Snapshot")
    lines.append(f"- {json.dumps(age_distribution, indent=2)}")
    return "\n".join(lines)


def _parse_ddmmyyyy(s: Any) -> Optional[date]:
    if not s or not isinstance(s, str):
        return None
    try:
        parts = s.strip().split("/")
        if len(parts) != 3:
            return None
        dd, mm, yyyy = int(parts[0]), int(parts[1]), int(parts[2])
        return date(yyyy, mm, dd)
    except Exception:
        return None


def _age_from_dob(dob: Any) -> Optional[int]:
    d = _parse_ddmmyyyy(dob)
    if not d:
        return None
    today = date.today()
    years = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
    if years < 0 or years > 120:
        return None
    return years


def _local_completion_insights(block_data: Any) -> str:
    payload: Dict[str, Any] = block_data if isinstance(block_data, dict) else {"block_data": block_data}
    block_data = payload.get("block_data", []) or []
    overall_rate = float(payload.get("overall_rate", 0) or 0)
    entity_label = payload.get("entity_label") or "blocks"

    if not block_data:
        return "No data available to generate insights."

    bottom5 = _bottom(block_data, "rate", 5)
    top5 = _top(block_data, "rate", 5)
    max_weeks = max(int(b.get("estimated_weeks", 0) or 0) for b in block_data)
    median_rate = _percentile([float(b.get("rate", 0) or 0) for b in block_data], 50)

    lines = []
    lines.append("## Completion Status Summary")
    lines.append(f"- Overall generation rate: **{overall_rate}%**")
    lines.append(f"- Estimated worst-case timeline (based on 2% weekly assumption): **{max_weeks} weeks**")

    lines.append(f"\n## Bottleneck {entity_label.title()} (lowest completion)")
    for b in bottom5:
        lines.append(
            f"- **{b['block']}**: rate **{b['rate']}%**, pending **{b['pending']:,}**, est. **{b['estimated_weeks']} weeks**"
        )

    lines.append(f"\n## Top Performing {entity_label.title()} (highest completion)")
    for b in top5:
        lines.append(f"- **{b['block']}**: rate **{b['rate']}%** (pending **{b['pending']:,}**)")

    # These headings are required by the frontend accordion buckets.
    lines.append("\n## Root Cause Signals (data-driven)")
    lines.append(f"- Median generation rate across {entity_label}: **{round(median_rate,1)}%**; units below this need focused camps + exception handling.")
    if bottom5:
        worst = bottom5[0]
        lines.append(
            f"- Worst bottleneck: **{worst.get('block')}** with **{worst.get('rate')}%** rate and **{int(worst.get('pending', 0) or 0):,}** pending."
        )
    lines.append("- Common blockers: identity mismatches, missing consent, school-level data entry gaps, and low device/internet access.")

    lines.append("\n## Recommendations")
    lines.append(f"- **Weekly completion camps** in bottom 5 {entity_label}, with a fixed calendar and on-site support.")
    lines.append("- **Exception list workflow**: top pending schools per unit, assigned owners, reviewed weekly.")
    lines.append("- **Pre-validation**: run checks to reduce rejections (identity mismatch, missing fields) before submission.")
    lines.append("- **Incentivize throughput**: publish a simple leaderboard of schools improved week-over-week.")

    lines.append("\n## Priority Action Items (next 30 days)")
    lines.append(f"- **Week 1**: publish bottom-10 schools per bottom-5 {entity_label}; assign case owners and camp dates.")
    lines.append(f"- **Week 2**: run 2 camps per bottom {entity_label[:-1] if entity_label.endswith('s') else entity_label}; clear identity mismatch backlog first.")
    lines.append(f"- **Week 3–4**: re-run exception list; target +10pp improvement in the bottom 3 {entity_label}.")

    lines.append("\n## Acceleration Strategy (process + data)")
    lines.append("- Run weekly **exception lists** (schools with low rate) and assign accountability at cluster level.")
    lines.append("- Improve throughput by batching: schedule **fixed weekly camps** per low-performing block.")
    lines.append("- Resolve blockers: Aadhaar/identity mismatches, missing consent, or data entry gaps.")

    lines.append("\n## Risk Factors")
    lines.append("- Low internet/device availability at schools; plan mobile/offline capture where needed.")
    lines.append("- Data quality issues causing rejections; add validation checks before submission.")
    return "\n".join(lines)


def _local_executive_summary(metrics: Dict[str, Any]) -> str:
    # Backwards compatible wrapper: older callers passed only metrics. New callers pass a richer payload.
    payload: Dict[str, Any] = metrics if isinstance(metrics, dict) else {}
    if "metrics" in payload:
        metrics = payload.get("metrics") or {}
    else:
        metrics = payload or {}

    if not metrics:
        return "No data available to generate executive summary."

    scope = payload.get("scope") or {}
    level = scope.get("level") or "state"
    district_name = payload.get("district_name") or ""
    block_name = payload.get("block_name") or ""
    school_name = payload.get("school_name") or ""

    def _fmt_pp(delta: float) -> str:
        sign = "+" if delta > 0 else ""
        return f"{sign}{round(delta,1)}pp"

    # Comparators (if present)
    cmp = payload.get("comparators") or {}
    district_cmp = cmp.get("district") or {}
    block_cmp = cmp.get("block") or {}

    # Rankings (if present)
    worst = payload.get("worst") or {}
    best = payload.get("best") or {}

    classroom_health = float(metrics.get("classroom_health", 0) or 0)
    toilet_functional = float(metrics.get("toilet_functional", 0) or 0)
    apaar_rate = float(metrics.get("apaar_rate", 0) or 0)
    dropout_rate = float(metrics.get("dropout_rate", 0) or 0)

    # Identify the weakest KPI (simple heuristic)
    kpis = {
        "Classroom health": classroom_health,
        "Toilet functionality": toilet_functional,
        "APAAR generation": apaar_rate,
        # for dropout, lower is better, so invert to a "health" score out of 100
        "Dropout rate": max(0.0, 100.0 - min(100.0, dropout_rate * 20.0)),
    }
    weakest = min(kpis.items(), key=lambda kv: kv[1])[0] if kpis else "APAAR generation"

    title = "Executive Summary"
    if level == "district":
        title = f"District Insights{f' — {district_name}' if district_name else ''}"
    elif level == "block":
        title = f"Block Insights{f' — {block_name}' if block_name else ''}"
    elif level == "school":
        title = f"School Insights{f' — {school_name}' if school_name else ''}"

    lines: List[str] = []
    lines.append(f"## {title}")
    lines.append(
        f"Snapshot across **{metrics.get('schools', 0)}** schools, **{metrics.get('teachers', 0)}** teachers and "
        f"**{metrics.get('students', 0):,}** students."
    )
    lines.append("")
    lines.append("### KPI Snapshot")
    lines.append(f"- Classroom health: **{round(classroom_health,1)}%**")
    lines.append(f"- Toilet functionality: **{round(toilet_functional,1)}%**")
    lines.append(f"- APAAR generation rate: **{round(apaar_rate,1)}%**")
    lines.append(f"- Dropout rate: **{round(dropout_rate,2)}%**")

    # These headings are required by the frontend accordion buckets.
    lines.append("\n## Root Cause Signals (data-driven)")
    lines.append(f"- Weakest KPI in this scope: **{weakest}**.")

    if level == "school" and block_cmp:
        # school vs block deltas
        try:
            lines.append(
                f"- vs block average: classroom {_fmt_pp(classroom_health - float(block_cmp.get('classroom_health', 0) or 0))}, "
                f"toilets {_fmt_pp(toilet_functional - float(block_cmp.get('toilet_functional', 0) or 0))}, "
                f"APAAR {_fmt_pp(apaar_rate - float(block_cmp.get('apaar_rate', 0) or 0))}."
            )
        except Exception:
            pass
    if level in ("block", "school") and district_cmp:
        try:
            lines.append(
                f"- vs district average: classroom {_fmt_pp(classroom_health - float(district_cmp.get('classroom_health', 0) or 0))}, "
                f"toilets {_fmt_pp(toilet_functional - float(district_cmp.get('toilet_functional', 0) or 0))}, "
                f"APAAR {_fmt_pp(apaar_rate - float(district_cmp.get('apaar_rate', 0) or 0))}."
            )
        except Exception:
            pass

    if worst:
        # Show the most relevant “bottom list” based on scope
        if level == "district" and worst.get("blocks"):
            lines.append("- Bottom blocks driving the gaps:")
            for item in (worst.get("blocks") or [])[:5]:
                lines.append(
                    f"  - **{item.get('name','')}**: APAAR **{item.get('apaar_rate','-')}%**, "
                    f"classroom **{item.get('classroom_health','-')}%**, toilets **{item.get('toilet_functional','-')}%**, "
                    f"dropout **{item.get('dropout_rate','-')}%**"
                )
        if level == "block" and worst.get("schools"):
            lines.append("- Bottom schools driving the gaps:")
            for item in (worst.get("schools") or [])[:5]:
                lines.append(
                    f"  - **{item.get('name','')}**: APAAR **{item.get('apaar_rate','-')}%**, "
                    f"classroom **{item.get('classroom_health','-')}%**, toilets **{item.get('toilet_functional','-')}%**"
                )

    lines.append("\n## Recommendations")
    # Tailor recommendations to weakest KPI + available bottom entities
    if weakest == "APAAR generation":
        lines.append("- Run **weekly APAAR camps** focused on the bottom-performing units (blocks/schools listed above).")
        lines.append("- Clear **identity mismatch / consent** blockers first; publish an exception list each week.")
        lines.append("- Track improvements as **week-over-week APAAR rate (+pp)** per unit.")
    elif weakest == "Toilet functionality":
        lines.append("- Prioritize **functional restoration** for non-functional toilets in the bottom units; track completion weekly.")
        lines.append("- Create a **minor repair roster** (plumber/electrician) and fix within 7 days for any new breakdown.")
        lines.append("- Add monthly **verification checks** to prevent reporting drift.")
    elif weakest == "Classroom health":
        lines.append("- Prioritize **major repairs/dilapidated** first, then preventive maintenance in bottom units.")
        lines.append("- Standardize BOQs + contractor empanelment to reduce turnaround time.")
        lines.append("- Track outputs: **classrooms repaired per week** and resulting health % improvement.")
    else:
        lines.append("- Strengthen early-warning tracking for dropout/migration and follow up cases weekly.")
        lines.append("- Validate classifications (dropout vs migration) to reduce misreporting.")
        lines.append("- Review at-risk units weekly and assign accountability owners.")

    lines.append("\n## Priority Action Items (next 30 days)")
    # Make actions concrete by referencing current bottom entities when possible
    if level == "district" and worst.get("blocks"):
        top_targets = [b.get("name") for b in (worst.get("blocks") or [])[:3] if b.get("name")]
        if top_targets:
            lines.append(f"- **Week 1**: focus support on blocks: **{', '.join(top_targets)}**; publish unit-wise exception lists.")
    if level == "block" and worst.get("schools"):
        top_targets = [s.get("name") for s in (worst.get("schools") or [])[:3] if s.get("name")]
        if top_targets:
            lines.append(f"- **Week 1**: focus support on schools: **{', '.join(top_targets)}**; assign case owners.")
    lines.append("- **Week 2**: run 2 targeted camps/visits for the selected bottom units; clear top blockers.")
    lines.append("- **Week 3–4**: re-run the tracker; target **+5–10pp** improvement for the bottom units.")
    return "\n".join(lines)


def _insights_provider() -> str:
    """
    Insights generation mode:
    - local (default): deterministic insights using real data + stats/ML-style scoring
    - openai: optional LLM; not used unless explicitly enabled
    """
    return os.environ.get("INSIGHTS_PROVIDER", "local").lower().strip() or "local"


async def _generate_insights(kind: str, payload: Dict[str, Any]) -> str:
    provider = _insights_provider()
    if provider != "openai":
        if kind == "dropout-risk":
            return _local_dropout_insights(payload)
        if kind == "infrastructure-forecast":
            return _local_infra_insights(payload)
        if kind == "teacher-shortage":
            return _local_teacher_insights(payload)
        if kind == "data-completion":
            return _local_completion_insights(payload)
        if kind == "executive-summary":
            # Pass through full payload so the summary can reference rankings/comparators for the selected scope
            return _local_executive_summary(payload)
        return "Insights unavailable for this analysis type."

    # Optional: keep openai as a future provider without forcing it for this project.
    raise HTTPException(status_code=501, detail="INSIGHTS_PROVIDER=openai is not enabled in this build.")


@router.get("/predictions/dropout-risk")
async def get_dropout_risk_predictions(
    current_user: dict = Depends(get_current_user),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """AI-powered dropout risk analysis"""
    scope_match = build_scope_match(
        district_code=district_code,
        block_code=block_code,
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name,
    )
    
    # Gather data for analysis
    dropbox_data = await db.dropbox_analytics.find(scope_match, {"_id": 0}).to_list(5000)
    # enrolment_data currently unused; keep for future modeling but scope it for correctness
    _ = await db.enrolment_analytics.find(scope_match, {"_id": 0}).to_list(1)
    
    level = _scope_level(district_code, block_code, udise_code)
    # Calculate dropout metrics by entity (district->block, block->school, school->school)
    block_metrics = {}
    for d in dropbox_data:
        if level in ("block", "school"):
            key = d.get("udise_code") or d.get("school_name") or "Unknown"
            name = d.get("school_name") or str(key)
        else:
            key = d.get("block_code") or d.get("block_name") or "Unknown"
            name = d.get("block_name") or str(key)

        if key not in block_metrics:
            block_metrics[key] = {"name": name, "dropout": 0, "total_remarks": 0, "migration": 0}
        block_metrics[key]["dropout"] += d.get("dropout", 0)
        block_metrics[key]["total_remarks"] += d.get("total_remarks", 0)
        block_metrics[key]["migration"] += d.get("migration", 0)
    
    # Calculate risk scores
    risk_data = []
    for _, metrics in block_metrics.items():
        dropout_rate = metrics["dropout"] / max(metrics["total_remarks"], 1) * 100
        risk_score = min(100, dropout_rate * 2 + (metrics["migration"] / max(metrics["total_remarks"], 1) * 50))
        risk_data.append({
            # keep key name "block" for frontend compatibility (may represent schools when scoped to a block)
            "block": metrics["name"],
            "dropout_count": metrics["dropout"],
            "dropout_rate": round(dropout_rate, 2),
            "migration_count": metrics["migration"],
            "risk_score": round(risk_score, 1),
            "risk_level": "High" if risk_score > 60 else "Medium" if risk_score > 30 else "Low"
        })
    
    risk_data.sort(key=lambda x: x["risk_score"], reverse=True)
    
    # Get AI insights
    try:
        entity_label = "school" if level == "school" else ("schools" if level == "block" else "blocks")
        ai_insights = await _generate_insights("dropout-risk", {"risk_data": risk_data, "entity_label": entity_label})
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{ai_insights}"
    except Exception as e:
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{_format_ai_exception(e)}"
    
    return {
        "scope": {
            "level": _scope_level(district_code, block_code, udise_code),
            "district_code": district_code,
            "block_code": block_code,
            "udise_code": udise_code,
        },
        "summary": {
            "total_blocks": len(risk_data),
            "high_risk_count": len([r for r in risk_data if r["risk_level"] == "High"]),
            "medium_risk_count": len([r for r in risk_data if r["risk_level"] == "Medium"]),
            "low_risk_count": len([r for r in risk_data if r["risk_level"] == "Low"]),
            "avg_dropout_rate": round(sum(r["dropout_rate"] for r in risk_data) / max(len(risk_data), 1), 2)
        },
        "block_risk_data": risk_data,
        "ai_insights": ai_insights,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@router.get("/predictions/infrastructure-forecast")
async def get_infrastructure_forecast(
    current_user: dict = Depends(get_current_user),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Infrastructure gap analysis and forecast"""
    scope_match = build_scope_match(
        district_code=district_code,
        block_code=block_code,
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name,
    )
    
    # Gather infrastructure data
    ct_data = await db.classrooms_toilets.find(scope_match, {"_id": 0}).to_list(100000)
    
    level = _scope_level(district_code, block_code, udise_code)
    # Aggregate by entity (district->block, block->school, school->school)
    block_infra = {}
    for school in ct_data:
        if level in ("block", "school"):
            key = school.get("udise_code") or school.get("school_name") or "Unknown"
            name = school.get("school_name") or str(key)
        else:
            key = school.get("block_code") or school.get("block_name") or "Unknown"
            name = school.get("block_name") or str(key)

        if key not in block_infra:
            block_infra[key] = {
                "name": name,
                "schools": 0, "classrooms": 0, "good": 0, "minor_repair": 0, "major_repair": 0,
                "toilets": 0, "functional_toilets": 0, "dilapidated": 0
            }
        block_infra[key]["schools"] += 1
        block_infra[key]["classrooms"] += school.get("classrooms_instructional", 0)
        block_infra[key]["good"] += school.get("pucca_good", 0) + school.get("part_pucca_good", 0)
        block_infra[key]["minor_repair"] += school.get("pucca_minor", 0) + school.get("part_pucca_minor", 0)
        block_infra[key]["major_repair"] += school.get("pucca_major", 0) + school.get("part_pucca_major", 0)
        block_infra[key]["toilets"] += school.get("boys_toilets_total", 0) + school.get("girls_toilets_total", 0)
        block_infra[key]["functional_toilets"] += school.get("boys_toilets_functional", 0) + school.get("girls_toilets_functional", 0)
        block_infra[key]["dilapidated"] += school.get("classrooms_dilapidated", 0)
    
    # Calculate forecasts
    forecast_data = []
    for _, data in block_infra.items():
        total_cr = data["classrooms"]
        repair_rate = (data["minor_repair"] + data["major_repair"]) / max(total_cr, 1) * 100
        
        # Forecast: Assume 5% annual degradation
        minor_next_year = int(data["minor_repair"] * 1.2 + data["good"] * 0.05)
        major_next_year = int(data["major_repair"] * 1.1 + data["minor_repair"] * 0.1)
        
        forecast_data.append({
            "block": data["name"],
            "schools": data["schools"],
            "total_classrooms": total_cr,
            "current_repair_needed": data["minor_repair"] + data["major_repair"],
            "repair_rate": round(repair_rate, 1),
            "dilapidated": data["dilapidated"],
            "forecast_minor_repair": minor_next_year,
            "forecast_major_repair": major_next_year,
            "estimated_budget_lakhs": round((minor_next_year * 0.5 + major_next_year * 2) / 100, 1),
            "priority": "High" if repair_rate > 10 or data["dilapidated"] > 5 else "Medium" if repair_rate > 5 else "Low"
        })
    
    forecast_data.sort(key=lambda x: x["repair_rate"], reverse=True)
    
    total_budget = sum(f["estimated_budget_lakhs"] for f in forecast_data)
    
    # Get AI insights
    try:
        entity_label = "school" if level == "school" else ("schools" if level == "block" else "blocks")
        ai_insights = await _generate_insights("infrastructure-forecast", {"forecast_data": forecast_data, "entity_label": entity_label})
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{ai_insights}"
    except Exception as e:
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{_format_ai_exception(e)}"
    
    return {
        "scope": {
            "level": _scope_level(district_code, block_code, udise_code),
            "district_code": district_code,
            "block_code": block_code,
            "udise_code": udise_code,
        },
        "summary": {
            "total_blocks": len(forecast_data),
            "total_classrooms": sum(f["total_classrooms"] for f in forecast_data),
            "current_repair_needed": sum(f["current_repair_needed"] for f in forecast_data),
            "forecast_repair_needed": sum(f["forecast_minor_repair"] + f["forecast_major_repair"] for f in forecast_data),
            "total_dilapidated": sum(f["dilapidated"] for f in forecast_data),
            "estimated_budget_lakhs": round(total_budget, 1),
            "high_priority_blocks": len([f for f in forecast_data if f["priority"] == "High"])
        },
        "block_forecast": forecast_data,
        "ai_insights": ai_insights,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@router.get("/predictions/teacher-shortage")
async def get_teacher_shortage_predictions(
    current_user: dict = Depends(get_current_user),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """Teacher shortage and retirement forecast"""
    scope_match = build_scope_match(
        district_code=district_code,
        block_code=block_code,
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name,
    )
    
    # Gather teacher data (project only what we need)
    ct_data = await db.ctteacher_analytics.find(
        scope_match,
        {"_id": 0, "district_code": 1, "block_code": 1, "udise_code": 1, "block_name": 1, "school_name": 1, "dob": 1, "ctet_qualified": 1},
    ).to_list(200000)

    level = _scope_level(district_code, block_code, udise_code)
    entity_label = "school" if level == "school" else ("schools" if level == "block" else "blocks")
    
    # Aggregate by entity (district->block, block->school, school->school)
    block_teachers = {}
    age_distribution = {"<30": 0, "30-40": 0, "40-50": 0, "50-55": 0, "55+": 0}
    
    for teacher in ct_data:
        unit = (
            (teacher.get("school_name") or teacher.get("udise_code") or "Unknown")
            if level in ("block", "school")
            else (teacher.get("block_name") or "Unknown")
        )
        age = _age_from_dob(teacher.get("dob"))
        if age is None:
            age = 40
        
        if unit not in block_teachers:
            block_teachers[unit] = {"total": 0, "retiring_5yr": 0, "retiring_3yr": 0, "new_entrants": 0, "ctet": 0}
        
        block_teachers[unit]["total"] += 1
        if age >= 55:
            block_teachers[unit]["retiring_5yr"] += 1
            age_distribution["55+"] += 1
        elif age >= 52:
            block_teachers[unit]["retiring_3yr"] += 1
            age_distribution["50-55"] += 1
        elif age >= 40:
            age_distribution["40-50"] += 1
        elif age >= 30:
            age_distribution["30-40"] += 1
        else:
            block_teachers[unit]["new_entrants"] += 1
            age_distribution["<30"] += 1
        
        if teacher.get("ctet_qualified"):
            block_teachers[unit]["ctet"] += 1
    
    # Calculate shortage forecasts
    shortage_data = []
    for block, data in block_teachers.items():
        retiring_pct = data["retiring_5yr"] / max(data["total"], 1) * 100
        shortage_data.append({
            "block": block,
            "total_teachers": data["total"],
            "retiring_in_5_years": data["retiring_5yr"],
            "retiring_in_3_years": data["retiring_3yr"],
            "new_entrants": data["new_entrants"],
            "ctet_qualified": data["ctet"],
            "ctet_rate": round(data["ctet"] / max(data["total"], 1) * 100, 1),
            "retirement_risk_pct": round(retiring_pct, 1),
            "forecast_shortage_5yr": data["retiring_5yr"] - data["new_entrants"],
            "risk_level": "High" if retiring_pct > 20 else "Medium" if retiring_pct > 10 else "Low"
        })
    
    shortage_data.sort(key=lambda x: x["retirement_risk_pct"], reverse=True)
    
    total_retiring = sum(s["retiring_in_5_years"] for s in shortage_data)
    
    # Get AI insights
    try:
        ai_insights = await _generate_insights(
            "teacher-shortage",
            {"shortage_data": shortage_data, "age_distribution": age_distribution, "entity_label": entity_label},
        )
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{ai_insights}"
    except Exception as e:
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{_format_ai_exception(e)}"
    
    return {
        "scope": {
            "level": _scope_level(district_code, block_code, udise_code),
            "district_code": district_code,
            "block_code": block_code,
            "udise_code": udise_code,
        },
        "summary": {
            "total_teachers": sum(s["total_teachers"] for s in shortage_data),
            "retiring_in_5_years": total_retiring,
            "retiring_in_3_years": sum(s["retiring_in_3_years"] for s in shortage_data),
            "new_entrants": sum(s["new_entrants"] for s in shortage_data),
            "net_shortage_5yr": sum(s["forecast_shortage_5yr"] for s in shortage_data),
            "avg_ctet_rate": round(sum(s["ctet_rate"] for s in shortage_data) / max(len(shortage_data), 1), 1),
            "high_risk_blocks": len([s for s in shortage_data if s["risk_level"] == "High"])
        },
        "age_distribution": age_distribution,
        "block_forecast": shortage_data,
        "ai_insights": ai_insights,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@router.get("/predictions/data-completion")
async def get_data_completion_forecast(
    current_user: dict = Depends(get_current_user),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """APAAR/Aadhaar completion timeline prediction"""
    scope_match = build_scope_match(
        district_code=district_code,
        block_code=block_code,
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name,
    )
    
    # Gather data
    apaar_data = await db.apaar_analytics.find(scope_match, {"_id": 0}).to_list(200000)
    # aadhaar_data currently unused; keep for future modeling but scope it for correctness
    _ = await db.aadhaar_analytics.find(scope_match, {"_id": 0}).to_list(1)
    
    level = _scope_level(district_code, block_code, udise_code)
    # Calculate APAAR metrics (district->block, block->school, school->school)
    apaar_metrics = []
    for school in apaar_data:
        if level in ("block", "school"):
            block = school.get("school_name", "Unknown")
        else:
            block = school.get("block_name", "Unknown")  # scope: district/school view uses block_name
        total = school.get("total_student", 0)
        generated = school.get("total_generated", 0)
        rate = generated / max(total, 1) * 100
        
        # Estimate completion based on current rate
        pending = total - generated
        # Assume 2% weekly progress
        weeks_to_complete = int(pending / max(total * 0.02, 1)) if rate < 100 else 0
        
        apaar_metrics.append({
            "school": school.get("school_name", "Unknown")[:40],
            "block": block,
            "total_students": total,
            "generated": generated,
            "generation_rate": round(rate, 1),
            "pending": pending,
            "weeks_to_complete": min(weeks_to_complete, 52)
        })
    
    # Aggregate by block
    block_completion = {}
    for m in apaar_metrics:
        block = m["block"]
        if block not in block_completion:
            block_completion[block] = {"total": 0, "generated": 0, "schools": 0}
        block_completion[block]["total"] += m["total_students"]
        block_completion[block]["generated"] += m["generated"]
        block_completion[block]["schools"] += 1
    
    block_data = []
    for block, data in block_completion.items():
        rate = data["generated"] / max(data["total"], 1) * 100
        pending = data["total"] - data["generated"]
        weeks = int(pending / max(data["total"] * 0.02, 1)) if rate < 100 else 0
        block_data.append({
            "block": block,
            "total_students": data["total"],
            "generated": data["generated"],
            "rate": round(rate, 1),
            "pending": pending,
            "estimated_weeks": min(weeks, 52),
            "completion_date": "Complete" if rate >= 99.9 else f"{weeks} weeks"
        })
    
    block_data.sort(key=lambda x: x["rate"])
    
    total_students = sum(b["total_students"] for b in block_data)
    total_generated = sum(b["generated"] for b in block_data)
    overall_rate = round(total_generated / max(total_students, 1) * 100, 1)
    
    # Get AI insights
    try:
        entity_label = "school" if level == "school" else ("schools" if level == "block" else "blocks")
        ai_insights = await _generate_insights("data-completion", {"block_data": block_data, "overall_rate": overall_rate, "entity_label": entity_label})
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{ai_insights}"
    except Exception as e:
        ai_insights = f"{_scope_prefix_md(district_code, block_code, udise_code)}\n\n{_format_ai_exception(e)}"
    
    return {
        "scope": {
            "level": _scope_level(district_code, block_code, udise_code),
            "district_code": district_code,
            "block_code": block_code,
            "udise_code": udise_code,
        },
        "summary": {
            "total_students": total_students,
            "apaar_generated": total_generated,
            "overall_rate": overall_rate,
            "pending": total_students - total_generated,
            "estimated_weeks_to_100": int((total_students - total_generated) / max(total_students * 0.02, 1)),
            "blocks_above_90": len([b for b in block_data if b["rate"] >= 90]),
            "blocks_below_80": len([b for b in block_data if b["rate"] < 80])
        },
        "block_data": block_data,
        "ai_insights": ai_insights,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@router.get("/insights/executive-summary")
async def get_executive_insights(
    current_user: dict = Depends(get_current_user),
    district_code: Optional[str] = Query(None),
    block_code: Optional[str] = Query(None),
    udise_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
    block_name: Optional[str] = Query(None),
    school_name: Optional[str] = Query(None),
):
    """AI-generated executive summary with insights and recommendations"""
    scope_match = build_scope_match(
        district_code=district_code,
        block_code=block_code,
        udise_code=udise_code,
        district_name=district_name,
        block_name=block_name,
        school_name=school_name,
    )
    
    # Resolve scope names/codes from data (so block-only/school-only calls still know district/block)
    scope_meta = {}
    try:
        sample = await db.classrooms_toilets.find_one(scope_match, {"_id": 0, "district_code": 1, "district_name": 1, "block_code": 1, "block_name": 1, "udise_code": 1, "school_name": 1})
        if not sample:
            sample = await db.apaar_analytics.find_one(scope_match, {"_id": 0, "district_code": 1, "district_name": 1, "block_code": 1, "block_name": 1, "udise_code": 1, "school_name": 1})
        if not sample:
            sample = await db.dropbox_analytics.find_one(scope_match, {"_id": 0, "district_code": 1, "district_name": 1, "block_code": 1, "block_name": 1, "udise_code": 1, "school_name": 1})
        if sample:
            scope_meta = {
                "district_code": sample.get("district_code") or district_code,
                "district_name": sample.get("district_name") or "",
                "block_code": sample.get("block_code") or block_code,
                "block_name": sample.get("block_name") or "",
                "udise_code": sample.get("udise_code") or udise_code,
                "school_name": sample.get("school_name") or "",
            }
    except Exception:
        scope_meta = {"district_code": district_code, "block_code": block_code, "udise_code": udise_code}

    resolved_district_code = scope_meta.get("district_code") or district_code
    resolved_block_code = scope_meta.get("block_code") or block_code
    resolved_udise_code = scope_meta.get("udise_code") or udise_code

    async def _metrics_for(match: Dict[str, Any]) -> Dict[str, Any]:
        # Infrastructure summary (scope-aware)
        ct_pipeline = [
            {
                "$group": {
                    "_id": None,
                    "classrooms": {"$sum": "$classrooms_instructional"},  # indent-fix
                    "good": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},  # indent-fix
                    "toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},  # indent-fix
                    "functional": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
                }
            }
        ]
        ct_result = await db.classrooms_toilets.aggregate(prepend_match(ct_pipeline, match)).to_list(1)
        ct = ct_result[0] if ct_result else {}  # indent-fix
    
        # APAAR summary  # indent-fix
        apaar_pipeline = [
            {"$group": {"_id": None, "students": {"$sum": "$total_student"}, "generated": {"$sum": "$total_generated"}}}
        ]
        apaar_result = await db.apaar_analytics.aggregate(prepend_match(apaar_pipeline, match)).to_list(1)
        apaar = apaar_result[0] if apaar_result else {}  # indent-fix
    
        # Dropbox summary  # indent-fix
        dropbox_pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$total_remarks"}, "dropout": {"$sum": "$dropout"}}}
        ]
        dropbox_result = await db.dropbox_analytics.aggregate(prepend_match(dropbox_pipeline, match)).to_list(1)
        dropbox = dropbox_result[0] if dropbox_result else {}  # indent-fix
    
        # Counts
        ct_count = await db.classrooms_toilets.count_documents(match)
        teacher_count = await db.ctteacher_analytics.count_documents(match)

        return {
            "schools": ct_count,  # indent-fix
            "teachers": teacher_count,  # indent-fix
        "students": apaar.get("students", 0),
        "classrooms": ct.get("classrooms", 0),
        "classroom_health": round(ct.get("good", 0) / max(ct.get("classrooms", 1), 1) * 100, 1),
        "toilets": ct.get("toilets", 0),
        "toilet_functional": round(ct.get("functional", 0) / max(ct.get("toilets", 1), 1) * 100, 1),
        "apaar_rate": round(apaar.get("generated", 0) / max(apaar.get("students", 1), 1) * 100, 1),
            "dropout_rate": round(dropbox.get("dropout", 0) / max(dropbox.get("total", 1), 1) * 100, 2),
        }

    async def _rank_blocks(district_code_in: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        # Build per-block metrics inside a district
        match = build_scope_match(district_code=district_code_in)
        apaar_pipeline = prepend_match([
            {"$group": {"_id": {"block_code": "$block_code", "block_name": "$block_name"},
                        "students": {"$sum": "$total_student"},
                        "generated": {"$sum": "$total_generated"}}},
            {"$project": {"_id": 0,
                          "block_code": "$_id.block_code",
                          "block_name": "$_id.block_name",
                          "apaar_rate": {"$round": [{"$multiply": [{"$divide": ["$generated", {"$max": ["$students", 1]}]}, 100]}, 1]}}}
        ], match)
        apaar_rows = await db.apaar_analytics.aggregate(apaar_pipeline).to_list(500)

        infra_pipeline = prepend_match([
            {"$group": {"_id": {"block_code": "$block_code", "block_name": "$block_name"},
                        "classrooms": {"$sum": "$classrooms_instructional"},
                        "good": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
                        "toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
                        "functional": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}}}},
            {"$project": {"_id": 0,
                          "block_code": "$_id.block_code",
                          "block_name": "$_id.block_name",
                          "classroom_health": {"$round": [{"$multiply": [{"$divide": ["$good", {"$max": ["$classrooms", 1]}]}, 100]}, 1]},
                          "toilet_functional": {"$round": [{"$multiply": [{"$divide": ["$functional", {"$max": ["$toilets", 1]}]}, 100]}, 1]}}}
        ], match)
        infra_rows = await db.classrooms_toilets.aggregate(infra_pipeline).to_list(500)

        dropbox_pipeline = prepend_match([
            {"$group": {"_id": {"block_code": "$block_code", "block_name": "$block_name"},
                        "total": {"$sum": "$total_remarks"},
                        "dropout": {"$sum": "$dropout"}}},
            {"$project": {"_id": 0,
                          "block_code": "$_id.block_code",
                          "block_name": "$_id.block_name",
                          "dropout_rate": {"$round": [{"$multiply": [{"$divide": ["$dropout", {"$max": ["$total", 1]}]}, 100]}, 2]}}}
        ], match)
        drop_rows = await db.dropbox_analytics.aggregate(dropbox_pipeline).to_list(500)

        by_key: Dict[str, Dict[str, Any]] = {}
        for r in apaar_rows:
            key = str(r.get("block_code") or r.get("block_name") or "").strip().upper()
            if not key:
                continue
            by_key[key] = {"name": r.get("block_name") or key, "apaar_rate": r.get("apaar_rate", 0)}
        for r in infra_rows:
            key = str(r.get("block_code") or r.get("block_name") or "").strip().upper()
            if not key:
                continue
            by_key.setdefault(key, {"name": r.get("block_name") or key})
            by_key[key]["classroom_health"] = r.get("classroom_health", 0)
            by_key[key]["toilet_functional"] = r.get("toilet_functional", 0)
        for r in drop_rows:
            key = str(r.get("block_code") or r.get("block_name") or "").strip().upper()
            if not key:
                continue
            by_key.setdefault(key, {"name": r.get("block_name") or key})
            by_key[key]["dropout_rate"] = r.get("dropout_rate", 0)

        items = list(by_key.values())
        def score(x: Dict[str, Any]) -> float:
            return (
                (100 - float(x.get("apaar_rate", 0) or 0))
                + (100 - float(x.get("classroom_health", 0) or 0))
                + (100 - float(x.get("toilet_functional", 0) or 0))
                + (float(x.get("dropout_rate", 0) or 0) * 20.0)
            )
        items.sort(key=score, reverse=True)
        worst = items[:5]
        best = sorted(items, key=score)[:5]
        return worst, best

    async def _rank_schools(block_code_in: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        # Per-school metrics within a block (use APAAR + infra; dropout is often sparse at school-level)
        match = build_scope_match(block_code=block_code_in)
        apaar_pipeline = prepend_match([
            {"$group": {"_id": {"udise_code": "$udise_code", "school_name": "$school_name"},
                        "students": {"$sum": "$total_student"},
                        "generated": {"$sum": "$total_generated"}}},
            {"$project": {"_id": 0,
                          "udise_code": "$_id.udise_code",
                          "school_name": "$_id.school_name",
                          "apaar_rate": {"$round": [{"$multiply": [{"$divide": ["$generated", {"$max": ["$students", 1]}]}, 100]}, 1]}}}
        ], match)
        apaar_rows = await db.apaar_analytics.aggregate(apaar_pipeline).to_list(5000)

        infra_pipeline = prepend_match([
            {"$group": {"_id": {"udise_code": "$udise_code", "school_name": "$school_name"},
                        "classrooms": {"$sum": "$classrooms_instructional"},
                        "good": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
                        "toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
                        "functional": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}}}},
            {"$project": {"_id": 0,
                          "udise_code": "$_id.udise_code",
                          "school_name": "$_id.school_name",
                          "classroom_health": {"$round": [{"$multiply": [{"$divide": ["$good", {"$max": ["$classrooms", 1]}]}, 100]}, 1]},
                          "toilet_functional": {"$round": [{"$multiply": [{"$divide": ["$functional", {"$max": ["$toilets", 1]}]}, 100]}, 1]}}}
        ], match)
        infra_rows = await db.classrooms_toilets.aggregate(infra_pipeline).to_list(5000)

        by_key: Dict[str, Dict[str, Any]] = {}
        for r in apaar_rows:
            key = str(r.get("udise_code") or r.get("school_name") or "").strip().upper()
            if not key:
                continue
            by_key[key] = {"name": r.get("school_name") or key, "apaar_rate": r.get("apaar_rate", 0)}
        for r in infra_rows:
            key = str(r.get("udise_code") or r.get("school_name") or "").strip().upper()
            if not key:
                continue
            by_key.setdefault(key, {"name": r.get("school_name") or key})
            by_key[key]["classroom_health"] = r.get("classroom_health", 0)
            by_key[key]["toilet_functional"] = r.get("toilet_functional", 0)

        items = list(by_key.values())
        def score(x: Dict[str, Any]) -> float:
            return (
                (100 - float(x.get("apaar_rate", 0) or 0))
                + (100 - float(x.get("classroom_health", 0) or 0))
                + (100 - float(x.get("toilet_functional", 0) or 0))
            )
        items.sort(key=score, reverse=True)
        worst = items[:5]
        best = sorted(items, key=score)[:5]
        return worst, best

    # Gather all key metrics
    metrics = await _metrics_for(scope_match)

    # Compute comparators so the output is contextual (school vs block, block vs district)
    comparators: Dict[str, Any] = {}
    if resolved_district_code:
        try:
            comparators["district"] = await _metrics_for(build_scope_match(district_code=resolved_district_code))
        except Exception:
            pass
    if resolved_block_code:
        try:
            comparators["block"] = await _metrics_for(build_scope_match(block_code=resolved_block_code))
        except Exception:
            pass

    # Compute worst/best lists within the selected scope
    worst: Dict[str, Any] = {}
    best: Dict[str, Any] = {}
    level = _scope_level(resolved_district_code, resolved_block_code, resolved_udise_code)
    try:
        if level == "district" and resolved_district_code:
            w, b = await _rank_blocks(resolved_district_code)
            worst["blocks"] = w
            best["blocks"] = b
        if level == "block" and resolved_block_code:
            w, b = await _rank_schools(resolved_block_code)
            worst["schools"] = w
            best["schools"] = b
    except Exception:
        pass
    
    # Get AI executive summary
    try:
        ai_summary = await _generate_insights(
            "executive-summary",
            {
                "metrics": metrics,
                "scope": {
                    "level": level,
                    "district_code": resolved_district_code,
                    "block_code": resolved_block_code,
                    "udise_code": resolved_udise_code,
                },
                "district_name": scope_meta.get("district_name") or "",
                "block_name": scope_meta.get("block_name") or "",
                "school_name": scope_meta.get("school_name") or "",
                "comparators": comparators,
                "worst": worst,
                "best": best,
            },
        )
        ai_summary = f"{_scope_prefix_md(resolved_district_code, resolved_block_code, resolved_udise_code)}\n\n{ai_summary}"
    except Exception as e:
        ai_summary = f"{_scope_prefix_md(resolved_district_code, resolved_block_code, resolved_udise_code)}\n\n{_format_ai_exception(e)}"
    
    return {
        "scope": {
            "level": level,
            "district_code": resolved_district_code,
            "block_code": resolved_block_code,
            "udise_code": resolved_udise_code,
        },
        "metrics": metrics,
        "comparators": comparators,
        "worst": worst,
        "best": best,
        "ai_summary": ai_summary,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


@router.get("/map/block-metrics")
async def get_block_map_metrics(
    district_code: Optional[str] = Query(None),
    district_name: Optional[str] = Query(None),
):
    """Get block-wise metrics for choropleth map"""
    scope_match = build_scope_match(district_code=district_code, district_name=district_name)
    
    # Aggregate metrics by block
    ct_pipeline = [
        {"$group": {
            "_id": "$block_name",
            "block_code": {"$first": "$block_code"},
            "schools": {"$sum": 1},
            "classrooms": {"$sum": "$classrooms_instructional"},
            "good_classrooms": {"$sum": {"$add": ["$pucca_good", "$part_pucca_good"]}},
            "toilets": {"$sum": {"$add": ["$boys_toilets_total", "$girls_toilets_total"]}},
            "functional_toilets": {"$sum": {"$add": ["$boys_toilets_functional", "$girls_toilets_functional"]}},
            "handwash": {"$sum": {"$cond": [{"$eq": ["$handwash_facility", True]}, 1, 0]}}
        }}
    ]
    ct_data = await db.classrooms_toilets.aggregate(prepend_match(ct_pipeline, scope_match)).to_list(200)
    
    # APAAR data
    apaar_pipeline = [
        {"$group": {
            "_id": "$block_name",
            "students": {"$sum": "$total_student"},
            "generated": {"$sum": "$total_generated"}
        }}
    ]
    apaar_data = await db.apaar_analytics.aggregate(prepend_match(apaar_pipeline, scope_match)).to_list(200)
    apaar_map = {a["_id"]: a for a in apaar_data}
    
    # Teacher data
    teacher_pipeline = [
        {"$group": {
            "_id": "$block_name",
            "teachers": {"$sum": 1},
            "ctet": {
                "$sum": {
                    "$cond": [
                        {
                            "$or": [
                                {"$eq": ["$ctet_qualified", True]},
                                {"$gt": ["$ctet_qualified", 0]},
                            ]
                        },
                        1,
                        0,
                    ]
                }
            }
        }}
    ]
    teacher_data = await db.ctteacher_analytics.aggregate(prepend_match(teacher_pipeline, scope_match)).to_list(200)
    teacher_map = {t["_id"]: t for t in teacher_data}
    
    # Combine metrics
    block_metrics = []
    for ct in ct_data:
        block_name = ct["_id"]
        block_code = ct.get("block_code", "")
        apaar = apaar_map.get(block_name, {})
        teacher = teacher_map.get(block_name, {})
        
        classroom_health = round(ct["good_classrooms"] / max(ct["classrooms"], 1) * 100, 1)
        toilet_pct = round(ct["functional_toilets"] / max(ct["toilets"], 1) * 100, 1)
        apaar_rate = round(apaar.get("generated", 0) / max(apaar.get("students", 1), 1) * 100, 1)
        ctet_rate = round(teacher.get("ctet", 0) / max(teacher.get("teachers", 1), 1) * 100, 1)
        
        # Calculate SHI
        shi = round((classroom_health * 0.25 + toilet_pct * 0.25 + apaar_rate * 0.25 + min(ctet_rate * 3, 25)) , 1)
        
        block_metrics.append({
            "block_code": block_code,
            "block_name": block_name,
            "schools": ct["schools"],
            "students": apaar.get("students", 0),
            "teachers": teacher.get("teachers", 0),
            "classroom_health": classroom_health,
            "toilet_functional": toilet_pct,
            "apaar_rate": apaar_rate,
            "teacher_quality": ctet_rate,
            "shi_score": shi,
            "rag_status": "green" if shi >= 75 else "amber" if shi >= 60 else "red"
        })
    
    block_metrics.sort(key=lambda x: x["shi_score"], reverse=True)
    
    return {
        "blocks": block_metrics,
        "total_blocks": len(block_metrics),
        "metric_ranges": {
            "shi": {"min": min(b["shi_score"] for b in block_metrics), "max": max(b["shi_score"] for b in block_metrics)},
            "classroom_health": {"min": min(b["classroom_health"] for b in block_metrics), "max": max(b["classroom_health"] for b in block_metrics)},
            "apaar_rate": {"min": min(b["apaar_rate"] for b in block_metrics), "max": max(b["apaar_rate"] for b in block_metrics)}
        }
    }
