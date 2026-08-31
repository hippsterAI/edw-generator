#!/usr/bin/env python3
"""
Enrollment intelligence dashboard over the generated warehouse.

Reads ONLY the data.* consumption views, plus the control/catalog schemas for
pipeline health. Every statement is a SELECT; nothing is written.

Connection config matches the warehouse loader (load.py): env vars EDW_SERVER,
EDW_DATABASE, EDW_USER, EDW_PASSWORD with the same defaults.

Run:  EDW_PASSWORD=... python app.py    ->  http://localhost:8600

All student data is synthetic. No real people, no real institution.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyodbc
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse

SERVER = os.environ.get("EDW_SERVER", "localhost,1433")
DATABASE = os.environ.get("EDW_DATABASE", "EDW_Dev")
USER = os.environ.get("EDW_USER", "sa")
PASSWORD = os.environ.get("EDW_PASSWORD")

HERE = Path(__file__).resolve().parent
app = FastAPI(title="Enrollment Intelligence")


@contextmanager
def connection():
    driver = next(d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d)
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USER};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes",
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


def _plain(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ", timespec="seconds") if isinstance(v, datetime) else v.isoformat()
    return v


def query(conn, sql: str, params: tuple = ()) -> list[dict]:
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    return [{c: _plain(v) for c, v in zip(cols, row)} for row in cur.fetchall()]


def scalar(conn, sql: str, params: tuple = ()):
    rows = query(conn, sql, params)
    return next(iter(rows[0].values())) if rows else None


# ---------------------------------------------------------------------------
# Tab 1 - Enrollment command center (data.StudentJourney)
# ---------------------------------------------------------------------------

# NOTE: data.StudentJourney can hold multiple rows per applicant (one per
# milestone, and potentially repeats of the same milestone). Before counting,
# deduplicate with ROW_NUMBER partitioned by applicant + intended entry term
# (+ funnel stage) so each person counts once per stage per entry term.
# Non-converting applicants have no StudentCode, so the partition key is the
# ApplicantCode, which every journey row carries.
# Post-enrollment milestones (stopped out / completed / graduated) also carry
# ApplicationState 'ENRL' but can land in later milestone terms, so the
# Registered stage additionally requires the registration milestone itself
# (StudentStatus 'ENRL'); otherwise one person would count as registered in
# several terms.
DEDUP_JOURNEY = """
WITH dedup AS (
    SELECT ApplicantCode, MilestoneTermCode, ApplicationStateCode, StudentStatusCode,
           rn = ROW_NUMBER() OVER (
               PARTITION BY ApplicantCode, MilestoneTermCode, ApplicationStateCode
               ORDER BY MilestoneDate)
    FROM data.StudentJourney
    WHERE ApplicationStateCode <> 'ENRL' OR StudentStatusCode = 'ENRL'
)
"""

# Funnel stages come from the ApplicationState dimension:
# applications started -> completed -> admitted -> registered.
FUNNEL_STAGES = [
    ("APPL", "Applications started"),
    ("COMP", "Applications completed"),
    ("ADMT", "Admitted"),
    ("ENRL", "Registered"),
]


@app.get("/api/terms")
def api_terms():
    with connection() as conn:
        rows = query(conn, """
            SELECT DISTINCT MilestoneTermCode, MilestoneTermName,
                   MilestoneTermStartDate
            FROM data.StudentJourney
            ORDER BY MilestoneTermStartDate DESC""")
    return {"terms": rows}


@app.get("/api/funnel")
def api_funnel(term: str = "ALL"):
    with connection() as conn:
        rows = query(conn, DEDUP_JOURNEY + """
            SELECT ApplicationStateCode, ApplicantCount = COUNT(*)
            FROM dedup
            WHERE rn = 1
              AND ApplicationStateCode IN ('APPL','COMP','ADMT','ENRL')
              AND (? = 'ALL' OR MilestoneTermCode = ?)
            GROUP BY ApplicationStateCode""", (term, term))
    by_code = {r["ApplicationStateCode"]: r["ApplicantCount"] for r in rows}
    stages, prev = [], None
    for code, label in FUNNEL_STAGES:
        n = by_code.get(code, 0)
        falloff = None if prev in (None, 0) else round(100.0 * (prev - n) / prev, 1)
        stages.append({"code": code, "label": label, "count": n, "falloff_pct": falloff})
        prev = n
    return {"term": term, "stages": stages}


@app.get("/api/yoy")
def api_yoy():
    """Registered (ENRL) student counts for the three most recent equivalent
    terms of each term type: Fall vs Fall, Spring vs Spring, Summer vs Summer."""
    with connection() as conn:
        rows = query(conn, DEDUP_JOURNEY + """
            , counts AS (
                SELECT j.MilestoneTermCode,
                       Registered = COUNT(*)
                FROM dedup j
                WHERE j.rn = 1 AND j.ApplicationStateCode = 'ENRL'
                GROUP BY j.MilestoneTermCode
            ), terms AS (
                SELECT DISTINCT MilestoneTermCode, MilestoneTermTypeCode,
                       MilestoneTermTypeName, MilestoneTermStartDate
                FROM data.StudentJourney
            ), ranked AS (
                SELECT t.MilestoneTermCode, t.MilestoneTermTypeCode,
                       t.MilestoneTermTypeName, t.MilestoneTermStartDate,
                       c.Registered,
                       rk = ROW_NUMBER() OVER (
                           PARTITION BY t.MilestoneTermTypeCode
                           ORDER BY t.MilestoneTermStartDate DESC)
                FROM terms t
                JOIN counts c ON c.MilestoneTermCode = t.MilestoneTermCode
            )
            SELECT MilestoneTermCode, MilestoneTermTypeCode, MilestoneTermTypeName,
                   Registered
            FROM ranked WHERE rk <= 3
            ORDER BY MilestoneTermTypeCode, MilestoneTermStartDate""")
    groups: dict[str, dict] = {}
    for r in rows:
        g = groups.setdefault(r["MilestoneTermTypeCode"],
                              {"type_name": r["MilestoneTermTypeName"], "terms": []})
        g["terms"].append({"term": r["MilestoneTermCode"], "registered": r["Registered"]})
    return {"groups": groups}


# ---------------------------------------------------------------------------
# Tab 2 - Students at risk (data.StudentHistory / data.StudentClass grain,
# with data.FinancialAid and data.Advisement for the aid/advising flags)
# ---------------------------------------------------------------------------

# The current term is in progress: no grades are posted yet (term GPA is NULL,
# earned hours are 0 for everyone). Academic-performance flags are therefore
# evaluated on each student's most recent GRADED term; the aid and advising
# flags use the current term itself. The UI labels both terms.
AT_RISK_SQL = """
WITH cur AS (
    SELECT StudentCode, ProgramCode, ProgramName, AcademicStandingName
    FROM data.StudentHistory
    WHERE TermCurrentTermFlag = 'Y' AND StudentStatusEnrolledFlag = 'Y'
), graded AS (
    SELECT StudentCode, TermCode, TermGPAValue,
           CreditHoursAttempted, CreditHoursEarned,
           rn = ROW_NUMBER() OVER (PARTITION BY StudentCode
                                   ORDER BY TermStartDate DESC)
    FROM data.StudentHistory
    WHERE TermGPAValue IS NOT NULL
), aid AS (
    SELECT StudentCode,
           Offered  = SUM(OfferedAmount),
           Accepted = SUM(ISNULL(AcceptedAmount, 0))
    FROM data.FinancialAid
    WHERE TermCurrentTermFlag = 'Y'
    GROUP BY StudentCode
), adv AS (
    SELECT DISTINCT StudentCode
    FROM data.Advisement
    WHERE TermCurrentTermFlag = 'Y'
)
SELECT cur.StudentCode, cur.ProgramCode, cur.ProgramName, cur.AcademicStandingName,
       GradedTermCode  = g1.TermCode,
       TermGPAValue    = g1.TermGPAValue,
       Attempted       = g1.CreditHoursAttempted,
       Earned          = g1.CreditHoursEarned,
       PriorTermCode   = g2.TermCode,
       PriorAttempted  = g2.CreditHoursAttempted,
       PriorEarned     = g2.CreditHoursEarned,
       AidOffered      = aid.Offered,
       AidAccepted     = aid.Accepted,
       HasAdvising     = CASE WHEN adv.StudentCode IS NULL THEN 0 ELSE 1 END
FROM cur
LEFT JOIN graded g1 ON g1.StudentCode = cur.StudentCode AND g1.rn = 1
LEFT JOIN graded g2 ON g2.StudentCode = cur.StudentCode AND g2.rn = 2
LEFT JOIN aid ON aid.StudentCode = cur.StudentCode
LEFT JOIN adv ON adv.StudentCode = cur.StudentCode
"""


def _ratio(earned, attempted):
    if attempted and attempted > 0:
        return float(earned or 0) / float(attempted)
    return None


@app.get("/api/at-risk")
def api_at_risk():
    with connection() as conn:
        current_term = scalar(conn, """
            SELECT MAX(TermCode) FROM data.StudentHistory
            WHERE TermCurrentTermFlag = 'Y'""")
        rows = query(conn, AT_RISK_SQL)

    flagged = []
    flag_totals = {"low_gpa": 0, "low_completion": 0, "completion_drop": 0,
                   "aid_unaccepted": 0, "no_advising": 0}
    for r in rows:
        reasons = []
        gpa = r["TermGPAValue"]
        gterm = r["GradedTermCode"]
        ratio = _ratio(r["Earned"], r["Attempted"])
        prior_ratio = _ratio(r["PriorEarned"], r["PriorAttempted"])

        if gpa is not None and gpa < 2.0:
            flag_totals["low_gpa"] += 1
            reasons.append(f"Term GPA {gpa:.2f} in {gterm} (below 2.0)")
        if ratio is not None and ratio < 0.5:
            flag_totals["low_completion"] += 1
            reasons.append(f"Completed {r['Earned']:.0f} of {r['Attempted']:.0f} "
                           f"credits in {gterm} ({ratio:.0%}, below 50%)")
        if ratio is not None and prior_ratio is not None and prior_ratio - ratio > 0.3:
            flag_totals["completion_drop"] += 1
            reasons.append(f"Completion ratio fell from {prior_ratio:.0%} in "
                           f"{r['PriorTermCode']} to {ratio:.0%} in {gterm}")
        if r["AidOffered"] is not None and r["AidOffered"] > 0 and (r["AidAccepted"] or 0) == 0:
            flag_totals["aid_unaccepted"] += 1
            reasons.append("Financial aid offered but not accepted this term")
        if not r["HasAdvising"]:
            flag_totals["no_advising"] += 1
            reasons.append("No advising contact this term")

        if reasons:
            flagged.append({
                "student": r["StudentCode"],
                "program": f"{r['ProgramCode']} {r['ProgramName']}",
                "graded_term": gterm,
                "term_gpa": gpa,
                "attempted": r["Attempted"],
                "earned": r["Earned"],
                "standing": r["AcademicStandingName"],
                "flag_count": len(reasons),
                "reasons": reasons,
            })

    flagged.sort(key=lambda s: (-s["flag_count"],
                                s["term_gpa"] if s["term_gpa"] is not None else 9.9))
    return {"current_term": current_term, "enrolled_count": len(rows),
            "flagged_count": len(flagged), "flag_totals": flag_totals,
            "students": flagged}


# ---------------------------------------------------------------------------
# Tab 3 - Program outcomes (data.Award, data.StudentHistory)
# ---------------------------------------------------------------------------

@app.get("/api/outcomes")
def api_outcomes():
    with connection() as conn:
        # Most recent COMPLETE academic year = latest year before the year the
        # current-flagged term belongs to.
        current_ay = scalar(conn, """
            SELECT MAX(TermAcademicYear) FROM data.StudentHistory
            WHERE TermCurrentTermFlag = 'Y'""")
        if current_ay is not None:
            ay = scalar(conn, """
                SELECT MAX(TermAcademicYear) FROM data.StudentHistory
                WHERE TermAcademicYear < ?""", (current_ay,))
        else:
            ay = scalar(conn, "SELECT MAX(TermAcademicYear) FROM data.StudentHistory")
        if ay is None:
            return {"academic_year": None, "programs": []}

        awards = query(conn, """
            SELECT ProgramCode, ProgramName, Awards = SUM(AwardCount)
            FROM data.Award
            WHERE AwardTermAcademicYear = ?
            GROUP BY ProgramCode, ProgramName""", (ay,))

        # Fall-to-fall retention: enrolled in the fall term of that academic
        # year and enrolled again (any program) in the following fall.
        fall_terms = query(conn, """
            SELECT DISTINCT TermAcademicYear, TermCode
            FROM data.StudentHistory
            WHERE TermTypeCode = 'FA' AND TermAcademicYear IN (?, ?)""",
            (ay, ay + 1))
        fall = {r["TermAcademicYear"]: r["TermCode"] for r in fall_terms}

        retention = query(conn, """
            WITH cohort AS (
                SELECT DISTINCT StudentCode, ProgramCode, ProgramName
                FROM data.StudentHistory
                WHERE TermTypeCode = 'FA' AND TermAcademicYear = ?
                  AND StudentStatusEnrolledFlag = 'Y'
            ), nextfall AS (
                SELECT DISTINCT StudentCode
                FROM data.StudentHistory
                WHERE TermTypeCode = 'FA' AND TermAcademicYear = ?
                  AND StudentStatusEnrolledFlag = 'Y'
            )
            SELECT c.ProgramCode, c.ProgramName,
                   Cohort   = COUNT(*),
                   Retained = SUM(CASE WHEN n.StudentCode IS NULL THEN 0 ELSE 1 END)
            FROM cohort c
            LEFT JOIN nextfall n ON n.StudentCode = c.StudentCode
            GROUP BY c.ProgramCode, c.ProgramName""", (ay, ay + 1))

    def program_label(r):
        # the warehouse's unknown-member row renders as "Unknown Unknown"
        if r["ProgramCode"] == "Unknown":
            return "No program recorded"
        return f"{r['ProgramCode']} {r['ProgramName']}"

    programs: dict[str, dict] = {}
    for r in awards:
        programs[r["ProgramCode"]] = {"program": program_label(r),
                                      "awards": r["Awards"], "cohort": 0, "retained": 0}
    for r in retention:
        p = programs.setdefault(r["ProgramCode"],
                                {"program": program_label(r),
                                 "awards": 0, "cohort": 0, "retained": 0})
        p["cohort"], p["retained"] = r["Cohort"], r["Retained"]
    out = sorted(programs.values(), key=lambda p: -p["awards"])
    for p in out:
        p["retention_pct"] = round(100.0 * p["retained"] / p["cohort"], 1) if p["cohort"] else None
    return {"academic_year": ay, "cohort_fall": fall.get(ay),
            "next_fall": fall.get(ay + 1), "programs": out}


# ---------------------------------------------------------------------------
# Tab 5 - Cross-system risk (data.StudentRisk: SIS + LMS + CRM + aid + ITSM)
# ---------------------------------------------------------------------------

# which source system stands behind each flag, for the per-student indicator
FLAG_SOURCES = {
    "SIS":  ("FlagLowGPA", "FlagLowCompletion", "FlagCompletionDrop"),
    "LMS":  ("FlagInactiveLMS", "FlagDecliningLMS", "FlagLateSubmissions"),
    "CRM":  ("FlagNoAdvisorContact", "FlagAdvisorMissed"),
    "AID":  ("FlagAidUnaccepted",),
    "ITSM": ("FlagOpenFinancialCase", "FlagOpenOtherCase"),
}

# data.StudentRisk aggregates the whole LMS fact behind window functions; a
# pushed-down filter makes the optimizer re-aggregate it per student (minutes).
# FORCE ORDER keeps the written join order and the query returns in ms.
CROSS_RISK_SQL = """
SELECT StudentCode, ProgramCode, ProgramName, AcademicStandingName,
       RiskScore, RiskLevel, RiskReasons,
       EffectiveGPA, DaysSinceLastActivity, ActivityTrendPct,
       AdvisorContacts, AdvisorMissed, OpenFinancialCases,
       FlagLowGPA, FlagLowCompletion, FlagCompletionDrop,
       FlagInactiveLMS, FlagDecliningLMS, FlagLateSubmissions,
       FlagNoAdvisorContact, FlagAdvisorMissed, FlagAidUnaccepted,
       FlagOpenFinancialCase, FlagOpenOtherCase
FROM data.StudentRisk
WHERE TermCurrentTermFlag = 'Y'
ORDER BY RiskScore DESC, StudentCode
OPTION (FORCE ORDER)
"""


@app.get("/api/cross-risk")
def api_cross_risk():
    with connection() as conn:
        current_term = scalar(conn, """
            SELECT MAX(TermCode) FROM data.StudentHistory
            WHERE TermCurrentTermFlag = 'Y'""")
        rows = query(conn, CROSS_RISK_SQL)
    levels = {"High": 0, "Moderate": 0, "Low": 0}
    for r in rows:
        levels[r["RiskLevel"]] = levels.get(r["RiskLevel"], 0) + 1
    students = []
    for r in rows[:150]:
        if r["RiskScore"] <= 0:
            break
        sources = [name for name, flags in FLAG_SOURCES.items() if any(r[f] for f in flags)]
        students.append({
            "student": r["StudentCode"],
            "program": f"{r['ProgramCode']} {r['ProgramName']}",
            "standing": r["AcademicStandingName"],
            "score": r["RiskScore"], "level": r["RiskLevel"],
            "reasons": r["RiskReasons"], "sources": sources,
        })
    return {"current_term": current_term, "scored_count": len(rows),
            "levels": levels, "students": students}


@app.get("/api/cross-risk-signals")
def api_cross_risk_signals():
    return json.loads((HERE / "weights.json").read_text())


# ---------------------------------------------------------------------------
# Tab 4 - Pipeline health (control.ProcessLog, catalog.Facts)
# ---------------------------------------------------------------------------

ORCHESTRATORS = ("Load_Warehouse", "Load_Dimensions", "Load_Facts")


@app.get("/api/pipeline")
def api_pipeline():
    with connection() as conn:
        last_runs = query(conn, """
            WITH runs AS (
                SELECT ProcessName, StatusCode, StartDateTime, EndDateTime,
                       DurationMs = DATEDIFF(ms, StartDateTime, EndDateTime),
                       RowsInserted, ErrorMessage,
                       AgeHours = DATEDIFF(minute, ISNULL(EndDateTime, StartDateTime),
                                           SYSDATETIME()) / 60.0,
                       rn = ROW_NUMBER() OVER (PARTITION BY ProcessName
                                               ORDER BY StartDateTime DESC)
                FROM control.ProcessLog
                WHERE ProcessName IN (?, ?, ?)
            )
            SELECT ProcessName, StatusCode, StartDateTime, EndDateTime,
                   DurationMs, RowsInserted, ErrorMessage, AgeHours
            FROM runs WHERE rn = 1""", ORCHESTRATORS)

        last_success = query(conn, """
            WITH runs AS (
                SELECT ProcessName, EndDateTime,
                       AgeHours = DATEDIFF(minute, EndDateTime, SYSDATETIME()) / 60.0,
                       rn = ROW_NUMBER() OVER (PARTITION BY ProcessName
                                               ORDER BY StartDateTime DESC)
                FROM control.ProcessLog
                WHERE ProcessName IN (?, ?, ?) AND StatusCode = 'Success'
            )
            SELECT ProcessName, EndDateTime, AgeHours FROM runs WHERE rn = 1""",
            ORCHESTRATORS)
        success_by_name = {r["ProcessName"]: r for r in last_success}

        problems = query(conn, """
            SELECT TOP 20 ProcessLogKey, ProcessName, ObjectName, StatusCode,
                   StartDateTime, EndDateTime, ErrorMessage = LEFT(ErrorMessage, 300)
            FROM control.ProcessLog
            WHERE StatusCode = 'Failure'
               OR (StatusCode = 'Running' AND EndDateTime IS NULL)
            ORDER BY StartDateTime DESC""")

        fact_counts = query(conn, """
            SELECT FactName = f.FactName,
                   TableName = f.SchemaName + '.' + f.TableName,
                   TableRowCount = SUM(p.rows)
            FROM catalog.Facts f
            JOIN sys.tables t ON t.name = f.TableName
                             AND SCHEMA_NAME(t.schema_id) = f.SchemaName
            JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
            WHERE f.EnabledFlag = 'Y'
            GROUP BY f.FactName, f.SchemaName, f.TableName
            ORDER BY f.FactName""")

    orchestrators = []
    for name in ORCHESTRATORS:
        run = next((r for r in last_runs if r["ProcessName"] == name), None)
        succ = success_by_name.get(name)
        if run is None:
            health = "red"
        elif run["StatusCode"] == "Failure" or (run["StatusCode"] == "Running"
                                                and run["EndDateTime"] is None
                                                and run["AgeHours"] > 1):
            health = "red"
        elif run["StatusCode"] == "Success" and run["AgeHours"] <= 24:
            health = "green"
        else:
            health = "amber"
        orchestrators.append({"name": name, "health": health, "last_run": run,
                              "last_success": succ})
    return {"orchestrators": orchestrators, "problem_runs": problems,
            "fact_counts": fact_counts}


@app.get("/")
def index():
    return FileResponse(HERE / "index.html")


if __name__ == "__main__":
    if not PASSWORD:
        raise SystemExit("password required: set EDW_PASSWORD (same as load.py)")
    uvicorn.run(app, host="127.0.0.1", port=8600)
