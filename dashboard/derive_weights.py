#!/usr/bin/env python3
"""
derive_weights.py -- derive the data.StudentRisk scoring weights from the
seeded warehouse instead of picking them by hand.

Method
  1. Build one row per enrolled student per term with eleven binary risk
     flags, computed across SIS, LMS, CRM, financial aid, and ITSM sources
     (the same SQL the view uses -- SIGNALS_SQL below is the single source
     of truth and is embedded verbatim into the generated view).
  2. Label each historical row: "non-return" = the student never enrolled in
     any later term. Current-term rows are unlabeled and excluded; students
     are excluded from their award term onward (graduating is not attrition).
  3. For each flag compute P(non-return | flag) vs P(non-return | no flag)
     and the odds ratio. Weight = round(4 * ln(odds ratio)), floored at 0.
     A signal that never fires or has no lift gets weight 0.
  4. Score every labeled row with those weights, print the observed
     non-return rate by score band, and pick the High/Moderate thresholds
     as the lowest scores whose bands exceed 60% / 30% non-return.

Outputs
  dashboard/weights.json            -- derivation table for the dashboard
  dashboard/sql/data_StudentRisk.sql -- CREATE OR ALTER VIEW with weights baked in

Run:  EDW_PASSWORD=... python3 derive_weights.py
Then deploy the view with sqlcmd -i sql/data_StudentRisk.sql.
"""
from __future__ import annotations

import json
import math
import os
from datetime import date
from pathlib import Path

import pyodbc

HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# The canonical cross-source signal SQL. Everything reads data.* views only.
# ---------------------------------------------------------------------------
SIGNALS_SQL = """
WITH hist AS (
    SELECT StudentCode, StudentName, TermCode, ProgramCode, ProgramName,
           TermStartDate, TermEndDate, TermCurrentTermFlag, AcademicStandingName,
           TermGPAValue, CreditHoursAttempted, CreditHoursEarned,
           -- a term with no posted grades (TermGPAValue NULL, e.g. the term in
           -- progress) has no completion evidence yet: NULL, not zero
           CompletionRatio = CASE WHEN CreditHoursAttempted > 0 AND TermGPAValue IS NOT NULL
                                  THEN CreditHoursEarned / CreditHoursAttempted END
    FROM data.StudentHistory
    WHERE StudentStatusEnrolledFlag = 'Y'
), acad AS (
    SELECT *,
           PriorTermGPAValue    = LAG(TermGPAValue)    OVER (PARTITION BY StudentCode ORDER BY TermStartDate),
           PriorCompletionRatio = LAG(CompletionRatio) OVER (PARTITION BY StudentCode ORDER BY TermStartDate)
    FROM hist
), week_totals AS (      -- LMS: collapse classes to one row per student-term-week
    SELECT StudentCode, TermCode, WeekStartDate,
           Acts   = SUM(LoginCount + SubmissionCount),
           Subs   = SUM(SubmissionCount),
           OnTime = SUM(OnTimeSubmissionCount)
    FROM data.LearningActivity
    GROUP BY StudentCode, TermCode, WeekStartDate
), term_weeks AS (       -- week index within each term, from the LMS calendar itself
    SELECT TermCode, WeekStartDate,
           WeekNumber = DENSE_RANK() OVER (PARTITION BY TermCode ORDER BY WeekStartDate),
           WeekCount  = COUNT(*)    OVER (PARTITION BY TermCode)
    FROM (SELECT DISTINCT TermCode, WeekStartDate FROM data.LearningActivity) w
), term_asof AS (        -- "now" for a term = the last seeded LMS week
    SELECT TermCode, AsOfDate = DATEADD(day, 6, MAX(WeekStartDate))
    FROM term_weeks GROUP BY TermCode
), lms AS (
    SELECT w.StudentCode, w.TermCode,
           LastActivityDate = MAX(CASE WHEN w.Acts > 0 THEN DATEADD(day, 6, w.WeekStartDate) END),
           EarlyAvg = AVG(CASE WHEN t.WeekNumber <= t.WeekCount / 2.0 THEN 1.0 * w.Acts END),
           LateAvg  = AVG(CASE WHEN t.WeekNumber >  t.WeekCount / 2.0 THEN 1.0 * w.Acts END),
           Subs = SUM(w.Subs), OnTime = SUM(w.OnTime)
    FROM week_totals w
    JOIN term_weeks t ON t.TermCode = w.TermCode AND t.WeekStartDate = w.WeekStartDate
    GROUP BY w.StudentCode, w.TermCode
), adv AS (
    SELECT StudentCode, TermCode,
           Contacts = SUM(ContactCount), Kept = SUM(KeptCount), Missed = SUM(MissedCount),
           LastContactDate = MAX(ContactDate)
    FROM data.AdvisorContact
    GROUP BY StudentCode, TermCode
), aid AS (
    SELECT StudentCode, TermCode,
           Offered   = SUM(OfferedAmount),
           Accepted  = SUM(ISNULL(AcceptedAmount, 0)),
           Disbursed = SUM(ISNULL(DisbursedAmount, 0))
    FROM data.FinancialAid
    GROUP BY StudentCode, TermCode
), cases AS (
    SELECT StudentCode, TermCode,
           OpenFinancialCases = SUM(CASE WHEN CaseProfileOpenFlag = 'Y' AND CaseProfileCategoryCode = 'FIN'  THEN CaseCount ELSE 0 END),
           OpenFinancialDays  = MAX(CASE WHEN CaseProfileOpenFlag = 'Y' AND CaseProfileCategoryCode = 'FIN'  THEN DaysOpen END),
           OpenTechnicalCases = SUM(CASE WHEN CaseProfileOpenFlag = 'Y' AND CaseProfileCategoryCode = 'TECH' THEN CaseCount ELSE 0 END),
           OpenPersonalCases  = SUM(CASE WHEN CaseProfileOpenFlag = 'Y' AND CaseProfileCategoryCode = 'PERS' THEN CaseCount ELSE 0 END),
           OpenAcademicCases  = SUM(CASE WHEN CaseProfileOpenFlag = 'Y' AND CaseProfileCategoryCode = 'ACAD' THEN CaseCount ELSE 0 END)
    FROM data.SupportCase
    GROUP BY StudentCode, TermCode
), signals AS (
    SELECT a.StudentCode, a.StudentName, a.TermCode, a.TermStartDate, a.TermCurrentTermFlag,
           a.ProgramCode, a.ProgramName, a.AcademicStandingName,
           a.TermGPAValue, a.PriorTermGPAValue,
           EffectiveGPA             = COALESCE(a.TermGPAValue, a.PriorTermGPAValue),
           a.CreditHoursAttempted, a.CreditHoursEarned,
           a.CompletionRatio, a.PriorCompletionRatio,
           EffectiveCompletionRatio = COALESCE(a.CompletionRatio, a.PriorCompletionRatio),
           CompletionChange         = a.CompletionRatio - a.PriorCompletionRatio,
           DaysSinceLastActivity = DATEDIFF(day, l.LastActivityDate, ISNULL(ta.AsOfDate, a.TermEndDate)),
           ActivityTrendPct = CASE WHEN l.EarlyAvg > 0
                                   THEN ROUND(100.0 * ISNULL(l.LateAvg, 0) / l.EarlyAvg, 0) END,
           SubmissionCount = ISNULL(l.Subs, 0),
           OnTimeSharePct  = CASE WHEN l.Subs > 0 THEN ROUND(100.0 * l.OnTime / l.Subs, 0) END,
           AdvisorContacts = ISNULL(v.Contacts, 0),
           AdvisorKept     = ISNULL(v.Kept, 0),
           AdvisorMissed   = ISNULL(v.Missed, 0),
           DaysSinceLastContact = DATEDIFF(day, v.LastContactDate, ISNULL(ta.AsOfDate, a.TermEndDate)),
           AidOffered   = ISNULL(f.Offered, 0),
           AidAccepted  = ISNULL(f.Accepted, 0),
           AidDisbursed = ISNULL(f.Disbursed, 0),
           OpenFinancialCases = ISNULL(c.OpenFinancialCases, 0),
           OpenFinancialDays  = c.OpenFinancialDays,
           OpenTechnicalCases = ISNULL(c.OpenTechnicalCases, 0),
           OpenPersonalCases  = ISNULL(c.OpenPersonalCases, 0),
           OpenAcademicCases  = ISNULL(c.OpenAcademicCases, 0),
           FlagLowGPA            = CASE WHEN COALESCE(a.TermGPAValue, a.PriorTermGPAValue) < 2.0 THEN 1 ELSE 0 END,
           FlagLowCompletion     = CASE WHEN COALESCE(a.CompletionRatio, a.PriorCompletionRatio) < 0.5 THEN 1 ELSE 0 END,
           FlagCompletionDrop    = CASE WHEN a.PriorCompletionRatio - a.CompletionRatio > 0.3 THEN 1 ELSE 0 END,
           FlagInactiveLMS       = CASE WHEN l.LastActivityDate IS NULL
                                          OR DATEDIFF(day, l.LastActivityDate, ISNULL(ta.AsOfDate, a.TermEndDate)) >= 14 THEN 1 ELSE 0 END,
           FlagDecliningLMS      = CASE WHEN l.EarlyAvg > 0 AND ISNULL(l.LateAvg, 0) < 0.5 * l.EarlyAvg THEN 1 ELSE 0 END,
           FlagLateSubmissions   = CASE WHEN l.Subs > 0 AND 1.0 * l.OnTime / l.Subs < 0.6 THEN 1 ELSE 0 END,
           FlagNoAdvisorContact  = CASE WHEN ISNULL(v.Contacts, 0) = 0 THEN 1 ELSE 0 END,
           FlagAdvisorMissed     = CASE WHEN ISNULL(v.Missed, 0) > 0 AND ISNULL(v.Kept, 0) = 0 THEN 1 ELSE 0 END,
           FlagAidUnaccepted     = CASE WHEN ISNULL(f.Offered, 0) > 0 AND ISNULL(f.Accepted, 0) = 0 THEN 1 ELSE 0 END,
           FlagOpenFinancialCase = CASE WHEN ISNULL(c.OpenFinancialCases, 0) > 0 THEN 1 ELSE 0 END,
           FlagOpenOtherCase     = CASE WHEN ISNULL(c.OpenTechnicalCases, 0) + ISNULL(c.OpenPersonalCases, 0)
                                           + ISNULL(c.OpenAcademicCases, 0) > 0 THEN 1 ELSE 0 END
    FROM acad a
    LEFT JOIN term_asof ta ON ta.TermCode = a.TermCode
    LEFT JOIN lms   l ON l.StudentCode = a.StudentCode AND l.TermCode = a.TermCode
    LEFT JOIN adv   v ON v.StudentCode = a.StudentCode AND v.TermCode = a.TermCode
    LEFT JOIN aid   f ON f.StudentCode = a.StudentCode AND f.TermCode = a.TermCode
    LEFT JOIN cases c ON c.StudentCode = a.StudentCode AND c.TermCode = a.TermCode
)
"""

# flag -> (short label, contributing source system, reason SQL expression)
FLAGS = {
    "FlagLowGPA":            ("Term GPA below 2.0", "SIS",
                              "CONCAT('term GPA ', FORMAT(s.EffectiveGPA, '0.00'), ' below 2.0')"),
    "FlagLowCompletion":     ("Completion ratio below 50%", "SIS",
                              "CONCAT('completed ', CAST(ROUND(100 * s.EffectiveCompletionRatio, 0) AS int), '% of attempted credits')"),
    "FlagCompletionDrop":    ("Completion ratio dropped >30 points", "SIS",
                              "CONCAT('completion ratio fell ', CAST(ROUND(-100 * s.CompletionChange, 0) AS int), ' points from prior term')"),
    "FlagInactiveLMS":       ("No LMS activity for 14+ days", "LMS",
                              "CONCAT('no LMS activity for ', s.DaysSinceLastActivity, ' days')"),
    "FlagDecliningLMS":      ("LMS activity fell below half of early-term level", "LMS",
                              "CONCAT('LMS activity down to ', CAST(s.ActivityTrendPct AS int), '% of early-term level')"),
    "FlagLateSubmissions":   ("Under 60% of submissions on time", "LMS",
                              "CONCAT('only ', CAST(s.OnTimeSharePct AS int), '% of submissions on time')"),
    "FlagNoAdvisorContact":  ("No advisor contact this term", "CRM",
                              "'no advisor contact this term'"),
    "FlagAdvisorMissed":     ("Advisor contact attempted, none kept", "CRM",
                              "CONCAT(s.AdvisorMissed, ' advisor contact(s) missed, none kept')"),
    "FlagAidUnaccepted":     ("Aid offered but not accepted", "FinancialAid",
                              "'financial aid offered but not accepted'"),
    "FlagOpenFinancialCase": ("Unresolved financial support case", "ITSM",
                              "CONCAT('financial support case unresolved for ', s.OpenFinancialDays, ' days')"),
    "FlagOpenOtherCase":     ("Other support case open", "ITSM",
                              "CONCAT(s.OpenTechnicalCases + s.OpenPersonalCases + s.OpenAcademicCases, ' non-financial support case(s) open')"),
}

VIEW_TEMPLATE = """-- ---------------------------------------------------------------------------
-- data.StudentRisk: one row per enrolled student per term, combining SIS,
-- LMS, CRM, financial aid, and ITSM signals into a composite risk score.
--
-- GENERATED by dashboard/derive_weights.py on {today} -- do not edit weights
-- by hand; re-run the script after reseeding. Weights are log-odds-ratio
-- based, derived from which signals actually preceded non-return in this
-- warehouse's data:
{weight_comment}
-- Score = sum of active flag weights, scaled to 0-100 by the total weight.
-- Levels (from observed non-return rates by score band):
--   High >= {high_t}, Moderate >= {mod_t}.
--
-- The reason string is assembled with scalar expressions only (CONCAT_WS in
-- descending-weight order, truncated after the third item): APPLY operators
-- here sent the optimizer into a minutes-long plan, while this form runs at
-- the speed of the signals query itself.
--
-- PERFORMANCE: append OPTION (FORCE ORDER) to any query that filters this
-- view (e.g. WHERE TermCurrentTermFlag = 'Y'). Without it the optimizer
-- pushes the predicate into a nested-loops re-aggregation of the LMS fact
-- and the query runs for minutes; with it, milliseconds.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW data.StudentRisk
AS
{signals_sql}
, scored AS (
    SELECT s.*,
           RiskScore = CAST(ROUND(100.0 * (
{score_terms}
           ) / {total_weight}, 0) AS int),
           FullReasons = CONCAT_WS('; ',
{reason_items}
           )
    FROM signals s
), position1 AS (
    SELECT scored.*, Position1 = CHARINDEX('; ', FullReasons) FROM scored
), position2 AS (
    SELECT position1.*,
           Position2 = CASE WHEN Position1 > 0 THEN CHARINDEX('; ', FullReasons, Position1 + 2) ELSE 0 END
    FROM position1
), position3 AS (
    SELECT position2.*,
           Position3 = CASE WHEN Position2 > 0 THEN CHARINDEX('; ', FullReasons, Position2 + 2) ELSE 0 END
    FROM position2
)
SELECT StudentCode, StudentName, TermCode, TermStartDate, TermCurrentTermFlag,
       ProgramCode, ProgramName, AcademicStandingName,
       TermGPAValue, PriorTermGPAValue, EffectiveGPA,
       CreditHoursAttempted, CreditHoursEarned,
       CompletionRatio, PriorCompletionRatio, EffectiveCompletionRatio, CompletionChange,
       DaysSinceLastActivity, ActivityTrendPct, SubmissionCount, OnTimeSharePct,
       AdvisorContacts, AdvisorKept, AdvisorMissed, DaysSinceLastContact,
       AidOffered, AidAccepted, AidDisbursed,
       OpenFinancialCases, OpenFinancialDays, OpenTechnicalCases, OpenPersonalCases, OpenAcademicCases,
       FlagLowGPA, FlagLowCompletion, FlagCompletionDrop,
       FlagInactiveLMS, FlagDecliningLMS, FlagLateSubmissions,
       FlagNoAdvisorContact, FlagAdvisorMissed, FlagAidUnaccepted,
       FlagOpenFinancialCase, FlagOpenOtherCase,
       RiskScore,
       RiskLevel = CASE WHEN RiskScore >= {high_t} THEN 'High'
                        WHEN RiskScore >= {mod_t}  THEN 'Moderate'
                        ELSE 'Low' END,
       RiskReasons = CASE WHEN FullReasons = '' THEN 'no active risk signals'
                          WHEN Position3 > 0 THEN LEFT(FullReasons, Position3 - 1)
                          ELSE FullReasons END
FROM position3;
"""


def connect() -> pyodbc.Connection:
    driver = next(d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d)
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={os.environ.get('EDW_SERVER', 'localhost,1433')};"
        f"DATABASE={os.environ.get('EDW_DATABASE', 'EDW_Dev')};"
        f"UID={os.environ.get('EDW_USER', 'sa')};PWD={os.environ['EDW_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=yes", autocommit=True)


def rows_as_dicts(cur) -> list[dict]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    cur.execute(SIGNALS_SQL + "SELECT * FROM signals")
    signals = rows_as_dicts(cur)

    cur.execute("""SELECT StudentCode, TermCode, TermStartDate FROM data.StudentHistory
                   WHERE StudentStatusEnrolledFlag = 'Y'""")
    history = rows_as_dicts(cur)
    cur.execute("""SELECT StudentCode, AwardStart = MIN(AwardTermStartDate) FROM data.Award
                   GROUP BY StudentCode""")
    award_start = {r["StudentCode"]: r["AwardStart"] for r in rows_as_dicts(cur)}

    last_start_by_student: dict[str, date] = {}
    starts_by_student: dict[str, list[date]] = {}
    for r in history:
        starts_by_student.setdefault(r["StudentCode"], []).append(r["TermStartDate"])
    for k, v in starts_by_student.items():
        last_start_by_student[k] = max(v)
    max_term_start = max(last_start_by_student.values())

    # label historical rows: non-return = no enrollment in any later term
    labeled = []
    for s in signals:
        t = s["TermStartDate"]
        if t >= max_term_start:
            continue                      # current term: outcome unknown
        a = award_start.get(s["StudentCode"])
        if a is not None and a <= t:
            continue                      # graduated: leaving is not attrition
        s["_nonreturn"] = 0 if any(x > t for x in starts_by_student[s["StudentCode"]]) else 1
        labeled.append(s)

    base = sum(s["_nonreturn"] for s in labeled) / len(labeled)
    print(f"labeled student-terms: {len(labeled):,}   base non-return rate: {base:.1%}\n")
    print(f"{'signal':<44}{'n(flag)':>8}{'P(nr|flag)':>11}{'P(nr|~flag)':>12}{'odds ratio':>11}{'weight':>7}")

    weights: dict[str, int] = {}
    derivation = []
    for flag, (label, source, _) in FLAGS.items():
        with_f = [s for s in labeled if s[flag]]
        without = [s for s in labeled if not s[flag]]
        n1 = len(with_f)
        p1 = sum(s["_nonreturn"] for s in with_f) / n1 if n1 else 0.0
        p0 = sum(s["_nonreturn"] for s in without) / len(without) if without else 0.0
        if n1 == 0 or p1 in (0.0, 1.0) or p0 in (0.0, 1.0):
            odds, w = None, 0
        else:
            odds = (p1 / (1 - p1)) / (p0 / (1 - p0))
            w = max(0, round(4 * math.log(odds)))
        weights[flag] = w
        derivation.append({"flag": flag, "label": label, "source": source, "n_flag": n1,
                           "nonreturn_with_flag": round(p1, 4), "nonreturn_without_flag": round(p0, 4),
                           "odds_ratio": round(odds, 2) if odds else None, "weight": w})
        print(f"{label:<44}{n1:>8,}{p1:>11.1%}{p0:>12.1%}{(f'{odds:.2f}' if odds else '—'):>11}{w:>7}")

    total_weight = sum(weights.values()) or 1

    # score the labeled rows and find level thresholds from observed rates
    for s in labeled:
        raw = sum(weights[f] for f in FLAGS if s[f])
        s["_score"] = round(100 * raw / total_weight)
    print(f"\nnon-return rate by score band (labeled rows, total weight {total_weight}):")
    bands = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50), (50, 101)]
    band_rates = {}
    for lo, hi in bands:
        rows = [s for s in labeled if lo <= s["_score"] < hi]
        rate = sum(s["_nonreturn"] for s in rows) / len(rows) if rows else None
        band_rates[(lo, hi)] = rate
        print(f"  score {lo:>3}-{hi - 1:<3}  n={len(rows):>6,}   non-return {('—' if rate is None else f'{rate:.1%}')}")
    high_t = next((lo for (lo, hi) in bands if (band_rates[(lo, hi)] or 0) >= 0.60), 50)
    mod_t = next((lo for (lo, hi) in bands if (band_rates[(lo, hi)] or 0) >= 0.30), 25)
    if mod_t >= high_t:
        mod_t = max(10, high_t - 20)
    print(f"\nthresholds: High >= {high_t}, Moderate >= {mod_t}")

    (HERE / "weights.json").write_text(json.dumps({
        "derived_at": str(date.today()), "labeled_rows": len(labeled),
        "base_nonreturn_rate": round(base, 4), "total_weight": total_weight,
        "high_threshold": high_t, "moderate_threshold": mod_t,
        "method": "weight = round(4 * ln(odds ratio of non-return with vs without the flag)), floored at 0",
        "signals": derivation}, indent=1))
    print(f"wrote {HERE / 'weights.json'}")

    weight_comment = "\n".join(
        f"--   {d['label']:<46} weight {d['weight']:>2}  (odds ratio {d['odds_ratio'] if d['odds_ratio'] is not None else 'n/a'})"
        for d in derivation)
    score_terms = " +\n".join(f"               s.{f} * {weights[f]}" for f in FLAGS)
    # reason items in descending weight order so CONCAT_WS yields the top
    # contributors first; zero-weight flags contribute nothing and are omitted
    by_weight = sorted((f for f in FLAGS if weights[f] > 0), key=lambda f: -weights[f])
    reason_items = ",\n".join(f"               CASE WHEN s.{f} = 1 THEN {FLAGS[f][2]} END" for f in by_weight)
    sql_dir = HERE / "sql"
    sql_dir.mkdir(exist_ok=True)
    (sql_dir / "data_StudentRisk.sql").write_text(VIEW_TEMPLATE.format(
        today=date.today(), weight_comment=weight_comment, signals_sql=SIGNALS_SQL.strip(),
        score_terms=score_terms, total_weight=total_weight, reason_items=reason_items,
        high_t=high_t, mod_t=mod_t))
    print(f"wrote {sql_dir / 'data_StudentRisk.sql'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
