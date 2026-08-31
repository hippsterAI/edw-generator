#!/usr/bin/env python3
"""
seed_synthetic.py -- FAKE data for every staging table so the pipeline can
run end to end. Nothing here is real: names are built from syllables, IDs
are sequential, distributions are plausible but invented. No real people.

    python seed_synthetic.py [--students 5000] [--terms 8] [--courses 200]
                             [--programs 40] [--faculty 150] [--start-year 2023]
                             [--seed 42] [--model model.yaml] [--out out/seed]

Emits T-SQL INSERT batches:
    out/seed/00_tenant_config.sql    helper.* rows (tenant configuration)
    out/seed/NN_stg_<Table>.sql      one file per staging table, dims then facts

Column order is read from model.yaml, so the seed can never drift from the
generated staging DDL: a generator that omits a staging column fails here,
and a staging table with no generator fails here.

Internal consistency: facts are DERIVED from the same in-memory population --
term summaries aggregate the enrollments, registrations replay them as
transactions, awards follow credit accumulation, the current-student
snapshot is whoever is enrolled in the last term.
"""
from __future__ import annotations

import argparse
import calendar
import random
from collections import defaultdict
from datetime import date, time, timedelta
from pathlib import Path

from edw.model import Model, load_model

BATCH = 500  # rows per INSERT ... VALUES (SQL Server allows 1000)

# ---------------------------------------------------------------------------
# Invented vocabulary. Deliberately not drawn from any name list.
# ---------------------------------------------------------------------------

FIRST_SYLLABLES = ["Ta", "Vo", "Ren", "Mi", "Kal", "Su", "Or", "El", "Dra", "Fen", "Lu", "Pa", "Quin", "Ze", "Ny", "Bel"]
MID_SYLLABLES = ["v", "r", "l", "n", "d", "th", "m", "s", "k", "sh"]
END_SYLLABLES = ["a", "en", "is", "o", "ia", "et", "ar", "ik", "ue", "on"]
SURNAME_STARTS = ["Mol", "Har", "Ves", "Tor", "Bran", "Cal", "Wen", "Ost", "Pel", "Rud", "Sal", "Yar", "Kir", "Dov", "Fal", "Nem"]
SURNAME_ENDS = ["dik", "wick", "ley", "ston", "field", "mere", "vale", "ford", "brook", "haven", "wyn", "grave"]

SUBJECTS = [  # (SubjectCode, SubjectName, developmental-capable, DivisionName)
    ("ART", "Art", False, "Arts and Humanities"), ("BIO", "Biology", False, "Science and Mathematics"),
    ("BUS", "Business", False, "Business and Technology"), ("CHM", "Chemistry", False, "Science and Mathematics"),
    ("CIS", "Computer Information Systems", False, "Business and Technology"), ("COM", "Communication", False, "Arts and Humanities"),
    ("ECN", "Economics", False, "Social Sciences"), ("EDU", "Education", False, "Social Sciences"),
    ("ENG", "English", True, "Arts and Humanities"), ("HIS", "History", False, "Social Sciences"),
    ("HLT", "Health Sciences", False, "Health Sciences"), ("MAT", "Mathematics", True, "Science and Mathematics"),
    ("MUS", "Music", False, "Arts and Humanities"), ("NUR", "Nursing", False, "Health Sciences"),
    ("PHY", "Physics", False, "Science and Mathematics"), ("PSY", "Psychology", False, "Social Sciences"),
    ("SOC", "Sociology", False, "Social Sciences"), ("SPA", "Spanish", False, "Arts and Humanities"),
    ("WLD", "Welding Technology", False, "Career and Technical Education"), ("AUT", "Automotive Technology", False, "Career and Technical Education"),
]
COURSE_WORDS = ["Introduction to", "Principles of", "Foundations of", "Applied", "Intermediate", "Advanced",
                "Survey of", "Topics in", "Practicum in", "Seminar in"]
COURSE_NOUNS = ["Systems", "Methods", "Analysis", "Practice", "Theory", "Design", "Studies", "Concepts", "Techniques", "Applications"]
AWARD_TYPES = [("AA", "Associate of Arts", 60, 0.25), ("AS", "Associate of Science", 60, 0.25),
               ("AAS", "Associate of Applied Science", 60, 0.30), ("CERT", "Certificate", 30, 0.20)]
STUDENT_TYPES = [("FT", "First-Time", 0.35), ("TR", "Transfer", 0.15), ("CO", "Continuing", 0.40), ("RE", "Returning", 0.10)]
APPLICANT_TYPES = [("FT", "First-Time", 0.5), ("TR", "Transfer", 0.2), ("RA", "Readmit", 0.15), ("DE", "Dual Enrollment", 0.15)]
# Source-system codes (tenant-specific; standardised by helper.CodeMapping). 'OTH' is intentionally unmapped.
RESIDENCY_SOURCE = [("IN", 0.55), ("OUT", 0.25), ("OS", 0.12), ("INTL", 0.05), ("OTH", 0.03)]
DELIVERY_SOURCE = [("F2F", 0.55), ("ONL", 0.30), ("HYB", 0.15)]
EMPLOYMENT_SOURCE = [("FT", 0.35), ("ADJ", 0.55), ("PT", 0.10)]
GRADES = [  # (GradeCode, GradeName, points, GradeTypeCode, GradeTypeName, CountsInGPA, EarnsCredit, Withdrawal, weight)
    ("A", "A", 4.0, "STD", "Standard", "Y", "Y", "N", 0.30), ("B", "B", 3.0, "STD", "Standard", "Y", "Y", "N", 0.28),
    ("C", "C", 2.0, "STD", "Standard", "Y", "Y", "N", 0.18), ("D", "D", 1.0, "STD", "Standard", "Y", "Y", "N", 0.06),
    ("F", "F", 0.0, "STD", "Standard", "Y", "N", "N", 0.08), ("W", "Withdrawal", None, "WDR", "Withdrawal", "N", "N", "Y", 0.07),
    ("I", "Incomplete", None, "INC", "Incomplete", "N", "N", "N", 0.01), ("P", "Pass", None, "PF", "Pass/Fail", "N", "Y", "N", 0.015),
    ("NP", "No Pass", None, "PF", "Pass/Fail", "N", "N", "N", 0.005),
]
ACADEMIC_STANDINGS = [("GOOD", "Good Standing", "Y", 1), ("WARN", "Academic Warning", "N", 2),
                      ("PROB", "Academic Probation", "N", 3), ("SUSP", "Academic Suspension", "N", 4)]
STUDENT_STATUSES = [("APPL", "Applied", "N", 1), ("ADMT", "Admitted", "N", 2), ("ENRL", "Enrolled", "Y", 3),
                    ("STOP", "Stopped Out", "N", 4), ("COMP", "Completed", "N", 5), ("GRAD", "Graduated", "N", 6)]
REGISTRATION_STATUSES = [("RE", "Registered", "Y", "N"), ("DR", "Dropped", "N", "Y"), ("WD", "Withdrawn", "N", "Y"), ("WL", "Waitlisted", "N", "N")]
APPLICATION_STATES = [("INQ", "Inquiry", "N", "N", 1), ("APPL", "Applied", "N", "N", 2), ("COMP", "Application Complete", "Y", "N", 3),
                      ("ADMT", "Admitted", "Y", "Y", 4), ("DENY", "Denied", "Y", "N", 5), ("ENRL", "Enrolled", "Y", "Y", 6)]
ADVISEMENTS = [("NSO", "New Student Orientation", "ONB", "Onboarding", "Y"), ("REG", "Registration Planning", "ACAD", "Academic", "N"),
               ("DEG", "Degree Audit Review", "ACAD", "Academic", "N"), ("EWA", "Early Alert Follow-Up", "SUCC", "Student Success", "N"),
               ("TRN", "Transfer Planning", "ACAD", "Academic", "N"), ("GRD", "Graduation Check", "ACAD", "Academic", "Y")]
COHORTS = [("PROMISE", "Promise Scholars", "SCHOL", "Scholarship"), ("TRIO", "Support Services Program", "SUPP", "Support Program"),
           ("DUAL", "Dual Enrollment", "DUAL", "Dual Enrollment"), ("VET", "Veteran Services", "SUPP", "Support Program"),
           ("HON", "Honors Program", "HON", "Honors")]
AID_SOURCES = [  # (code, name, AidTypeCode, AidTypeName, FundingSourceCode, FundingSourceName, NeedBased, typical amount, take-up weight)
    ("PELL", "Federal Need Grant", "GRANT", "Grant", "FED", "Federal", "Y", 3200, 0.45),
    ("SEOG", "Federal Supplemental Grant", "GRANT", "Grant", "FED", "Federal", "Y", 600, 0.10),
    ("STGR", "State Access Grant", "GRANT", "Grant", "STATE", "State", "Y", 1500, 0.25),
    ("DSUB", "Federal Subsidized Loan", "LOAN", "Loan", "FED", "Federal", "Y", 1750, 0.20),
    ("DUNS", "Federal Unsubsidized Loan", "LOAN", "Loan", "FED", "Federal", "N", 2000, 0.15),
    ("FSCH", "Foundation Scholarship", "SCHOL", "Scholarship", "INST", "Institutional", "N", 1000, 0.12),
    ("PSCH", "Private Donor Scholarship", "SCHOL", "Scholarship", "PRIV", "Private", "N", 750, 0.05),
    ("FWS", "Federal Work Study", "WORK", "Work Study", "FED", "Federal", "Y", 1200, 0.06),
]
# Multi-source descriptors: CRM contacts, LMS activity, ITSM support cases.
CONTACT_TYPES = [("APPT", "Appointment", 0.4), ("CALL", "Phone Call", 0.25), ("EMAIL", "Email", 0.25), ("WALK", "Walk-In", 0.10)]
CONTACT_OUTCOMES = [("KEPT", "Kept"), ("NOSHOW", "No-Show"), ("CANC", "Cancelled")]
CONTACT_TOPICS = [("ACAD", "Academic Planning"), ("FIN", "Financial"), ("CAREER", "Career"), ("PERS", "Personal")]
ACTIVITY_TYPES = [("LOGIN", "Login"), ("SUBMIT", "Assignment Submission"), ("DISC", "Discussion Post"), ("QUIZ", "Quiz Attempt")]
ACTIVITY_PLATFORMS = [("WEB", "Web Portal", 0.8), ("MOB", "Mobile App", 0.2)]
CASE_CATEGORIES = [("FIN", "Financial"), ("TECH", "Technical"), ("PERS", "Personal"), ("ACAD", "Academic")]
CASE_PRIORITIES = [("LOW", "Low"), ("MED", "Medium"), ("HIGH", "High")]
CASE_STATUSES = [("OPEN", "Open", "Y"), ("RES", "Resolved", "N"), ("CLSD", "Closed Unresolved", "N")]

CAMPUSES = [("MAIN", "Main Campus"), ("NORTH", "North Center"), ("DTWN", "Downtown Center")]
BUILDINGS = ["Science Hall", "Liberal Arts Building", "Technology Center", "Health Sciences Building", "Library Annex"]
CLASS_START_TIMES = [time(8, 0), time(9, 30), time(11, 0), time(12, 30), time(14, 0), time(15, 30), time(17, 30), time(19, 0)]
FACULTY_RANKS = [("INST", "Instructor", 0.5), ("ASST", "Assistant Professor", 0.25), ("ASSC", "Associate Professor", 0.15), ("PROF", "Professor", 0.10)]

CODE_MAPPINGS = {  # MappingName -> [(SourceCode, StandardCode, StandardName)]
    "DeliveryType": [("F2F", "InPerson", "In Person"), ("ONL", "Online", "Online"), ("HYB", "Hybrid", "Hybrid")],
    "Residency": [("IN", "InDistrict", "In District"), ("OUT", "OutOfDistrict", "Out of District"),
                  ("OS", "OutOfState", "Out of State"), ("INTL", "International", "International")],
    "EmploymentType": [("FT", "FullTime", "Full-Time Faculty"), ("ADJ", "Adjunct", "Adjunct Faculty"), ("PT", "PartTime", "Part-Time Faculty")],
}
AGE_BANDS = [  # IPEDS-style bands: (code, name, min, max)
    ("U18", "Under 18", 0, 17), ("18-19", "18 to 19", 18, 19), ("20-21", "20 to 21", 20, 21), ("22-24", "22 to 24", 22, 24),
    ("25-29", "25 to 29", 25, 29), ("30-34", "30 to 34", 30, 34), ("35-39", "35 to 39", 35, 39), ("40-49", "40 to 49", 40, 49),
    ("50-64", "50 to 64", 50, 64), ("65+", "65 and over", 65, 130),
]


def weighted(rng: random.Random, options):
    """options: iterable of tuples whose LAST element is a weight."""
    opts = list(options)
    return rng.choices(opts, weights=[o[-1] for o in opts], k=1)[0]


def person_name(rng: random.Random) -> str:
    first = rng.choice(FIRST_SYLLABLES) + rng.choice(MID_SYLLABLES) + rng.choice(END_SYLLABLES)
    last = rng.choice(SURNAME_STARTS) + rng.choice(SURNAME_ENDS)
    return f"{first.capitalize()} {last}"


def add_minutes(t: time, minutes: int) -> time:
    total = (t.hour * 60 + t.minute + minutes) % 1440
    return time(total // 60, total % 60)


# ---------------------------------------------------------------------------
# The synthetic population. Each make_* returns list[dict] keyed by staging
# column name; later generators derive from earlier results. Keys starting
# with "_" are simulation state and never reach staging.
# ---------------------------------------------------------------------------

class Population:
    def __init__(self, rng: random.Random, students: int, terms: int, courses: int, programs: int, faculty: int, start_year: int):
        self.rng = rng
        self.terms = self.make_terms(terms, start_year)
        self.term_index = {t["TermCode"]: i for i, t in enumerate(self.terms)}
        self.last_term = self.terms[-1]
        self.snapshot_date = self.last_term["TermStartDate"] + timedelta(days=30)
        self.calendar = self.make_calendar()
        self.time = self.make_time()
        self.parts_of_term = self.make_parts_of_term()
        self.programs = self.make_programs(programs)
        self.courses = self.make_courses(courses)
        self.credits = {c["CourseCode"]: c["CreditHours"] for c in self.courses}
        self.faculty = self.make_faculty(faculty)
        self.locations = self.make_locations()
        self.students = self.make_students(students)
        self.student_by_code = {s["StudentCode"]: s for s in self.students}
        self.applicants = self.make_applicants()
        self.classes = self.make_classes()
        self.student_classes = self.make_student_classes()
        self.term_summaries = self.make_term_summaries()
        self.awards = self.make_awards()
        self.enrolled_terms = defaultdict(list)  # StudentCode -> [TermCode ...] in order
        for s in self.term_summaries:
            self.enrolled_terms[s["StudentCode"]].append(s["TermCode"])
        self.risk_terms = self.assign_risk()
        self.milestones = self.make_milestones()
        self.current_students = self.make_current_students()
        self.registrations = self.make_registrations()
        self.advisement_sessions = self.make_advisement_sessions()
        self.financial_aid = self.make_financial_aid()
        self.class_sections = self.make_class_sections()
        self.cohort_memberships = self.make_cohort_memberships()
        self.advisor_contacts = self.make_advisor_contacts()
        self.learning_activity = self.make_learning_activity()
        self.support_cases = self.make_support_cases()

    # ------------------------------------------------------------ time ----
    def make_terms(self, n: int, start_year: int) -> list[dict]:
        cycle = [("FA", "Fall", (8, 20), (12, 12)), ("SP", "Spring", (1, 15), (5, 10)), ("SU", "Summer", (6, 2), (7, 28))]
        terms, year, i = [], start_year, 0
        while len(terms) < n:
            code, name, (sm, sd), (em, ed) = cycle[i % 3]
            start, end = date(year, sm, sd), date(year, em, ed)
            terms.append({"TermCode": f"{year}{code}", "TermName": f"{name} {year}",
                          "AcademicYear": year + 1 if code == "FA" else year, "TermTypeCode": code, "TermTypeName": name,
                          "TermStartDate": start, "TermEndDate": end, "CensusDate": start + timedelta(days=14), "CurrentTermFlag": "N"})
            i += 1
            if code == "FA":
                year += 1
        terms[-1]["CurrentTermFlag"] = "Y"
        return terms

    def make_calendar(self) -> list[dict]:
        first = self.terms[0]["TermStartDate"] - timedelta(days=365)
        last = self.terms[-1]["TermEndDate"] + timedelta(days=90)
        rows, d = [], first
        while d <= last:
            rows.append({"CalendarDate": d, "CalendarYear": d.year, "CalendarQuarter": (d.month - 1) // 3 + 1,
                         "CalendarMonth": d.month, "CalendarMonthName": calendar.month_name[d.month],
                         "CalendarDayOfMonth": d.day, "CalendarDayOfWeek": d.isoweekday() % 7 + 1,
                         "CalendarDayName": calendar.day_name[d.weekday()], "WeekdayFlag": "Y" if d.weekday() < 5 else "N",
                         "FiscalYear": d.year + 1 if d.month >= 7 else d.year, "AcademicYear": d.year + 1 if d.month >= 8 else d.year})
            d += timedelta(days=1)
        return rows

    def make_time(self) -> list[dict]:
        rows = []
        for h in range(24):
            for m in range(60):
                period = ("MORN", "Morning") if h < 12 else ("AFT", "Afternoon") if h < 17 else ("EVE", "Evening")
                rows.append({"TimeValue": time(h, m), "TimeName": time(h, m).strftime("%I:%M %p"), "HourOfDay": h, "MinuteOfHour": m,
                             "TimePeriodCode": period[0], "TimePeriodName": period[1]})
        return rows

    def make_parts_of_term(self) -> list[dict]:
        rows = []
        for t in self.terms:
            start, end = t["TermStartDate"], t["TermEndDate"]
            rows.append({"PartOfTermCode": "FULL", "PartOfTermName": "Full Term", "TermCode": t["TermCode"], "PartOfTermStartDate": start,
                         "PartOfTermEndDate": end, "CensusDate": t["CensusDate"], "WeekCount": (end - start).days // 7})
            if t["TermTypeCode"] != "SU":
                mid = start + timedelta(days=56)
                rows.append({"PartOfTermCode": "8W1", "PartOfTermName": "First Eight Weeks", "TermCode": t["TermCode"], "PartOfTermStartDate": start,
                             "PartOfTermEndDate": mid - timedelta(days=1), "CensusDate": start + timedelta(days=7), "WeekCount": 8})
                rows.append({"PartOfTermCode": "8W2", "PartOfTermName": "Second Eight Weeks", "TermCode": t["TermCode"], "PartOfTermStartDate": mid,
                             "PartOfTermEndDate": end, "CensusDate": mid + timedelta(days=7), "WeekCount": 8})
        return rows

    # -------------------------------------------------------- catalog ----
    def make_programs(self, n: int) -> list[dict]:
        rows = []
        for i in range(n):
            subj_code, subj_name, _, division = SUBJECTS[i % len(SUBJECTS)]
            award = weighted(self.rng, AWARD_TYPES)
            rows.append({"ProgramCode": f"{subj_code}-{award[0]}{i // len(SUBJECTS) + 1}",
                         "ProgramName": f"{subj_name} {'Certificate' if award[0] == 'CERT' else award[1].split()[-1]}",
                         "AwardTypeCode": award[0], "AwardTypeName": award[1], "_required_credits": award[2],
                         "InstructionalProgramCode": f"{self.rng.randint(10, 52):02d}.{self.rng.randint(100, 9999):04d}",
                         "DivisionName": division, "ActiveProgramFlag": "Y" if self.rng.random() < 0.92 else "N"})
        return rows

    def make_courses(self, n: int) -> list[dict]:
        rows, used = [], set()
        while len(rows) < n:
            subj_code, subj_name, dev_ok, _ = self.rng.choice(SUBJECTS)
            developmental = dev_ok and self.rng.random() < 0.3
            number = f"{self.rng.randint(10, 99):03d}" if developmental else f"{self.rng.randint(100, 299)}"
            code = f"{subj_code}-{number}"
            if code in used:
                continue
            used.add(code)
            rows.append({"CourseCode": code, "CourseName": f"{self.rng.choice(COURSE_WORDS)} {subj_name} {self.rng.choice(COURSE_NOUNS)}",
                         "SubjectCode": subj_code, "SubjectName": subj_name, "CourseNumber": number,
                         "CreditHours": self.rng.choice([1, 3, 3, 3, 4, 4, 5]), "CourseLevelCode": "DEV" if developmental else "COL",
                         "CourseLevelName": "Developmental" if developmental else "College Level",
                         "ActiveCourseFlag": "Y" if self.rng.random() < 0.95 else "N"})
        return rows

    def make_faculty(self, n: int) -> list[dict]:
        rows = []
        for i in range(n):
            rank = weighted(self.rng, FACULTY_RANKS)
            rows.append({"FacultyCode": f"F{20000 + i}", "FacultyName": person_name(self.rng),
                         "EmploymentTypeCode": weighted(self.rng, EMPLOYMENT_SOURCE)[0],
                         "DepartmentName": SUBJECTS[i % len(SUBJECTS)][1], "FacultyRankCode": rank[0], "FacultyRankName": rank[1],
                         "ActiveFacultyFlag": "Y" if self.rng.random() < 0.93 else "N"})
        return rows

    def make_locations(self) -> list[dict]:
        rows = [{"LocationCode": "ONLINE", "LocationName": "Online", "CampusCode": "VIRT", "CampusName": "Virtual Campus",
                 "BuildingName": "Virtual", "RoomNumber": None, "LocationTypeCode": "ONL", "LocationTypeName": "Online"}]
        for campus_code, campus_name in CAMPUSES:
            for b, building in enumerate(BUILDINGS[: 5 if campus_code == "MAIN" else 2]):
                for room in range(1, 6):
                    lab = room == 5
                    rows.append({"LocationCode": f"{campus_code}-{b + 1}{room:02d}{'L' if lab else ''}", "LocationName": f"{building} {b + 1}{room:02d}",
                                 "CampusCode": campus_code, "CampusName": campus_name, "BuildingName": building, "RoomNumber": f"{b + 1}{room:02d}",
                                 "LocationTypeCode": "LAB" if lab else "CLS", "LocationTypeName": "Laboratory" if lab else "Classroom"})
        return rows

    # --------------------------------------------------------- people ----
    def make_students(self, n: int) -> list[dict]:
        rows = []
        for i in range(n):
            stype = weighted(self.rng, STUDENT_TYPES)
            first_term = self.rng.choice(self.terms[: max(1, len(self.terms) - 1)])
            # age at first term: skewed young with a long adult-learner tail
            age = 17 + int(self.rng.expovariate(1 / 7.0)) if self.rng.random() < 0.85 else self.rng.randint(30, 62)
            rows.append({"StudentCode": f"S{1000000 + i}", "StudentName": person_name(self.rng), "StudentTypeCode": stype[0], "StudentTypeName": stype[1],
                         "ResidencyCode": weighted(self.rng, RESIDENCY_SOURCE)[0], "DegreeSeekingFlag": "Y" if self.rng.random() < 0.8 else "N",
                         "FirstTermCode": first_term["TermCode"], "_age_at_first_term": min(age, 80),
                         "_program": self.rng.choice(self.programs)["ProgramCode"] if self.rng.random() < 0.95 else None})
        return rows

    def age_on(self, student: dict, on: date) -> int:
        first = self.terms[self.term_index[student["FirstTermCode"]]]["TermStartDate"]
        return student["_age_at_first_term"] + max(0, (on - first).days) // 365

    def make_applicants(self) -> list[dict]:
        """Admissions funnel shape (community-college realistic): of every 100
        applications started, ~70 are completed, ~55 admitted, ~35 registered.
        Enrolled students are the registered 35%; the other 65% are
        non-converting applicants whose journey stops where they abandoned."""
        rows, i = [], 0
        for s in self.students:  # every student applied and passed every stage
            i += 1
            atype = weighted(self.rng, APPLICANT_TYPES)
            rows.append({"ApplicantCode": f"A{500000 + i}", "ApplicantName": s["StudentName"], "ApplicantTypeCode": atype[0], "ApplicantTypeName": atype[1],
                         "ResidencyCode": s["ResidencyCode"], "ApplicationTermCode": s["FirstTermCode"], "IntendedProgramCode": s["_program"],
                         "ConvertedToStudentFlag": "Y", "StudentCode": s["StudentCode"], "_stage": "ENRL"})
        # non-converters: students are 35% of applications, so add 65/35 more.
        # Stop-stage mix rescaled to the 65% who never register:
        #   never completed 30/65, completed-not-admitted 15/65, admitted-not-registered 20/65
        for _ in range(round(len(self.students) * 65 / 35)):
            i += 1
            atype = weighted(self.rng, APPLICANT_TYPES)
            stage = weighted(self.rng, [("APPL", 30), ("COMP", 15), ("ADMT", 20)])[0]
            age = 17 + int(self.rng.expovariate(1 / 7.0)) if self.rng.random() < 0.85 else self.rng.randint(30, 62)
            rows.append({"ApplicantCode": f"A{500000 + i}", "ApplicantName": person_name(self.rng), "ApplicantTypeCode": atype[0], "ApplicantTypeName": atype[1],
                         "ResidencyCode": weighted(self.rng, RESIDENCY_SOURCE)[0],
                         "ApplicationTermCode": self.rng.choice(self.terms[: max(1, len(self.terms) - 1)])["TermCode"],
                         "IntendedProgramCode": self.rng.choice(self.programs)["ProgramCode"], "ConvertedToStudentFlag": "N", "StudentCode": None,
                         "_stage": stage, "_age": min(age, 80)})
        return rows

    # -------------------------------------------------------- schedule ----
    def make_classes(self) -> list[dict]:
        pot_by_key = {(p["TermCode"], p["PartOfTermCode"]): p for p in self.parts_of_term}
        rows = []
        for t in self.terms:
            crn = 10000
            for c in self.courses:
                if c["ActiveCourseFlag"] == "N" and self.rng.random() < 0.7:
                    continue
                sections = weighted(self.rng, [(1, 0.5), (2, 0.3), (3, 0.15), (4, 0.05)])[0]
                for s in range(1, sections + 1):
                    crn += 1
                    delivery = weighted(self.rng, DELIVERY_SOURCE)[0]
                    part = "FULL" if t["TermTypeCode"] == "SU" or self.rng.random() < 0.8 else self.rng.choice(["8W1", "8W2"])
                    pot = pot_by_key[(t["TermCode"], part)]
                    online = delivery == "ONL"
                    start_time = None if online else self.rng.choice(CLASS_START_TIMES)
                    rows.append({"ClassCode": str(crn), "ClassName": f"{c['CourseName']} ({c['CourseCode']}-{s:03d})", "TermCode": t["TermCode"],
                                 "PartOfTermCode": part, "CourseCode": c["CourseCode"], "SectionNumber": f"{s:03d}", "DeliveryTypeCode": delivery,
                                 "PrimaryFacultyCode": self.rng.choice(self.faculty)["FacultyCode"],
                                 "LocationCode": "ONLINE" if online else self.rng.choice(self.locations[1:])["LocationCode"],
                                 "MaximumEnrollmentCount": self.rng.choice([20, 24, 25, 30, 35]),
                                 "ClassStartDate": pot["PartOfTermStartDate"], "ClassEndDate": pot["PartOfTermEndDate"],
                                 "_start_time": start_time, "_end_time": None if online else add_minutes(start_time, self.rng.choice([75, 110]))})
        return rows

    def make_student_classes(self) -> list[dict]:
        by_term: dict[str, list[dict]] = defaultdict(list)
        for cl in self.classes:
            by_term[cl["TermCode"]].append(cl)
        rows = []
        for st in self.students:
            enrolled = False
            for t in self.terms[self.term_index[st["FirstTermCode"]]:]:
                if enrolled and self.rng.random() > (0.45 if t["TermTypeCode"] == "SU" else 0.72):
                    continue
                enrolled = True
                picks = self.rng.sample(by_term[t["TermCode"]], k=weighted(self.rng, [(1, 0.15), (2, 0.25), (3, 0.25), (4, 0.25), (5, 0.10)])[0])
                reg_date = t["TermStartDate"] - timedelta(days=self.rng.randint(1, 75))
                in_progress = t is self.last_term
                for cl in picks:
                    grade = None if in_progress else weighted(self.rng, GRADES)
                    ch = self.credits[cl["CourseCode"]]
                    withdrawn = grade is not None and grade[7] == "Y"
                    earns = grade is not None and grade[6] == "Y"
                    rows.append({"StudentCode": st["StudentCode"], "TermCode": t["TermCode"], "ClassCode": cl["ClassCode"], "CourseCode": cl["CourseCode"],
                                 "PartOfTermCode": cl["PartOfTermCode"], "ProgramCode": st["_program"], "FacultyCode": cl["PrimaryFacultyCode"],
                                 "DeliveryTypeCode": cl["DeliveryTypeCode"], "GradeCode": grade[0] if grade else None, "RegistrationDate": reg_date,
                                 "WithdrawalDate": t["TermStartDate"] + timedelta(days=self.rng.randint(15, 70)) if withdrawn else None,
                                 "CreditHoursAttempted": ch, "CreditHoursEarned": ch if earns else 0,
                                 "GradePoints": round(grade[2] * ch, 2) if grade and grade[2] is not None else 0,
                                 "RegisteredCount": 1, "CompletedCount": 0 if (withdrawn or in_progress) else 1, "WithdrawnCount": 1 if withdrawn else 0,
                                 "_gpa_credits": ch if grade and grade[5] == "Y" else 0})
        return rows

    # ------------------------------------------------------ outcomes ----
    def make_term_summaries(self) -> list[dict]:
        grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for r in self.student_classes:
            grouped[(r["StudentCode"], r["TermCode"])].append(r)
        rows = []
        for st in self.students:
            cum_earned = cum_points = cum_gpa_credits = 0.0
            for t in self.terms:
                recs = grouped.get((st["StudentCode"], t["TermCode"]))
                if not recs:
                    continue
                attempted = sum(r["CreditHoursAttempted"] for r in recs)
                earned = sum(r["CreditHoursEarned"] for r in recs)
                points = sum(r["GradePoints"] for r in recs)
                gpa_credits = sum(r["_gpa_credits"] for r in recs)
                cum_earned += earned
                cum_points += points
                cum_gpa_credits += gpa_credits
                term_gpa = round(points / gpa_credits, 2) if gpa_credits else None
                cum_gpa = round(cum_points / cum_gpa_credits, 2) if cum_gpa_credits else None
                standing = "GOOD" if cum_gpa is None or cum_gpa >= 2.0 else "WARN" if cum_gpa >= 1.5 else "PROB" if cum_gpa >= 1.0 else "SUSP"
                rows.append({"StudentCode": st["StudentCode"], "TermCode": t["TermCode"], "ProgramCode": st["_program"], "StudentStatusCode": "ENRL",
                             "AcademicStandingCode": standing, "TermGPAValue": term_gpa, "CumulativeGPAValue": cum_gpa,
                             "AgeAtTermStart": self.age_on(st, t["TermStartDate"]), "CreditHoursAttempted": attempted, "CreditHoursEarned": earned,
                             "CumulativeCreditHoursEarned": cum_earned, "GradePoints": points, "EnrolledClassCount": len(recs),
                             "FullTimeCount": 1 if attempted >= 12 else 0, "EnrolledCount": 1})
        return rows

    def make_awards(self) -> list[dict]:
        required = {p["ProgramCode"]: p["_required_credits"] for p in self.programs}
        by_student: dict[str, list[dict]] = defaultdict(list)
        for s in self.term_summaries:
            by_student[s["StudentCode"]].append(s)
        rows = []
        for st in self.students:
            program = st["_program"]
            if not program or st["DegreeSeekingFlag"] == "N":
                continue
            for i, s in enumerate(by_student[st["StudentCode"]]):
                if s["TermCode"] != self.last_term["TermCode"] and s["CumulativeCreditHoursEarned"] >= required[program] * 0.75 and self.rng.random() < 0.6:
                    t = self.terms[self.term_index[s["TermCode"]]]
                    award_date = t["TermEndDate"] + timedelta(days=7)
                    rows.append({"StudentCode": st["StudentCode"], "ProgramCode": program, "TermCode": s["TermCode"], "AwardDate": award_date,
                                 "AgeAtAward": self.age_on(st, award_date), "CumulativeGPAValue": s["CumulativeGPAValue"], "AwardCount": 1,
                                 "CreditHoursEarnedAtAward": s["CumulativeCreditHoursEarned"], "TermsToCompletionCount": i + 1})
                    break
        return rows

    def make_milestones(self) -> list[dict]:
        """One journey per APPLICANT. Application-phase milestones carry the
        ApplicationState funnel stage (started -> completed -> admitted ->
        registered) and stop where the applicant abandoned; only converters
        continue into post-enrollment milestones (stopped/completed/graduated,
        ApplicationState stays ENRL). Non-converters have no StudentCode."""
        awarded = {a["StudentCode"]: a for a in self.awards}
        enrolled_terms: dict[str, list[str]] = defaultdict(list)
        for s in self.term_summaries:
            enrolled_terms[s["StudentCode"]].append(s["TermCode"])
        stage_order = ["APPL", "COMP", "ADMT", "ENRL"]
        rows = []
        for ap in self.applicants:
            st = self.student_by_code.get(ap["StudentCode"]) if ap["StudentCode"] else None
            term = self.terms[self.term_index[ap["ApplicationTermCode"]]]
            program = st["_program"] if st else ap["IntendedProgramCode"]
            reached = stage_order[: stage_order.index(ap["_stage"]) + 1]

            # events: (StudentStatusCode, ApplicationStateCode, TermCode, date)
            entry = ap["ApplicationTermCode"]
            on = term["TermStartDate"] - timedelta(days=self.rng.randint(90, 200))
            events = [("APPL", "APPL", entry, on)]
            if "COMP" in reached:
                on += timedelta(days=self.rng.randint(5, 25))
                events.append(("APPL", "COMP", entry, on))
            if "ADMT" in reached:
                on += timedelta(days=self.rng.randint(7, 30))
                events.append(("ADMT", "ADMT", entry, on))
            elif ap["_stage"] == "COMP" and self.rng.random() < 0.6:
                # some completed-but-not-admitted applications were denied
                on += timedelta(days=self.rng.randint(10, 30))
                events.append(("APPL", "DENY", entry, on))
            if st is not None:  # registered: converted to a student
                events.append(("ENRL", "ENRL", entry, term["TermStartDate"]))
                terms = enrolled_terms[st["StudentCode"]]
                if st["StudentCode"] in awarded:
                    a = awarded[st["StudentCode"]]
                    events.append(("COMP", "ENRL", a["TermCode"], a["AwardDate"] - timedelta(days=7)))
                    events.append(("GRAD", "ENRL", a["TermCode"], a["AwardDate"]))
                elif terms and terms[-1] != self.last_term["TermCode"]:
                    last = self.terms[self.term_index[terms[-1]]]
                    events.append(("STOP", "ENRL", last["TermCode"], last["TermEndDate"] + timedelta(days=1)))

            previous = None
            for status, app_state, term_code, on in events:
                rows.append({"ApplicantCode": ap["ApplicantCode"], "StudentCode": ap["StudentCode"], "TermCode": term_code,
                             "MilestoneDate": on, "ProgramCode": program, "StudentStatusCode": status, "ApplicationStateCode": app_state,
                             "AgeAtMilestone": self.age_on(st, on) if st else ap["_age"], "MilestoneCount": 1,
                             "DaysSincePreviousMilestone": (on - previous).days if previous else 0,
                             "TermsSinceFirstTerm": self.term_index[term_code] - self.term_index[ap["ApplicationTermCode"]]})
                previous = on
        return rows

    def make_current_students(self) -> list[dict]:
        by_student: dict[str, list[dict]] = defaultdict(list)
        for s in self.term_summaries:
            by_student[s["StudentCode"]].append(s)
        rows = []
        for s in self.term_summaries:
            if s["TermCode"] != self.last_term["TermCode"]:
                continue
            st = self.student_by_code[s["StudentCode"]]
            history = by_student[s["StudentCode"]]
            rows.append({"StudentCode": s["StudentCode"], "CurrentTermCode": self.last_term["TermCode"], "FirstTermCode": st["FirstTermCode"],
                         "ProgramCode": st["_program"], "StudentStatusCode": "ENRL", "AcademicStandingCode": s["AcademicStandingCode"],
                         "CumulativeGPAValue": s["CumulativeGPAValue"], "AgeAtSnapshot": self.age_on(st, self.snapshot_date), "SnapshotDate": self.snapshot_date,
                         "CumulativeCreditHoursAttempted": sum(h["CreditHoursAttempted"] for h in history),
                         "CumulativeCreditHoursEarned": s["CumulativeCreditHoursEarned"], "CurrentTermCreditHours": s["CreditHoursAttempted"],
                         "TermsEnrolledCount": len(history), "StudentCount": 1})
        return rows

    # ------------------------------------------------------- activity ----
    def make_registrations(self) -> list[dict]:
        def at_time() -> time:
            return time(self.rng.randint(7, 20), self.rng.randint(0, 59))
        rows = []
        for r in self.student_classes:
            base = {"StudentCode": r["StudentCode"], "TermCode": r["TermCode"], "ClassCode": r["ClassCode"], "CourseCode": r["CourseCode"],
                    "CreditHours": r["CreditHoursAttempted"], "TransactionCount": 1}
            rows.append({**base, "RegistrationSequenceNumber": 1, "RegistrationStatusCode": "RE", "RegistrationDate": r["RegistrationDate"],
                         "RegistrationTime": at_time(), "RegistrationCount": 1, "DropCount": 0})
            if r["WithdrawalDate"]:
                rows.append({**base, "RegistrationSequenceNumber": 2, "RegistrationStatusCode": "WD", "RegistrationDate": r["WithdrawalDate"],
                             "RegistrationTime": at_time(), "RegistrationCount": 0, "DropCount": 1})
            elif self.rng.random() < 0.06:  # a pre-census drop and re-add that never reached the enrollment grain
                drop_date = r["RegistrationDate"] + timedelta(days=self.rng.randint(1, 10))
                rows.append({**base, "RegistrationSequenceNumber": 2, "RegistrationStatusCode": "DR", "RegistrationDate": drop_date,
                             "RegistrationTime": at_time(), "RegistrationCount": 0, "DropCount": 1})
                rows.append({**base, "RegistrationSequenceNumber": 3, "RegistrationStatusCode": "RE", "RegistrationDate": drop_date + timedelta(days=self.rng.randint(1, 5)),
                             "RegistrationTime": at_time(), "RegistrationCount": 1, "DropCount": 0})
        return rows

    def make_advisement_sessions(self) -> list[dict]:
        rows, seen = [], set()
        for s in self.term_summaries:
            st = self.student_by_code[s["StudentCode"]]
            t = self.terms[self.term_index[s["TermCode"]]]
            first_term = s["TermCode"] == st["FirstTermCode"]
            if not first_term and self.rng.random() > 0.35:
                continue
            adv = ADVISEMENTS[0] if first_term else self.rng.choice(ADVISEMENTS[1:])
            on = t["TermStartDate"] + timedelta(days=self.rng.randint(-30, 60))
            at = time(self.rng.randint(8, 16), self.rng.choice([0, 15, 30, 45]))
            key = (s["StudentCode"], adv[0], on, at)
            if key in seen:
                continue
            seen.add(key)
            rows.append({"StudentCode": s["StudentCode"], "AdvisementCode": adv[0], "AdvisorCode": self.rng.choice(self.faculty)["FacultyCode"],
                         "TermCode": s["TermCode"], "ProgramCode": st["_program"], "AdvisementDate": on, "AdvisementTime": at,
                         "SessionCount": 1, "DurationMinutes": self.rng.choice([15, 30, 30, 45, 60])})
        return rows

    def make_financial_aid(self) -> list[dict]:
        rows = []
        for s in self.term_summaries:
            if self.rng.random() > 0.55:
                continue
            st = self.student_by_code[s["StudentCode"]]
            t = self.terms[self.term_index[s["TermCode"]]]
            for src in AID_SOURCES:
                if self.rng.random() >= src[8]:
                    continue
                offered = round(src[7] * self.rng.uniform(0.5, 1.2), 2)
                accepted = offered if src[2] != "LOAN" or self.rng.random() < 0.7 else round(offered * self.rng.uniform(0.3, 0.9), 2)
                disbursed = accepted if self.rng.random() < 0.9 else 0
                rows.append({"StudentCode": s["StudentCode"], "TermCode": s["TermCode"], "FinancialAidSourceCode": src[0], "ProgramCode": st["_program"],
                             "OfferDate": t["TermStartDate"] - timedelta(days=self.rng.randint(30, 120)),
                             "DisbursementDate": t["TermStartDate"] + timedelta(days=self.rng.randint(10, 25)) if disbursed else None,
                             "OfferedAmount": offered, "AcceptedAmount": accepted, "DisbursedAmount": disbursed, "AwardCount": 1})
        return rows

    def make_class_sections(self) -> list[dict]:
        enrolled: dict[tuple[str, str], int] = defaultdict(int)
        for r in self.student_classes:
            enrolled[(r["TermCode"], r["ClassCode"])] += 1
        rows = []
        for cl in self.classes:
            n = enrolled[(cl["TermCode"], cl["ClassCode"])]
            ch = self.credits[cl["CourseCode"]]
            rows.append({"TermCode": cl["TermCode"], "ClassCode": cl["ClassCode"], "CourseCode": cl["CourseCode"], "PartOfTermCode": cl["PartOfTermCode"],
                         "FacultyCode": cl["PrimaryFacultyCode"], "LocationCode": cl["LocationCode"], "DeliveryTypeCode": cl["DeliveryTypeCode"],
                         "ClassStartTime": cl["_start_time"], "ClassEndTime": cl["_end_time"], "SectionCount": 1, "EnrolledCount": n,
                         "WaitlistedCount": self.rng.randint(1, 6) if n >= cl["MaximumEnrollmentCount"] * 0.9 and self.rng.random() < 0.5 else 0,
                         "MaximumEnrollmentCount": cl["MaximumEnrollmentCount"], "CreditHours": ch,
                         "ContactHours": round(ch * 16 * (1.5 if cl["LocationCode"].endswith("L") else 1), 2)})
        return rows

    def make_cohort_memberships(self) -> list[dict]:
        rows = []
        for st in self.students:
            for code, *_ in COHORTS:
                if self.rng.random() < 0.06:
                    t = self.terms[self.term_index[st["FirstTermCode"]]]
                    rows.append({"StudentCode": st["StudentCode"], "CustomCohortCode": code, "EntryTermCode": t["TermCode"],
                                 "EntryDate": t["TermStartDate"] - timedelta(days=self.rng.randint(0, 30)), "ProgramCode": st["_program"], "MembershipCount": 1})
        return rows

    # ------------------------------------- multi-source risk simulation ----
    def assign_risk(self) -> dict[str, str]:
        """StudentCode -> the term in which the student's cross-system signals
        deteriorate (LMS activity stops, advisor contacts no-showed or absent,
        an unresolved financial case). ~70% of students who stop out without an
        award show the pattern in their final enrolled term; ~6% of persisters
        show it too and recover -- so the correlation with non-return is strong
        but deliberately imperfect (some leave with no warning, some recover)."""
        awarded = {a["StudentCode"] for a in self.awards}
        risk = {}
        for st in self.students:
            terms = self.enrolled_terms.get(st["StudentCode"])
            if not terms:
                continue
            stopped = terms[-1] != self.last_term["TermCode"] and st["StudentCode"] not in awarded
            if stopped and self.rng.random() < 0.70:
                risk[st["StudentCode"]] = terms[-1]
            elif not stopped and self.rng.random() < 0.06:
                risk[st["StudentCode"]] = self.rng.choice(terms)
        return risk

    def term_weeks(self, term: dict) -> list[date]:
        """Week start dates for a term; the in-progress term stops at the snapshot."""
        end = min(term["TermEndDate"], self.snapshot_date) if term is self.last_term else term["TermEndDate"]
        weeks, d = [], term["TermStartDate"]
        while d <= end:
            weeks.append(d)
            d += timedelta(days=7)
        return weeks

    def make_advisor_contacts(self) -> list[dict]:
        rows = []
        seq: dict[str, int] = defaultdict(int)
        prior: dict[str, date] = {}
        for s in self.term_summaries:
            code, term = s["StudentCode"], self.terms[self.term_index[s["TermCode"]]]
            at_risk = self.risk_terms.get(code) == s["TermCode"]
            if at_risk:
                # deteriorating students go quiet: over half have no contact at
                # all; the rest were reached for but mostly no-showed
                n = 0 if self.rng.random() < 0.55 else self.rng.randint(1, 3)
                outcomes = [("KEPT", 0.2), ("NOSHOW", 0.6), ("CANC", 0.2)]
                topics = [("FIN", 0.4), ("ACAD", 0.3), ("PERS", 0.2), ("CAREER", 0.1)]
            else:
                n = weighted(self.rng, [(0, 0.35), (1, 0.40), (2, 0.20), (3, 0.05)])[0]
                outcomes = [("KEPT", 0.85), ("NOSHOW", 0.10), ("CANC", 0.05)]
                topics = [("ACAD", 0.5), ("CAREER", 0.2), ("FIN", 0.15), ("PERS", 0.15)]
            end = min(term["TermEndDate"], self.snapshot_date) if term is self.last_term else term["TermEndDate"]
            span = max(1, (end - term["TermStartDate"]).days)
            for on in sorted(term["TermStartDate"] + timedelta(days=self.rng.randint(0, span)) for _ in range(n)):
                outcome = weighted(self.rng, outcomes)[0]
                ctype = weighted(self.rng, CONTACT_TYPES)[0]
                topic = weighted(self.rng, topics)[0]
                seq[code] += 1
                rows.append({"StudentCode": code, "TermCode": s["TermCode"], "AdvisorCode": self.rng.choice(self.faculty)["FacultyCode"],
                             "AdvisorContactCode": f"{ctype}-{outcome}-{topic}", "ContactDate": on, "ContactSequenceNumber": seq[code],
                             "ContactCount": 1, "KeptCount": 1 if outcome == "KEPT" else 0, "MissedCount": 1 if outcome == "NOSHOW" else 0,
                             "DaysSincePriorContact": (on - prior[code]).days if code in prior else 0})
                prior[code] = on
        return rows

    def make_learning_activity(self) -> list[dict]:
        """Weekly LMS engagement per student per class. At-risk students hit a
        'cliff' week 30-55% into their risk term: activity declines for two
        weeks, then stops. Withdrawn classes stop at the withdrawal date."""
        class_by_key = {(c["TermCode"], c["ClassCode"]): c for c in self.classes}
        platform: dict[str, str] = {}
        rows = []
        for r in self.student_classes:
            code = r["StudentCode"]
            term = self.terms[self.term_index[r["TermCode"]]]
            weeks = self.term_weeks(term)
            cl = class_by_key[(r["TermCode"], r["ClassCode"])]
            weeks = [w for w in weeks if cl["ClassStartDate"] <= w <= cl["ClassEndDate"]] or weeks[:1]
            plat = platform.setdefault(code, weighted(self.rng, ACTIVITY_PLATFORMS)[0])
            at_risk = self.risk_terms.get(code) == r["TermCode"]
            cliff = max(1, int(len(weeks) * self.rng.uniform(0.30, 0.55))) if at_risk else None
            last_active = weeks[0] - timedelta(days=self.rng.randint(1, 5))
            for wi, wstart in enumerate(weeks):
                factor = 1.0
                if cliff is not None and wi >= cliff:
                    factor = max(0.0, 1.0 - 0.5 * (wi - cliff + 1))       # fades over 2 weeks, then silence
                if r["WithdrawalDate"] and wstart > r["WithdrawalDate"]:
                    factor = 0.0
                elif not at_risk and self.rng.random() < 0.05:
                    factor = 0.0                                          # healthy students skip a week sometimes
                logins = round(self.rng.randint(2, 7) * factor)
                subs = round(self.rng.randint(0, 3) * factor)
                on_time = sum(1 for _ in range(subs) if self.rng.random() < (0.55 if at_risk and wi >= cliff else 0.85))
                week_end = wstart + timedelta(days=6)
                if logins or subs:
                    last_active = week_end - timedelta(days=self.rng.randint(0, 3))
                atype = weighted(self.rng, [("SUBMIT", 0.6), ("DISC", 0.2), ("QUIZ", 0.2)])[0] if subs else "LOGIN"
                rows.append({"StudentCode": code, "TermCode": r["TermCode"], "ClassCode": r["ClassCode"], "CourseCode": r["CourseCode"],
                             "WeekStartDate": wstart, "LearningActivityCode": f"{atype}-{plat}",
                             "LoginCount": logins, "SubmissionCount": subs, "OnTimeSubmissionCount": on_time,
                             "LateSubmissionCount": subs - on_time, "DaysSinceLastActivity": max(0, (week_end - last_active).days)})
        return rows

    def make_support_cases(self) -> list[dict]:
        rows, n = [], 0
        for s in self.term_summaries:
            code, term = s["StudentCode"], self.terms[self.term_index[s["TermCode"]]]
            end = min(term["TermEndDate"], self.snapshot_date) if term is self.last_term else term["TermEndDate"]
            at_risk = self.risk_terms.get(code) == s["TermCode"]
            cases = []
            if at_risk and self.rng.random() < 0.80:
                # the financial trouble behind the disengagement: opened mid-term, never resolved
                cases.append(("FIN", weighted(self.rng, [("MED", 0.4), ("HIGH", 0.6)])[0], "OPEN"))
                if self.rng.random() < 0.25:
                    cases.append((weighted(self.rng, [("TECH", 0.4), ("ACAD", 0.4), ("PERS", 0.2)])[0], "LOW", "RES"))
            elif self.rng.random() < 0.08:
                cat = weighted(self.rng, [("TECH", 0.40), ("ACAD", 0.30), ("PERS", 0.20), ("FIN", 0.10)])[0]
                status = weighted(self.rng, [("RES", 0.85), ("CLSD", 0.10), ("OPEN", 0.05)])[0]
                cases.append((cat, weighted(self.rng, [("LOW", 0.6), ("MED", 0.4)])[0], status))
            for cat, pri, status in cases:
                n += 1
                span = max(1, (end - term["TermStartDate"]).days)
                opened = term["TermStartDate"] + timedelta(days=self.rng.randint(5, span))
                if status == "OPEN":
                    closed, days_open = None, (end - opened).days
                else:
                    days_open = self.rng.randint(3, 20)
                    closed = min(opened + timedelta(days=days_open), end)
                rows.append({"StudentCode": code, "TermCode": s["TermCode"], "SupportCaseCode": f"{cat}-{pri}-{status}",
                             "OpenedDate": opened, "ClosedDate": closed, "SupportCaseNumber": f"C{100000 + n}",
                             "CaseCount": 1, "DaysOpen": max(0, days_open), "ReopenedCount": 1 if self.rng.random() < 0.08 else 0})
        return rows

    # ------------------------------------------- small reference dims ----
    def reference_dims(self) -> dict[str, list[dict]]:
        first_term = self.terms[0]["TermCode"]
        return {
            "DeliveryType": [{"DeliveryTypeCode": c, "DistanceEducationFlag": "N" if c == "F2F" else "Y"} for c, _ in DELIVERY_SOURCE],
            "Grade": [{"GradeCode": g[0], "GradeName": g[1], "GradePointValue": g[2], "GradeTypeCode": g[3], "GradeTypeName": g[4],
                       "CountsInGPAFlag": g[5], "EarnsCreditFlag": g[6], "WithdrawalFlag": g[7]} for g in GRADES],
            "GPA": [{"GPAValue": round(i / 100, 2)} for i in range(0, 401)],
            "Age": [{"AgeValue": a} for a in range(0, 121)],
            "AcademicStanding": [{"AcademicStandingCode": c, "AcademicStandingName": n, "GoodStandingFlag": g, "SortOrder": o} for c, n, g, o in ACADEMIC_STANDINGS],
            "StudentStatus": [{"StudentStatusCode": c, "StudentStatusName": n, "EnrolledFlag": e, "SortOrder": o} for c, n, e, o in STUDENT_STATUSES],
            "RegistrationStatus": [{"RegistrationStatusCode": c, "RegistrationStatusName": n, "CountsAsRegisteredFlag": r, "DropFlag": d}
                                   for c, n, r, d in REGISTRATION_STATUSES],
            "ApplicationState": [{"ApplicationStateCode": c, "ApplicationStateName": n, "CompletedApplicationFlag": f, "AdmittedFlag": a, "SortOrder": o}
                                 for c, n, f, a, o in APPLICATION_STATES],
            "Advisement": [{"AdvisementCode": c, "AdvisementName": n, "AdvisementTypeCode": tc, "AdvisementTypeName": tn, "RequiredFlag": r}
                           for c, n, tc, tn, r in ADVISEMENTS],
            "CustomCohort": [{"CustomCohortCode": c, "CustomCohortName": n, "CohortTypeCode": tc, "CohortTypeName": tn, "StartTermCode": first_term,
                              "ActiveCohortFlag": "Y"} for c, n, tc, tn in COHORTS],
            "FinancialAidSource": [{"FinancialAidSourceCode": s[0], "FinancialAidSourceName": s[1], "AidTypeCode": s[2], "AidTypeName": s[3],
                                    "FundingSourceCode": s[4], "FundingSourceName": s[5], "NeedBasedFlag": s[6]} for s in AID_SOURCES],
            "AdvisorContact": [{"AdvisorContactCode": f"{tc}-{oc}-{pc}", "AdvisorContactName": f"{tn}, {on}, {pn}",
                                "ContactTypeCode": tc, "ContactTypeName": tn, "ContactOutcomeCode": oc, "ContactOutcomeName": on,
                                "ContactTopicCode": pc, "ContactTopicName": pn, "ContactKeptFlag": "Y" if oc == "KEPT" else "N"}
                               for tc, tn, _ in CONTACT_TYPES for oc, on in CONTACT_OUTCOMES for pc, pn in CONTACT_TOPICS],
            "LearningActivity": [{"LearningActivityCode": f"{ac}-{pc}", "LearningActivityName": f"{an} via {pn}",
                                  "ActivityTypeCode": ac, "ActivityTypeName": an, "ActivityPlatformCode": pc, "ActivityPlatformName": pn}
                                 for ac, an in ACTIVITY_TYPES for pc, pn, _ in ACTIVITY_PLATFORMS],
            "SupportCase": [{"SupportCaseCode": f"{cc}-{rc}-{sc}", "SupportCaseName": f"{cn}, {rn} Priority, {sn}",
                             "SupportCaseCategoryCode": cc, "SupportCaseCategoryName": cn,
                             "SupportCasePriorityCode": rc, "SupportCasePriorityName": rn,
                             "SupportCaseResolutionStatusCode": sc, "SupportCaseResolutionStatusName": sn, "SupportCaseOpenFlag": of}
                            for cc, cn in CASE_CATEGORIES for rc, rn in CASE_PRIORITIES for sc, sn, of in CASE_STATUSES],
        }

    def staging_data(self) -> dict[str, list[dict]]:
        """Staging table name -> rows."""
        return {
            "Calendar": self.calendar, "Time": self.time, "Term": self.terms, "PartOfTerm": self.parts_of_term,
            "Student": self.students, "Applicant": self.applicants, "Faculty": self.faculty, "Program": self.programs,
            "Course": self.courses, "Class": self.classes, "Location": self.locations, **self.reference_dims(),
            "StudentClass": self.student_classes, "StudentHistory": self.term_summaries, "StudentJourney": self.milestones,
            "CurrentStudent": self.current_students, "Registration": self.registrations, "Award": self.awards,
            "AdvisementSession": self.advisement_sessions, "FinancialAid": self.financial_aid,
            "ClassSection": self.class_sections, "CustomCohortMembership": self.cohort_memberships,
            "AdvisorContactAttempt": self.advisor_contacts, "LearningActivityWeek": self.learning_activity,
            "SupportCaseLog": self.support_cases,
        }


# ---------------------------------------------------------------------------
# SQL emission
# ---------------------------------------------------------------------------

def literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "'Y'" if v else "'N'"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, (date, time)):
        return f"'{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"


def write_inserts(path: Path, table: str, columns: list[str], rows: list[dict]) -> None:
    missing = {c for r in rows[:1] for c in columns if c not in r}
    if missing:
        raise SystemExit(f"{table}: seed generator does not produce staging column(s) {sorted(missing)}")
    with path.open("w") as fh:
        fh.write(f"-- SYNTHETIC DATA for {table}: {len(rows)} rows. Generated by seed_synthetic.py. No real people.\n")
        fh.write(f"TRUNCATE TABLE {table};\nGO\n")
        cols = ", ".join(columns)
        for i in range(0, len(rows), BATCH):
            chunk = rows[i:i + BATCH]
            values = ",\n".join("(" + ", ".join(literal(r[c]) for c in columns) + ")" for r in chunk)
            fh.write(f"INSERT {table} ({cols}) VALUES\n{values};\nGO\n")


def write_tenant_config(path: Path, source_system: str) -> None:
    def guard(exists: str, insert: str) -> str:
        return f"IF NOT EXISTS (SELECT 1 FROM {exists})\n    {insert};\n"
    with path.open("w") as fh:
        fh.write("-- TENANT CONFIGURATION (synthetic). Idempotent. The only per-institution content in the warehouse.\n")
        for mapping, rows in CODE_MAPPINGS.items():
            for src, std, name in rows:
                fh.write(guard(f"helper.CodeMapping WHERE MappingName = '{mapping}' AND SourceSystemName = '{source_system}' AND SourceCode = '{src}'",
                               f"INSERT helper.CodeMapping (MappingName, SourceSystemName, SourceCode, StandardCode, StandardName) "
                               f"VALUES ('{mapping}', '{source_system}', '{src}', '{std}', '{name}')"))
        for i, (code, name, lo, hi) in enumerate(AGE_BANDS, start=1):
            fh.write(guard(f"helper.AgeBandDefinition WHERE AgeBandCode = '{code}'",
                           f"INSERT helper.AgeBandDefinition (AgeBandCode, AgeBandName, MinimumAge, MaximumAge, SortOrder) "
                           f"VALUES ('{code}', '{name}', {lo}, {hi}, {i})"))
        for i, (src, std, name) in enumerate(CODE_MAPPINGS["DeliveryType"], start=1):
            fh.write(guard(f"helper.DeliveryType WHERE DeliveryTypeCode = '{std}'",
                           f"INSERT helper.DeliveryType (DeliveryTypeCode, DeliveryTypeName, DistanceEducationFlag, SortOrder) "
                           f"VALUES ('{std}', '{name}', '{'N' if src == 'F2F' else 'Y'}', {i})"))
        fh.write(guard("helper.SchoolName WHERE SchoolCode = 'SYN'",
                       "INSERT helper.SchoolName (SchoolCode, SchoolName, SchoolShortName, PrimarySchoolFlag) "
                       "VALUES ('SYN', 'Synthetic Community College', 'Synthetic CC', 'Y')"))
        fh.write("GO\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--students", type=int, default=5000)
    ap.add_argument("--terms", type=int, default=8)
    ap.add_argument("--courses", type=int, default=200)
    ap.add_argument("--programs", type=int, default=40)
    ap.add_argument("--faculty", type=int, default=150)
    ap.add_argument("--start-year", type=int, default=2023)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--model", default="model.yaml")
    ap.add_argument("--out", default="out/seed")
    args = ap.parse_args()

    model: Model = load_model(args.model)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for stale in out.glob("*.sql"):
        stale.unlink()

    population = Population(random.Random(args.seed), args.students, args.terms, args.courses, args.programs, args.faculty, args.start_year)
    data = population.staging_data()

    write_tenant_config(out / "00_tenant_config.sql", model.settings["source_system_default"])
    print(f"wrote {out / '00_tenant_config.sql'}")

    entities = [*model.dims, *model.facts]
    for i, entity in enumerate(entities, start=1):
        if entity.staging_table not in data:
            raise SystemExit(f"seed_synthetic.py has no generator for {entity.staging}; add one to Population.staging_data()")
        cols = [c.name for c in entity.staging_columns]
        path = out / f"{i:02d}_stg_{entity.staging_table}.sql"
        write_inserts(path, entity.staging, cols, data[entity.staging_table])
        print(f"wrote {path}  ({len(data[entity.staging_table]):,} rows)")
    unused = sorted(set(data) - {e.staging_table for e in entities})
    if unused:
        print(f"note: generators with no staging table in the model: {unused}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
