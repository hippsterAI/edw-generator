# EDW Generator

A dimensional-warehouse **generator** for community colleges. The product is
not a warehouse; it is a pattern encoded as rules plus a model file. Edit
`model.yaml`, run `generate.py`, and the full SQL Server warehouse — schemas,
staging, dimensions, facts, views, ETL procedures, orchestrators, and the
metadata catalog that drives them — is emitted from scratch.

Clean-room: the pattern below is a set of structural conventions. No
institution's column list, business logic, or naming is reproduced.

```
model.yaml ──► generate.py ──► out/*.sql ──► load.py deploy ──► SQL Server
     │                                               │
     └──► validate.py (fails on any rule violation)  └──► load.py seed / run / query
```

## Scope

| Dimensions (22) | Facts (10) |
|---|---|
| Calendar, Time, Term, PartOfTerm, Student, Applicant, Faculty, Program, Course, Class, Location, DeliveryType, Grade, GPA, Age, AcademicStanding, StudentStatus, RegistrationStatus, ApplicationState, Advisement, CustomCohort, FinancialAidSource | StudentClass, StudentHistory, StudentJourney, CurrentStudent, Registration, Award, Advisement, FinancialAid, Class, CustomCohort |

Every entity carries a deliberately minimal attribute set drawn from public
higher-ed domain knowledge (IPEDS reporting concepts, standard registrar
vocabulary). It is a starting model meant to be extended, not a superset.

Deployed footprint: 32 staging tables, 22 dimension tables, 10 fact tables,
50 lookup views, 10 data views, 39 ETL procedures, 4 tenant tables, 3
catalog tables, 1 run log. With the default synthetic seed (5,000 students,
8 terms) the full load runs in about 13 seconds.

## The pattern

### Every dimension

```
<Entity>Key                  int identity, clustered PK
...business attributes...
<Entity>SourceSystemName     varchar(50)    NOT NULL
<Entity>SourceSystemKey      varchar(200)   NOT NULL
<Entity>SCDType1HashValue    varbinary(64)  NOT NULL
<Entity>SCDType2HashValue    varbinary(64)  NOT NULL
<Entity>ActiveRecordFlag     char(1)        NOT NULL
<Entity>StartEffectiveDate   date           NOT NULL
<Entity>EndEffectiveDate     date           NOT NULL
<Entity>LastUpdatedDateTime  datetime       NOT NULL
+ nonclustered index (SourceSystemKey, SourceSystemName, ActiveRecordFlag)
+ an Unknown member row with key -1
```

Why each conformance column exists:

| Column | Purpose |
|---|---|
| `SourceSystemName` | Which system the row came from. Lets two sources coexist in one dimension (SIS + CRM applicants) without key collisions, and lets `helper.CodeMapping` be scoped per source. |
| `SourceSystemKey` | The natural key as the source expresses it (`'|'`-joined when composite). The only way back from the warehouse to the source record; the join column for every fact load. |
| `SCDType1HashValue` | SHA2-512 over the Type 1 attributes. A single comparison tells the load "something overwrite-able changed" without comparing every column. |
| `SCDType2HashValue` | SHA2-512 over the Type 2 attributes. A difference means "close this version and open a new one". |
| `ActiveRecordFlag` | `'Y'` on the current version of each key. Facts join on it; the index covers it. Cheaper than `EndEffectiveDate = '9999-12-31'` and unambiguous. |
| `StartEffectiveDate` / `EndEffectiveDate` | The half-open interval `[Start, End)` during which this version was true. End is `9999-12-31` while active. Enables as-of queries. |
| `LastUpdatedDateTime` | When the pipeline last touched the row. Audit and incremental-extract support for downstream consumers. |

### Every fact

```
<Fact>Key                    int identity, clustered PK
<Role>Key ...                one FK per dimension role, named for the ROLE
...additive measures...
<Fact>SourceSystemName       varchar(50)    NOT NULL
<Fact>SourceSystemKey        varchar(200)   NOT NULL
<Fact>LastUpdatedDateTime    datetime       NOT NULL
+ nonclustered index on the two highest-cardinality FKs
+ foreign key constraints to every dimension
```

### Naming — no exceptions, enforced by `validate.py`

| Thing | Rule |
|---|---|
| Surrogate key | `<Entity>Key` |
| Code column | `<Entity>Code` — the source system's value (or the mapped standard value) |
| Name column | `<Entity>Name` — human readable |
| Combined | `<Entity>CodeAndName` |
| Booleans | `<Thing>Flag`, `char(1)` `'Y'`/`'N'`. Never `bit`. |
| Dates | `<Event>Date`, type `date`. `<Event>DateTime` for datetimes. |
| Case | PascalCase. No underscores. No abbreviations (`Desc`, `Nbr`, `Cd`, `Stu`…). Acronyms only if listed in `naming.allowed_acronyms`. |

### Role-playing dimensions

When a fact references one dimension in several roles, the FK is named for
the role (`RegistrationDateKey`, `WithdrawalDateKey`, both → `dim.Calendar`)
and a view in `lookup` exposes the dimension under that name. The dimension
table is never duplicated.

Column renaming in a role view strips the dimension prefix and applies the
role prefix, collapsing a duplicated trailing token and leaving columns
that already start with the role name alone:

```
lookup.RegistrationDate:  CalendarKey   → RegistrationDateKey
                          CalendarDate  → RegistrationDate
                          CalendarYear  → RegistrationDateYear
                          WeekdayFlag   → RegistrationDateWeekdayFlag
lookup.Section (Class):   ClassCode     → SectionCode
                          SectionNumber → SectionNumber
lookup.AgeAtAward (Age):  AgeValue      → AgeAtAwardValue
                          AgeBandName   → AgeAtAwardBandName
```

A role may never equal the fact's own name (`<Role>Key` would collide with
`<Fact>Key`), which is why fact `Class` references dim `Class` as `Section`,
fact `Advisement` references dim `Advisement` as `AdvisementService`, and
fact `CustomCohort` references dim `CustomCohort` as `Cohort`. `validate.py`
enforces this.

### Schemas

| Schema | Contents |
|---|---|
| `catalog` | One row per dimension, fact, staging table. **The pipeline is driven by these rows.** |
| `control` | `ProcessLog` run history; `ExecutionStart/Completion/Failure`; `PrepareStagingTable`, `ValidateTableName`. |
| `helper` | **Tenant configuration** — the only place per-institution variation lives: `AgeBandDefinition`, `DeliveryType`, `SchoolName`, `CodeMapping`. |
| `stg` | Staging: 1:1 landing tables, no transformation. |
| `dim` / `fact` | The model. |
| `etl` | Generated transform procedures and orchestrators. |
| `lookup` | One view per dimension role. |
| `data` | One consumption view per fact, pre-joined, keys resolved to names. Role-qualified column names (`StudentResidencyName`, `ClassDeliveryTypeName`) so wide joins never collide. |

### The pipeline

```
EXTRACT     source → stg.<Entity>            out of scope to execute; DDL + catalog rows are generated.
                                             Call control.PrepareStagingTable before each load.
TRANSFORM   stg → dim/fact                   etl.Dim_<X>_InsertUpdate
                                             etl.Fact_<X>_FullRefresh
                                             etl.Fact_<X>_PartialRefresh   (large: true only)
CONTROL     etl.Load_Warehouse               → Load_Dimensions → Load_Facts, walking catalog rows
                                             in LoadOrder. Dimensions before facts, always.
```

Every procedure logs to `control.ProcessLog` with a parent key, so a run is a
tree: `Load_Warehouse` → `Load_Dimensions` → `Dim_Student_InsertUpdate` …

**`Dim_<X>_InsertUpdate`** (one transaction):

1. Shape staging into `#Source`: one row per source key (duplicates
   collapsed), `helper.CodeMapping` applied, derived expressions computed,
   both hashes calculated.
2. **Expire** — active rows whose `SCDType2HashValue` differs: set
   `EndEffectiveDate = @LoadDate`, `ActiveRecordFlag = 'N'`.
3. **Insert** — every source key with no active row (new keys, and the keys
   just expired) as a new active version.
4. **Overwrite** — rows whose `SCDType1HashValue` differs: update the Type 1
   attributes in place on *every* version of that key (Type 1 rewrites history).

**`Fact_<X>_FullRefresh`**: truncate, then insert from staging, resolving
each `<Role>Key` by joining the dimension on `SourceSystemKey` +
`ActiveRecordFlag = 'Y'`. Unresolved keys get `-1` (Unknown), so FKs always
hold and no fact row is ever dropped for a missing dimension.

**`Fact_<X>_PartialRefresh`**: same insert, but scoped — every
`partial_refresh_role` key present in staging (e.g. every Term) is deleted
and reloaded; other slices are untouched. Generated only for `large: true`.

## Files

| File | Role |
|---|---|
| `model.yaml` | The model. **The only file you edit to add an entity.** |
| `edw/model.py` | Loads and resolves the model (implies conformance columns, binds FKs). |
| `generate.py` | Renders `out/01..11` + `out/99_drop_all.sql`. |
| `validate.py` | Fails with a numbered list if the model or the emitted SQL breaks a rule. |
| `seed_synthetic.py` | Fake staging data (invented names from syllables, sequential IDs). No real people. |
| `load.py` | Runs the SQL via `sqlcmd`: `deploy`, `seed`, `run`, `query`, `drop`, `all`. |

Generated output:

```
out/01_schemas.sql              out/07_lookup_views.sql
out/02_control_framework.sql    out/08_data_views.sql
out/03_staging.sql              out/09_etl_procedures.sql
out/04_dimensions.sql           out/10_orchestrators.sql
out/05_facts.sql                out/11_catalog_seed.sql
out/06_foreign_keys.sql         out/99_drop_all.sql
out/seed/*.sql                  (from seed_synthetic.py)
```

All DDL is idempotent: tables/indexes/FKs/schemas/seed rows are guarded by
existence checks; procedures and views use stub-then-`ALTER` so re-running a
file is always safe. Note the corollary: a **column type change** in
`model.yaml` is not applied to an existing table by re-running `04`. Use
`load.py drop` then `deploy` (dev), or write a migration (prod).

## How to run

Requirements: Python 3.10+, `pip install pyyaml sqlparse`, `sqlcmd` on PATH,
a SQL Server 2016+ instance.

```bash
python generate.py                 # model.yaml -> out/
python validate.py                 # model rules + emitted-SQL consistency; exit 1 on any violation
python seed_synthetic.py           # -> out/seed/  (5,000 students, 8 terms, 200 courses, 40 programs)

export EDW_SERVER=localhost,1433 EDW_DATABASE=EDW_Dev EDW_USER=sa EDW_PASSWORD='...'
python load.py deploy              # creates the database if needed, runs out/01..11
python load.py seed                # loads tenant config + staging
python load.py run                 # EXEC etl.Load_Warehouse; prints the ProcessLog tree
python load.py query               # sample rows from data.StudentClass
python load.py query --sql "SELECT TermCode, COUNT(*) FROM data.StudentClass GROUP BY TermCode"
python load.py run --mode Partial  # uses Fact_<X>_PartialRefresh where the catalog has one
python load.py drop                # out/99_drop_all.sql
```

`seed_synthetic.py --students 20000 --terms 12 --courses 400 --programs 60 --faculty 300 --seed 7`
scales the fake data. It reads staging column lists from `model.yaml`, so the
seed can never drift from the generated DDL.

## How to add an entity

Edit `model.yaml` only. Never `generate.py`.

### A dimension

```yaml
dims:
  Faculty:
    description: One row per instructor.
    source_key: [FacultyCode]                     # staging column(s) forming SourceSystemKey
    staging: {source_system: SIS, source_object: Instructor}
    attributes:
      - {name: FacultyCode,          type: varchar(20),  scd: 1}
      - {name: FacultyName,          type: varchar(100), scd: 1}
      - {name: FacultyCodeAndName,   type: varchar(130), scd: 1,
         expression: "CONCAT({FacultyCode}, ' - ', {FacultyName})"}
      - {name: EmploymentTypeCode,   type: varchar(50),  scd: 2, mapping: EmploymentType}
      - {name: EmploymentTypeName,   type: varchar(100), scd: 2, mapping_name_of: EmploymentTypeCode}
      - {name: FullTimeFlag,         type: char(1),      scd: 2}
```

Declare **only business attributes**. Every conformance column is implied.
Every attribute carries `scd: 1` (overwrite) or `scd: 2` (version).

Attribute options:

| Option | Meaning |
|---|---|
| `expression` | Derived from other attributes with `{Name}` placeholders. Not in staging. |
| `mapping` | The staging value is a *source* code; `helper.CodeMapping` (`MappingName`, per `SourceSystemName`) translates it to the standard code. Unmapped codes pass through unchanged. Must be ≥ `varchar(50)`. |
| `mapping_name_of` | This column is the `StandardName` for the named mapped code column. Not in staging. Must be ≥ `varchar(100)`. |
| `nullable` | Default `true`. |

`expression` is any T-SQL. The `Age` dimension uses a scalar subquery
against `helper.AgeBandDefinition` to band ages by tenant configuration;
`GPA` uses a `CASE`. Banded dimensions (`GPA`, `Age`) hold one row per
value (0.00–4.00 at 0.01; 0–120 years) so facts join on plain equality and
the band logic lives in one place.

`staging: {table: ...}` overrides the staging table name (default
`stg.<Entity>`); required when a dim and a fact share a name.

Quote any type containing a comma: `type: "decimal(5,2)"` (YAML flow mappings
split on commas; the loader rejects the stray key).

### A fact

```yaml
facts:
  Registration:
    description: One row per registration transaction.
    grain: student x class x registration event
    large: true
    partial_refresh_role: Term
    source_key: [StudentCode, TermCode, ClassCode, RegistrationSequence]
    staging: {source_system: SIS, source_object: RegistrationHistory}
    dimension_keys:
      - {role: Student,          dim: Student,  source: [StudentCode]}
      - {role: Class,            dim: Class,    source: [TermCode, ClassCode]}
      - {role: Term,             dim: Term,     source: [TermCode]}
      - {role: RegistrationDate, dim: Calendar, source: [RegistrationDate]}
      - {role: DropDate,         dim: Calendar, source: [DropDate]}
    measures:
      - {name: RegistrationCount, type: int}
      - {name: CreditHours,       type: "decimal(5,2)"}
    index_keys: [Student, Class]
```

* `role` names the FK (`<Role>Key`) and the `lookup` view. Reuse a dimension
  under several roles freely.
* `source` lists the staging columns holding that dimension's source key,
  positionally aligned with the dimension's `source_key`. Types are
  inherited from the dimension, so the fact never restates them.
* The fact's staging table is the union of every `source` column plus the
  measures.
* `source_columns` declares staging columns that exist only to make the
  `source_key` unique (a transaction sequence number, for example).
* `large: true` adds `Fact_<X>_PartialRefresh` scoped by `partial_refresh_role`.

Then:

```bash
python generate.py && python validate.py && python load.py deploy
```

New objects are created; existing ones are untouched. The new entity is
registered in `catalog.*` and picked up by `etl.Load_Warehouse` on the next
run — no orchestrator edits.

If you extend `seed_synthetic.py` for a new entity, add a `make_*` method
on `Population` and register it in `staging_data()`; the script refuses to
emit a staging table it has no generator for. Facts are derived from one
in-memory population, so term summaries, registrations, awards, milestones
and the current-student snapshot all agree with the enrollment rows.

## Tenant configuration

The model never varies per institution. Codes do. Everything institution
specific lives in `helper`:

| Table | Holds |
|---|---|
| `helper.CodeMapping` | `(MappingName, SourceSystemName, SourceCode) → (StandardCode, StandardName)`. Used by any attribute with `mapping:`. |
| `helper.DeliveryType` | The institution's delivery modalities and which count as distance education. |
| `helper.AgeBandDefinition` | Age bands for the Age dimension (IPEDS-style ranges). |
| `helper.SchoolName` | The institution's own identity for labels and multi-campus setups. |

Seed these per tenant (see `out/seed/00_tenant_config.sql` for the synthetic example).

## What the shipped model does not contain

No PII columns: no birth date, ethnicity, gender, address, phone, email.
Institutions add those to `model.yaml` themselves under their own
governance; the product does not ship them.

## Known simplifications (starter model)

* Facts resolve dimension keys to the **active** version, not the version
  effective on the event date. As-of joins (`StartEffectiveDate <= EventDate < EndEffectiveDate`)
  are a straightforward extension of `fact_select()` in `generate.py`.
* Two Type 2 changes to the same key on the same `@LoadDate` produce a
  version with `Start = End` (a zero-length interval). Correct for
  half-open semantics; visible in history.
* Dimension load order is declaration order in `model.yaml` (`LoadOrder` in
  `catalog.Dimensions`). Dimensions do not depend on each other in this
  pattern (no snowflaking), so any order works.
