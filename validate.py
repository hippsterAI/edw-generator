#!/usr/bin/env python3
"""
validate.py -- fail loudly when model.yaml breaks the pattern.

    python validate.py [--model model.yaml] [--out out]

Checks the resolved model (implied conformance columns included) and, when
out/ exists, the emitted SQL: every dimension table carries all conformance
columns, sqlparse can tokenise every batch, and no file references a
warehouse object that a later file creates.

Exit code 1 with a numbered list of violations; 0 when clean.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from edw.model import (DIM_CONFORMANCE_SUFFIXES, FACT_CONFORMANCE_SUFFIXES, PASCAL_RE, Model,
                       base_type, load_model, pascal_tokens, type_length)

# Tokens that are abbreviations of a word. A name containing one fails.
ABBREVIATIONS = {
    "Abbr", "Acad", "Acct", "Addr", "Adm", "Amt", "Appl", "Apt", "Avg", "Bal", "Bldg", "Cal", "Cat", "Cd",
    "Chg", "Cnt", "Cntry", "Co", "Cred", "Crs", "Ct", "Ctry", "Cur", "Curr", "Db", "Dec", "Def", "Dept",
    "Desc", "Dev", "Dim", "Dir", "Dist", "Div", "Doc", "Dt", "Dtm", "Ed", "Emp", "Enr", "Eff", "Equiv", "Est",
    "Ext", "Fac", "Fin", "Flg", "Fld", "Fmt", "Freq", "Frm", "Ftpt", "Grp", "Hr", "Hrs", "Id", "Ident",
    "Inc", "Ind", "Inst", "Instr", "Int", "Inv", "Lang", "Len", "Loc", "Lvl", "Mgr", "Misc",
    "Mo", "Mod", "Msg", "Mth", "Nbr", "Nm", "No", "Num", "Obj", "Org", "Orig", "Pct", "Ph", "Phn", "Pos",
    "Prev", "Prg", "Prgm", "Prim", "Proc", "Prod", "Prog", "Pt", "Qtr", "Qty", "Rec", "Ref", "Reg", "Req",
    "Res", "Sched", "Sec", "Sem", "Seq", "Sess", "Src", "Srvc", "Stat", "Std", "Stu", "Sub", "Subj", "Svc",
    "Sys", "Tbl", "Tel", "Tm", "Tot", "Trm", "Ttl", "Ty", "Typ", "Upd", "Usr", "Val",
    "Vend", "Wk", "Yr", "Yrs",
}


class Violations(list):
    def add(self, where: str, message: str) -> None:
        self.append(f"{where}: {message}")


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

def check_name(v: Violations, where: str, name: str, allowed_acronyms: set[str], allow_conformance: bool = False) -> None:
    if not PASCAL_RE.match(name):
        v.add(where, f"'{name}' is not PascalCase (letters/digits only, leading capital, no underscores)")
        return
    for tok in pascal_tokens(name):
        word = re.sub(r"\d+$", "", tok)
        if not word:
            continue
        if word.isupper() and len(word) > 1 and word not in allowed_acronyms and not (allow_conformance and word == "SCD"):
            v.add(where, f"'{name}' contains acronym '{word}' not listed in naming.allowed_acronyms")
        elif word in ABBREVIATIONS:
            v.add(where, f"'{name}' contains abbreviation '{word}'; spell it out")


def check_column_conventions(v: Violations, where: str, name: str, sql_type: str) -> None:
    bt = base_type(sql_type)
    if name.endswith("Flag") and sql_type.lower().replace(" ", "") != "char(1)":
        v.add(where, f"'{name}' ends in Flag so it must be char(1), got {sql_type}")
    if name.endswith("Date") and not name.endswith("DateTime") and bt != "date":
        v.add(where, f"'{name}' ends in Date so it must be type date, got {sql_type}")
    if name.endswith("DateTime") and bt not in ("datetime", "datetime2", "smalldatetime"):
        v.add(where, f"'{name}' ends in DateTime so it must be a datetime type, got {sql_type}")
    if (name.endswith("Code") or name.endswith("Name") or name.endswith("CodeAndName")) and bt not in ("varchar", "nvarchar", "char", "nchar"):
        v.add(where, f"'{name}' ends in Code/Name so it must be a character type, got {sql_type}")
    if bt in ("bit", "boolean") :
        v.add(where, f"'{name}' uses {sql_type}; booleans are char(1) <Thing>Flag")


# ---------------------------------------------------------------------------
# Model checks
# ---------------------------------------------------------------------------

def check_model(model: Model) -> Violations:
    v = Violations()
    acronyms = set(model.naming.get("allowed_acronyms") or [])
    for kind, names in (("dimension", [d.name for d in model.dims]), ("fact", [f.name for f in model.facts])):
        for n in sorted({n for n in names if names.count(n) > 1}):
            v.add("model", f"{kind} name '{n}' is used more than once")
    staging = [e.staging_table for e in [*model.dims, *model.facts]]
    for n in sorted({n for n in staging if staging.count(n) > 1}):
        v.add("model", f"staging table stg.{n} is claimed by more than one entity; set staging.table on one of them")

    for d in model.dims:
        where = f"dim {d.name}"
        check_name(v, where, d.name, acronyms)
        if not d.attributes:
            v.add(where, "declares no business attributes")
        if not d.source_key:
            v.add(where, "declares no source_key")
        seen: set[str] = set()
        for a in d.attributes:
            aw = f"{where}.{a.name}"
            if a.name in seen:
                v.add(aw, "duplicate attribute name")
            seen.add(a.name)
            check_name(v, aw, a.name, acronyms)
            if not a.type:
                v.add(aw, "missing type")
            else:
                check_column_conventions(v, aw, a.name, a.type)
            if a.scd not in (1, 2):
                v.add(aw, f"scd marker must be 1 or 2, got {a.scd!r}")
            for suffix in DIM_CONFORMANCE_SUFFIXES:
                if a.name == d.conf(suffix):
                    v.add(aw, f"conformance column {a.name} is implied by the pattern; do not declare it")
            if a.mapping and (type_length(a.type) or 0) < 50:
                v.add(aw, f"mapped attributes must be at least varchar(50) to hold helper.CodeMapping.StandardCode, got {a.type}")
            if a.mapping_name_of and (type_length(a.type) or 0) < 100:
                v.add(aw, f"mapping_name_of attributes must be at least varchar(100) to hold helper.CodeMapping.StandardName, got {a.type}")
            if a.mapping_name_of:
                target = d.attribute(a.mapping_name_of)
                if target is None or not target.mapping:
                    v.add(aw, f"mapping_name_of '{a.mapping_name_of}' must name an attribute with a mapping")
                elif target.scd != a.scd:
                    v.add(aw, f"scd marker must match the mapped code attribute '{target.name}' (scd {target.scd})")
            if a.expression:
                for ref in re.findall(r"\{(\w+)\}", a.expression):
                    if d.attribute(ref) is None:
                        v.add(aw, f"expression references unknown attribute '{ref}'")
        for k in d.source_key:
            a = d.attribute(k)
            if a is None:
                v.add(where, f"source_key column '{k}' is not a declared attribute")
            elif not a.in_staging:
                v.add(where, f"source_key column '{k}' is derived and not present in staging")
        # Conformance completeness on the resolved model.
        resolved = {c.name for c in d.all_columns}
        for suffix in DIM_CONFORMANCE_SUFFIXES:
            if d.conf(suffix) not in resolved:
                v.add(where, f"missing conformance column {d.conf(suffix)}")

    for f in model.facts:
        where = f"fact {f.name}"
        check_name(v, where, f.name, acronyms)
        if not f.dimension_keys:
            v.add(where, "declares no dimension_keys")
        roles: set[str] = set()
        for dk in f.dimension_keys:
            kw = f"{where}.{dk.role}"
            check_name(v, kw, dk.role, acronyms)
            if dk.role in roles:
                v.add(kw, "duplicate role")
            if dk.role == f.name:
                v.add(kw, f"role equals the fact name, so {dk.column} would collide with the fact key; name the role for what the dimension plays here")
            roles.add(dk.role)
            if dk.dimension is None:
                v.add(kw, f"points at dimension '{dk.dim}' which is not in the model")
                continue
            if len(dk.source) != len(dk.dimension.source_key):
                v.add(kw, f"source has {len(dk.source)} column(s) but {dk.dim}.source_key has {len(dk.dimension.source_key)}")
            for s in dk.source:
                check_name(v, kw, s, acronyms)
        for c in f.source_columns:
            cw = f"{where}.{c.name}"
            check_name(v, cw, c.name, acronyms)
            if not c.type:
                v.add(cw, "missing type")
            else:
                check_column_conventions(v, cw, c.name, c.type)
        for m in f.measures:
            mw = f"{where}.{m.name}"
            check_name(v, mw, m.name, acronyms)
            if not m.type:
                v.add(mw, "missing type")
            elif base_type(m.type) not in ("int", "bigint", "smallint", "tinyint", "decimal", "numeric", "money", "float", "real"):
                v.add(mw, f"measures must be numeric (additive), got {m.type}")
            if m.name in roles or f"{m.name}Key" in {dk.column for dk in f.dimension_keys}:
                v.add(mw, "measure name collides with a role")
            for suffix in FACT_CONFORMANCE_SUFFIXES:
                if m.name == f.conf(suffix):
                    v.add(mw, "conformance column is implied by the pattern; do not declare it")
        if not f.source_key:
            v.add(where, "declares no source_key")
        names = [c.name for c in f.all_columns]
        for n in sorted({n for n in names if names.count(n) > 1}):
            v.add(where, f"column '{n}' would be produced more than once in {f.table}")
        if all(dk.dimension for dk in f.dimension_keys):
            staging = {c.name for c in f.staging_columns}
            for k in f.source_key:
                if k not in staging:
                    v.add(where, f"source_key column '{k}' is not a staging column (must be a dimension source column, source_columns entry, or measure)")
        if len(f.index_keys) != 2:
            v.add(where, f"index_keys must name exactly two roles, got {f.index_keys}")
        for r in f.index_keys:
            if r not in roles:
                v.add(where, f"index_keys role '{r}' is not a declared dimension_keys role")
        if f.large and (not f.partial_refresh_role or f.partial_refresh_role not in roles):
            v.add(where, "large facts must set partial_refresh_role to one of their roles")
        if f.partial_refresh_role and not f.large:
            v.add(where, "partial_refresh_role is only meaningful when large: true")
        # Data view column uniqueness.
        if all(dk.dimension for dk in f.dimension_keys):
            from generate import data_view_columns
            names = [n for n, _ in data_view_columns(f)]
            for n in sorted({n for n in names if names.count(n) > 1}):
                v.add(where, f"data view column '{n}' would be produced more than once")
    return v


# ---------------------------------------------------------------------------
# Emitted SQL checks
# ---------------------------------------------------------------------------

GO_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)
CREATE_RE = re.compile(r"\b(?:CREATE|ALTER)\s+(?:TABLE|VIEW|PROCEDURE|UNIQUE\s+CLUSTERED\s+INDEX|NONCLUSTERED\s+INDEX)\s+"
                       r"(?:(?:\w+)\s+ON\s+)?(\w+)\.(\w+)", re.IGNORECASE)
CREATE_SCHEMA_RE = re.compile(r"CREATE\s+SCHEMA\s+\[(\w+)\]", re.IGNORECASE)
REF_RE = re.compile(r"\b(catalog|control|helper|stg|dim|fact|etl|lookup|data)\.(\w+)\b")


def check_output(model: Model, out_dir: Path) -> Violations:
    v = Violations()
    files = sorted(p for p in out_dir.glob("*.sql") if not p.name.startswith("99"))
    if not files:
        v.add(str(out_dir), "no generated SQL found; run generate.py first")
        return v
    try:
        import sqlparse
    except ImportError:
        sqlparse = None
        v.add("sqlparse", "not installed; skipping tokenisation check (pip install sqlparse)")

    # 1. Conformance columns present in the emitted dimension DDL.
    ddl = (out_dir / "04_dimensions.sql").read_text() if (out_dir / "04_dimensions.sql").exists() else ""
    for d in model.dims:
        m = re.search(rf"CREATE TABLE dim\.{d.name} \((.*?)\n\);", ddl, re.S)
        if not m:
            v.add(f"04_dimensions.sql", f"no CREATE TABLE for dim.{d.name}")
            continue
        body = m.group(1)
        for suffix in DIM_CONFORMANCE_SUFFIXES:
            col = d.conf(suffix)
            if not re.search(rf"^\s+{col}\s", body, re.M):
                v.add(f"04_dimensions.sql dim.{d.name}", f"missing conformance column {col}")
        if not re.search(rf"IX_dim_{d.name}_SourceSystemKey ON dim\.{d.name}\s*\(\s*{d.conf('SourceSystemKey')}, {d.conf('SourceSystemName')}, {d.conf('ActiveRecordFlag')}\)", ddl):
            v.add(f"04_dimensions.sql dim.{d.name}", "missing non-clustered index on (SourceSystemKey, SourceSystemName, ActiveRecordFlag)")

    # 2. sqlparse tokenises each batch; 3. dependency order across files.
    defined: set[str] = set()
    for path in files:
        text = path.read_text()
        for i, batch in enumerate(GO_RE.split(text)):
            if not batch.strip():
                continue
            if sqlparse:
                try:
                    stmts = sqlparse.parse(batch)
                    if not stmts:
                        v.add(f"{path.name} batch {i}", "sqlparse produced no statements")
                except Exception as e:  # pragma: no cover
                    v.add(f"{path.name} batch {i}", f"sqlparse failed: {e}")
            batch_defs = {f"{s}.{o}".lower() for s, o in CREATE_RE.findall(batch)}
            # A batch may reference itself (stub-then-ALTER); add before checking references.
            defined |= batch_defs
            for schema, obj in REF_RE.findall(batch):
                ref = f"{schema}.{obj}".lower()
                if ref in defined or schema.lower() in ("etl",) and obj.startswith("Fact_") and obj.endswith("_PartialRefresh"):
                    continue
                # Procedure stubs create the object in the same batch via EXEC('CREATE PROCEDURE ...').
                if re.search(rf"'CREATE (?:PROCEDURE|VIEW) {schema}\.{obj} ", batch):
                    defined.add(ref)
                    continue
                if ref.startswith("tempdb."):
                    continue
                v.add(f"{path.name} batch {i}", f"references {schema}.{obj} before any file defines it")
        for s in CREATE_SCHEMA_RE.findall(text):
            defined.add(f"schema.{s}".lower())
    return v


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", default="model.yaml")
    ap.add_argument("--out", default="out", help="generated SQL directory; checked when present")
    ap.add_argument("--skip-output", action="store_true", help="only validate the model")
    args = ap.parse_args()

    try:
        model = load_model(args.model)
    except Exception as e:
        print(f"FAIL: cannot load {args.model}: {e}")
        return 1

    violations = check_model(model)
    out_dir = Path(args.out)
    if not args.skip_output and out_dir.exists():
        violations.extend(check_output(model, out_dir))

    if violations:
        print(f"FAIL: {len(violations)} violation(s)")
        for i, msg in enumerate(violations, 1):
            print(f"  {i:>3}. {msg}")
        return 1
    print(f"OK: model valid ({len(model.dims)} dims, {len(model.facts)} facts, {len(model.roles)} roles)"
          + ("" if args.skip_output or not out_dir.exists() else f"; {out_dir}/ consistent with model"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
