"""
Model loading and resolution.

Reads model.yaml and turns it into fully-resolved Dimension / Fact objects
where every implied conformance column is materialised. generate.py renders
from the resolved model; validate.py checks it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Conformance pattern (PART 1 of the specification)
# ---------------------------------------------------------------------------

# (suffix, type, nullable) -- prefixed with the entity name at resolution time
DIM_CONFORMANCE_TRAILING = [
    ("SourceSystemName",    "varchar(50)",   False),
    ("SourceSystemKey",     "varchar(200)",  False),
    ("SCDType1HashValue",   "varbinary(64)", False),
    ("SCDType2HashValue",   "varbinary(64)", False),
    ("ActiveRecordFlag",    "char(1)",       False),
    ("StartEffectiveDate",  "date",          False),
    ("EndEffectiveDate",    "date",          False),
    ("LastUpdatedDateTime", "datetime",      False),
]
DIM_CONFORMANCE_SUFFIXES = ["Key"] + [s for s, _, _ in DIM_CONFORMANCE_TRAILING]

FACT_CONFORMANCE_TRAILING = [
    ("SourceSystemName",    "varchar(50)",  False),
    ("SourceSystemKey",     "varchar(200)", False),
    ("LastUpdatedDateTime", "datetime",     False),
]
FACT_CONFORMANCE_SUFFIXES = ["Key"] + [s for s, _, _ in FACT_CONFORMANCE_TRAILING]

SCHEMAS = ["catalog", "control", "helper", "stg", "dim", "fact", "etl", "lookup", "data"]


# ---------------------------------------------------------------------------
# Resolved model objects
# ---------------------------------------------------------------------------

@dataclass
class Column:
    name: str
    type: str
    nullable: bool = True
    scd: int | None = None          # 1 | 2 for business attributes, None for conformance/measures
    conformance: bool = False       # implied by the pattern, not declared
    expression: str | None = None   # derived from other staging columns
    mapping: str | None = None      # helper.CodeMapping group for a Code column
    mapping_name_of: str | None = None  # this column is the StandardName of that Code column

    @property
    def in_staging(self) -> bool:
        """Business attributes land in staging unless they are derived."""
        return (not self.conformance) and self.expression is None and self.mapping_name_of is None


@dataclass
class Dimension:
    name: str
    description: str
    source_key: list[str]
    source_system: str
    source_object: str
    staging_table: str
    attributes: list[Column]

    @property
    def table(self) -> str:
        return f"dim.{self.name}"

    @property
    def staging(self) -> str:
        return f"stg.{self.staging_table}"

    @property
    def key(self) -> str:
        return f"{self.name}Key"

    def conf(self, suffix: str) -> str:
        return f"{self.name}{suffix}"

    @property
    def conformance_columns(self) -> list[Column]:
        return [Column(self.conf(s), t, n, conformance=True) for s, t, n in DIM_CONFORMANCE_TRAILING]

    @property
    def all_columns(self) -> list[Column]:
        """Physical column order: key, business attributes, conformance trailer."""
        return [Column(self.key, "int", False, conformance=True)] + self.attributes + self.conformance_columns

    @property
    def staging_columns(self) -> list[Column]:
        return [a for a in self.attributes if a.in_staging]

    def attribute(self, name: str) -> Column | None:
        return next((a for a in self.attributes if a.name == name), None)

    def scd_attributes(self, scd: int) -> list[Column]:
        return [a for a in self.attributes if a.scd == scd]

    @property
    def source_key_columns(self) -> list[Column]:
        return [self.attribute(c) for c in self.source_key]


@dataclass
class DimensionKey:
    role: str
    dim: str
    source: list[str]
    dimension: Dimension | None = None   # bound at resolution

    @property
    def column(self) -> str:
        return f"{self.role}Key"


@dataclass
class Fact:
    name: str
    description: str
    grain: str
    large: bool
    partial_refresh_role: str | None
    source_key: list[str]
    source_system: str
    source_object: str
    staging_table: str
    dimension_keys: list[DimensionKey]
    measures: list[Column]
    index_keys: list[str]
    source_columns: list[Column] = field(default_factory=list)  # key-only staging columns

    @property
    def table(self) -> str:
        return f"fact.{self.name}"

    @property
    def staging(self) -> str:
        return f"stg.{self.staging_table}"

    @property
    def key(self) -> str:
        return f"{self.name}Key"

    def conf(self, suffix: str) -> str:
        return f"{self.name}{suffix}"

    @property
    def conformance_columns(self) -> list[Column]:
        return [Column(self.conf(s), t, n, conformance=True) for s, t, n in FACT_CONFORMANCE_TRAILING]

    @property
    def fk_columns(self) -> list[Column]:
        return [Column(dk.column, "int", False) for dk in self.dimension_keys]

    @property
    def all_columns(self) -> list[Column]:
        return ([Column(self.key, "int", False, conformance=True)]
                + self.fk_columns + self.measures + self.conformance_columns)

    def dimension_key(self, role: str) -> DimensionKey | None:
        return next((dk for dk in self.dimension_keys if dk.role == role), None)

    @property
    def staging_columns(self) -> list[Column]:
        """
        Union of every source column referenced by the FKs (typed from the
        referenced dimension's source_key attribute), the declared
        source_columns, and the measures.
        """
        seen: dict[str, Column] = {}
        for dk in self.dimension_keys:
            for src_col, dim_col in zip(dk.source, dk.dimension.source_key_columns):
                if src_col not in seen:
                    seen[src_col] = Column(src_col, dim_col.type)
        for c in [*self.source_columns, *self.measures]:
            if c.name not in seen:
                seen[c.name] = Column(c.name, c.type)
        return list(seen.values())

    def staging_column(self, name: str) -> Column | None:
        return next((c for c in self.staging_columns if c.name == name), None)


@dataclass
class Model:
    version: int
    settings: dict
    naming: dict
    dims: list[Dimension]
    facts: list[Fact]
    raw: dict = field(repr=False, default_factory=dict)

    def dim(self, name: str) -> Dimension | None:
        return next((d for d in self.dims if d.name == name), None)

    def fact(self, name: str) -> Fact | None:
        return next((f for f in self.facts if f.name == name), None)

    @property
    def roles(self) -> list[tuple[str, Dimension]]:
        """Every (role, dimension) pair used by any fact, plus each dim under its own name."""
        out: dict[str, Dimension] = {d.name: d for d in self.dims}
        for f in self.facts:
            for dk in f.dimension_keys:
                out.setdefault(dk.role, dk.dimension)
        return sorted(out.items(), key=lambda kv: (kv[1].name, kv[0] != kv[1].name, kv[0]))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

class ModelError(Exception):
    pass


COLUMN_KEYS = {"name", "type", "nullable", "scd", "expression", "mapping", "mapping_name_of"}


def _column(raw: dict, entity: str) -> Column:
    if not isinstance(raw, dict) or "name" not in raw:
        raise ModelError(f"{entity}: attribute entries must be mappings with a 'name'")
    unknown = set(raw) - COLUMN_KEYS
    if unknown:
        raise ModelError(f"{entity}.{raw['name']}: unknown key(s) {sorted(unknown)} "
                         f"(types containing commas, e.g. decimal(5,2), must be quoted in YAML flow mappings)")
    return Column(
        name=str(raw["name"]),
        type=str(raw.get("type", "")),
        nullable=bool(raw.get("nullable", True)),
        scd=raw.get("scd"),
        expression=raw.get("expression"),
        mapping=raw.get("mapping"),
        mapping_name_of=raw.get("mapping_name_of"),
    )


def load_model(path: str | Path = "model.yaml") -> Model:
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ModelError("model.yaml must be a mapping")

    settings = {
        "hash_algorithm": "SHA2_512",
        "end_of_time": "9999-12-31",
        "unknown_member_key": -1,
        "source_system_default": "SIS",
    }
    settings.update(raw.get("settings") or {})
    naming = {"allowed_acronyms": []}
    naming.update(raw.get("naming") or {})

    dims: list[Dimension] = []
    for name, d in (raw.get("dims") or {}).items():
        d = d or {}
        stg = d.get("staging") or {}
        dims.append(Dimension(
            name=str(name),
            description=str(d.get("description", "")),
            source_key=list(d.get("source_key") or []),
            source_system=str(stg.get("source_system", settings["source_system_default"])),
            source_object=str(stg.get("source_object", name)),
            staging_table=str(stg.get("table", name)),
            attributes=[_column(a, f"dim {name}") for a in (d.get("attributes") or [])],
        ))

    facts: list[Fact] = []
    for name, f in (raw.get("facts") or {}).items():
        f = f or {}
        stg = f.get("staging") or {}
        facts.append(Fact(
            name=str(name),
            description=str(f.get("description", "")),
            grain=str(f.get("grain", "")),
            large=bool(f.get("large", False)),
            partial_refresh_role=f.get("partial_refresh_role"),
            source_key=list(f.get("source_key") or []),
            source_system=str(stg.get("source_system", settings["source_system_default"])),
            source_object=str(stg.get("source_object", name)),
            staging_table=str(stg.get("table", name)),
            dimension_keys=[DimensionKey(str(k["role"]), str(k["dim"]), list(k.get("source") or []))
                            for k in (f.get("dimension_keys") or [])],
            measures=[_column(m, f"fact {name}") for m in (f.get("measures") or [])],
            index_keys=list(f.get("index_keys") or []),
            source_columns=[_column(c, f"fact {name}") for c in (f.get("source_columns") or [])],
        ))

    model = Model(int(raw.get("version", 1)), settings, naming, dims, facts, raw)

    # Bind FK -> dimension. Unknown dims stay unbound; validate.py reports them.
    for fact in model.facts:
        for dk in fact.dimension_keys:
            dk.dimension = model.dim(dk.dim)
    return model


# ---------------------------------------------------------------------------
# Naming helpers shared by generator and validator
# ---------------------------------------------------------------------------

PASCAL_RE = re.compile(r"^[A-Z][A-Za-z0-9]*$")
_TOKEN_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z]|[0-9]|$)|[A-Z][a-z0-9]*")


def pascal_tokens(name: str) -> list[str]:
    """'SCDType1HashValue' -> ['SCD', 'Type1', 'Hash', 'Value']."""
    return _TOKEN_RE.findall(name)


def role_column(role: str, dim_name: str, column: str) -> str:
    """
    Rename a dimension column for a role-playing view.

      Calendar / RegistrationDate : CalendarDate  -> RegistrationDate
                                    CalendarYear  -> RegistrationDateYear
                                    WeekdayFlag   -> RegistrationDateWeekdayFlag
      Student  / Student          : StudentCode   -> StudentCode   (identity)
      Class    / Section          : SectionNumber -> SectionNumber (already role-prefixed)

    Rule: strip the dimension-name prefix if present, then prefix with the
    role. If the role already ends with the remaining suffix (Date/Date) the
    duplicate token is collapsed.
    """
    if role == dim_name or column.startswith(role):
        return column
    rest = column[len(dim_name):] if column.startswith(dim_name) else column
    if rest and role.endswith(rest):
        return role
    return f"{role}{rest}"


def base_type(sql_type: str) -> str:
    return sql_type.split("(")[0].strip().lower()


def type_length(sql_type: str) -> int | None:
    m = re.search(r"\((\d+)", sql_type)
    return int(m.group(1)) if m else None
