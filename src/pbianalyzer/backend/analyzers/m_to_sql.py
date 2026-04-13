"""Translate parsed M Query steps to SQL fragments for SDP pipeline generation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .m_query_parser import (
    MSource,
    MStep,
    MStepType,
    ParsedMQuery,
    SourceType,
)


class ConversionTier:
    AUTO = "auto"
    PARTIAL = "partial"
    MANUAL = "manual"


@dataclass
class SQLFragment:
    """A SQL clause fragment generated from an M step."""
    step_name: str
    step_type: MStepType
    clause_type: str  # "select", "where", "group_by", "order_by", "join", "cast", "rename"
    sql: str
    tier: str = ConversionTier.AUTO
    notes: str = ""
    original_m: str = ""


@dataclass
class TranslatedQuery:
    """Full SQL query built from translated M steps."""
    source_table: str = ""
    source_fqn: str = ""
    select_columns: list[str] = field(default_factory=list)
    aliases: dict[str, str] = field(default_factory=dict)
    casts: dict[str, str] = field(default_factory=dict)
    where_clauses: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    joins: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    added_columns: list[tuple[str, str]] = field(default_factory=list)
    distinct: bool = False
    limit: int | None = None
    offset: int | None = None
    raw_sql: str = ""
    fragments: list[SQLFragment] = field(default_factory=list)
    tier: str = ConversionTier.AUTO
    warnings: list[str] = field(default_factory=list)
    manual_steps: list[str] = field(default_factory=list)


# --- M expression to SQL translators ---

_M_OPERATOR_MAP = {
    "=": "=",
    "<>": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
}

_M_LIST_FUNC_TO_SQL = {
    "List.Sum": "SUM",
    "List.Average": "AVG",
    "List.Min": "MIN",
    "List.Max": "MAX",
    "List.Count": "COUNT",
    "List.Distinct": "DISTINCT",
    "Table.RowCount": "COUNT(*)",
}


def translate_m_query(parsed: ParsedMQuery) -> TranslatedQuery:
    """Translate a parsed M query into a TranslatedQuery with SQL fragments."""
    result = TranslatedQuery()

    if parsed.source.source_type == SourceType.DATABRICKS_QUERY:
        result.raw_sql = parsed.source.sql_query
        result.source_fqn = ""
        result.tier = ConversionTier.AUTO
        result.fragments.append(SQLFragment(
            step_name="Source",
            step_type=MStepType.SQL_QUERY,
            clause_type="raw_sql",
            sql=parsed.source.sql_query,
            tier=ConversionTier.AUTO,
            notes="Direct SQL passthrough from Databricks.Query",
        ))
        return result

    source_fqn = _build_source_fqn(parsed.source)
    result.source_table = parsed.source.table or _last_part(source_fqn)
    result.source_fqn = source_fqn

    if parsed.source.source_type not in (
        SourceType.DATABRICKS_CATALOG,
        SourceType.DATABRICKS_QUERY,
    ):
        if parsed.source.source_type != SourceType.UNKNOWN:
            result.tier = ConversionTier.PARTIAL
            result.warnings.append(
                f"Non-Databricks source: {parsed.source.source_type.value}. "
                f"Table must be ingested into Unity Catalog first."
            )

    for step in parsed.steps:
        if step.step_type in (MStepType.SOURCE, MStepType.NAVIGATION):
            continue
        fragment = _translate_step(step, parsed)
        if fragment:
            result.fragments.append(fragment)
            _apply_fragment(result, fragment)

    if any(f.tier == ConversionTier.MANUAL for f in result.fragments):
        result.tier = ConversionTier.MANUAL
    elif any(f.tier == ConversionTier.PARTIAL for f in result.fragments):
        result.tier = ConversionTier.PARTIAL

    return result


def build_sql_statement(translated: TranslatedQuery) -> str:
    """Build a complete SQL SELECT statement from translated fragments."""
    if translated.raw_sql:
        return translated.raw_sql.strip()

    if not translated.source_fqn:
        return ""

    select_parts: list[str] = []
    if translated.aggregations and translated.group_by:
        select_parts.extend(translated.group_by)
        select_parts.extend(translated.aggregations)
    else:
        if translated.select_columns:
            for col in translated.select_columns:
                if col in translated.removed_columns:
                    continue
                alias = translated.aliases.get(col)
                cast = translated.casts.get(col)
                expr = col
                if cast:
                    expr = f"CAST({col} AS {cast})"
                if alias:
                    expr = f"{expr} AS {alias}"
                select_parts.append(expr)
        else:
            has_transforms = translated.aliases or translated.casts or translated.removed_columns
            if has_transforms:
                select_parts.append("*")
            for old_name, new_name in translated.aliases.items():
                select_parts.append(f"{_safe_name(old_name)} AS {new_name}")
            for col_name, cast_type in translated.casts.items():
                if col_name not in translated.aliases:
                    select_parts.append(f"CAST({_safe_name(col_name)} AS {cast_type}) AS {_safe_name(col_name)}")

        if translated.added_columns:
            for col_name, col_expr in translated.added_columns:
                sql_expr = _m_each_expr_to_sql(col_expr)
                select_parts.append(f"{sql_expr} AS {_safe_name(col_name)}")

    if not select_parts:
        select_clause = "  *"
    else:
        lines = [f"  {p}" for p in select_parts]
        select_clause = ",\n".join(lines)

    distinct = "DISTINCT\n" if translated.distinct else ""
    sql = f"SELECT\n{distinct}{select_clause}\nFROM {translated.source_fqn}"

    for join_clause in translated.joins:
        sql += f"\n{join_clause}"

    if translated.where_clauses:
        sql += "\nWHERE " + "\n  AND ".join(translated.where_clauses)

    if translated.group_by and translated.aggregations:
        sql += "\nGROUP BY " + ", ".join(translated.group_by)

    if translated.order_by:
        sql += "\nORDER BY " + ", ".join(translated.order_by)

    if translated.limit is not None:
        sql += f"\nLIMIT {translated.limit}"
    if translated.offset is not None:
        sql += f"\nOFFSET {translated.offset}"

    return sql


def _build_source_fqn(source: MSource) -> str:
    """Build a fully-qualified table name from source info."""
    parts = []
    if source.catalog:
        parts.append(source.catalog)
    if source.schema:
        parts.append(source.schema)
    if source.table:
        parts.append(source.table)
    if parts:
        return ".".join(parts)
    if source.database:
        return source.database
    return ""


def _last_part(fqn: str) -> str:
    if "." in fqn:
        return fqn.rsplit(".", 1)[-1]
    return fqn


def _translate_step(step: MStep, parsed: ParsedMQuery) -> SQLFragment | None:
    """Translate a single M step to a SQL fragment."""
    translators = {
        MStepType.FILTER: _translate_filter,
        MStepType.RENAME: _translate_rename,
        MStepType.TYPE_CAST: _translate_type_cast,
        MStepType.ADD_COLUMN: _translate_add_column,
        MStepType.REMOVE_COLUMNS: _translate_remove_columns,
        MStepType.SELECT_COLUMNS: _translate_select_columns,
        MStepType.GROUP: _translate_group,
        MStepType.SORT: _translate_sort,
        MStepType.JOIN: _translate_join,
        MStepType.EXPAND: _translate_expand,
        MStepType.DISTINCT: _translate_distinct,
        MStepType.FIRST_N: _translate_first_n,
        MStepType.SKIP: _translate_skip,
        MStepType.REPLACE_VALUE: _translate_replace_value,
    }

    translator = translators.get(step.step_type)
    if translator:
        return translator(step, parsed)

    manual_types = {
        MStepType.PIVOT, MStepType.UNPIVOT, MStepType.COMBINE,
        MStepType.FILL, MStepType.CUSTOM, MStepType.UNKNOWN,
    }
    if step.step_type in manual_types:
        return SQLFragment(
            step_name=step.name,
            step_type=step.step_type,
            clause_type="manual",
            sql="",
            tier=ConversionTier.MANUAL,
            notes=f"M step '{step.step_type.value}' requires manual conversion",
            original_m=step.expression,
        )

    return None


def _translate_filter(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    details = step.details
    if "column" in details and "operator" in details:
        col = details["column"]
        op = _M_OPERATOR_MAP.get(details["operator"], details["operator"])
        val = details["value"]
        if not _is_literal(val):
            val = f"'{val}'"
        sql = f"{_safe_name(col)} {op} {val}"
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="where", sql=sql,
            notes="Filter condition", original_m=step.expression,
        )

    condition = details.get("condition", "")
    if condition:
        sql = _m_condition_to_sql(condition)
        if sql:
            return SQLFragment(
                step_name=step.name, step_type=step.step_type,
                clause_type="where", sql=sql,
                notes="Filter condition (complex)", original_m=step.expression,
            )

    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="where", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Complex filter -- review M expression",
        original_m=step.expression,
    )


def _translate_rename(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    renames = step.details.get("renames", [])
    if renames:
        parts = [f'{_safe_name(r["old"])} AS {_safe_name(r["new"])}' for r in renames]
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="rename", sql="; ".join(parts),
            notes=f"Rename {len(renames)} column(s)", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="rename", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Could not parse rename pairs", original_m=step.expression,
    )


def _translate_type_cast(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    casts = step.details.get("casts", [])
    if casts:
        parts = [f'CAST({_safe_name(c["column"])} AS {c["type"]})' for c in casts]
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="cast", sql="; ".join(parts),
            notes=f"Type cast {len(casts)} column(s)", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="cast", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Could not parse type casts", original_m=step.expression,
    )


def _translate_add_column(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    col_name = step.details.get("column_name", "")
    col_expr = step.details.get("expression", "")
    if col_name and col_expr:
        sql_expr = _m_each_expr_to_sql(col_expr)
        if sql_expr:
            return SQLFragment(
                step_name=step.name, step_type=step.step_type,
                clause_type="select", sql=f"{sql_expr} AS {_safe_name(col_name)}",
                notes=f"Computed column: {col_name}", original_m=step.expression,
            )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="select", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Complex computed column -- review expression",
        original_m=step.expression,
    )


def _translate_remove_columns(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    cols = step.details.get("columns", [])
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="remove", sql=", ".join(cols),
        notes=f"Remove {len(cols)} column(s)", original_m=step.expression,
    )


def _translate_select_columns(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    cols = step.details.get("columns", [])
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="select_cols", sql=", ".join(_safe_name(c) for c in cols),
        notes=f"Select {len(cols)} column(s)", original_m=step.expression,
    )


def _translate_group(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    group_cols = step.details.get("group_columns", [])
    aggs = step.details.get("aggregations", [])
    agg_parts = []
    for agg in aggs:
        sql_func = _M_LIST_FUNC_TO_SQL.get(agg.get("function", ""), "")
        col = agg.get("column", "")
        name = agg.get("name", "")
        if sql_func and col:
            if sql_func == "COUNT(*)":
                agg_parts.append(f"COUNT(*) AS {_safe_name(name)}")
            else:
                agg_parts.append(f"{sql_func}({_safe_name(col)}) AS {_safe_name(name)}")
    if group_cols:
        sql = f"GROUP BY {', '.join(_safe_name(c) for c in group_cols)}"
        if agg_parts:
            sql += f" | AGGS: {', '.join(agg_parts)}"
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="group_by", sql=sql,
            notes="Grouped aggregation", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="group_by", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Could not parse grouping", original_m=step.expression,
    )


def _translate_sort(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    sort_cols = step.details.get("sort_columns", [])
    if sort_cols:
        parts = [f'{_safe_name(s["column"])} {s["direction"]}' for s in sort_cols]
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="order_by", sql=", ".join(parts),
            notes=f"Sort by {len(sort_cols)} column(s)", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="order_by", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Could not parse sort", original_m=step.expression,
    )


def _translate_join(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    right = step.details.get("right_table", "")
    left_key = step.details.get("left_key", "")
    right_key = step.details.get("right_key", "")
    join_kind = step.details.get("join_kind", "inner")

    join_type_map = {
        "inner": "INNER JOIN",
        "left": "LEFT JOIN",
        "right": "RIGHT JOIN",
        "full": "FULL OUTER JOIN",
        "leftouter": "LEFT JOIN",
        "rightouter": "RIGHT JOIN",
        "fullouter": "FULL OUTER JOIN",
    }
    sql_join = join_type_map.get(join_kind, "LEFT JOIN")

    if right and left_key:
        alias = _safe_name(right).lower()
        on_clause = f"{alias}.{_safe_name(right_key)} = source.{_safe_name(left_key)}"
        sql = f"{sql_join} {{{{catalog}}}}.{{{{schema}}}}.{_safe_name(right)} AS {alias} ON {on_clause}"
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="join", sql=sql,
            notes=f"{sql_join} with {right}", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="join", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Complex join -- review M expression",
        original_m=step.expression,
    )


def _translate_expand(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    cols = step.details.get("expanded_columns", [])
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="select", sql=", ".join(_safe_name(c) for c in cols),
        notes=f"Expand {len(cols)} column(s) from joined table",
        original_m=step.expression,
    )


def _translate_distinct(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="distinct", sql="DISTINCT",
        notes="Distinct rows", original_m=step.expression,
    )


def _translate_first_n(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    n_match = re.search(r"Table\.FirstN\s*\([^,]+,\s*(\d+)", step.expression)
    n = int(n_match.group(1)) if n_match else 0
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="limit", sql=f"LIMIT {n}" if n else "",
        notes=f"First {n} rows", original_m=step.expression,
    )


def _translate_skip(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    n_match = re.search(r"Table\.Skip\s*\([^,]+,\s*(\d+)", step.expression)
    n = int(n_match.group(1)) if n_match else 0
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="offset", sql=f"OFFSET {n}" if n else "",
        notes=f"Skip {n} rows", original_m=step.expression,
    )


def _translate_replace_value(step: MStep, _parsed: ParsedMQuery) -> SQLFragment:
    m = re.search(
        r'Table\.ReplaceValue\s*\([^,]+,\s*([^,]+),\s*([^,]+),\s*Replacer\.\w+\s*,\s*\{\s*"([^"]+)"',
        step.expression,
    )
    if m:
        old_val = m.group(1).strip()
        new_val = m.group(2).strip()
        col = m.group(3)
        if old_val.lower() == "null":
            sql = f"COALESCE({_safe_name(col)}, {new_val})"
        else:
            sql = f"CASE WHEN {_safe_name(col)} = {old_val} THEN {new_val} ELSE {_safe_name(col)} END"
        return SQLFragment(
            step_name=step.name, step_type=step.step_type,
            clause_type="select", sql=f"{sql} AS {_safe_name(col)}",
            notes=f"Replace value in {col}", original_m=step.expression,
        )
    return SQLFragment(
        step_name=step.name, step_type=step.step_type,
        clause_type="select", sql="",
        tier=ConversionTier.PARTIAL,
        notes="Complex replace -- review M expression", original_m=step.expression,
    )


def _apply_fragment(result: TranslatedQuery, fragment: SQLFragment) -> None:
    """Apply a translated fragment to the running TranslatedQuery state."""
    if fragment.clause_type == "where" and fragment.sql:
        result.where_clauses.append(fragment.sql)

    elif fragment.clause_type == "rename":
        for pair in fragment.sql.split("; "):
            parts = pair.split(" AS ")
            if len(parts) == 2:
                result.aliases[parts[0].strip()] = parts[1].strip()

    elif fragment.clause_type == "cast":
        for pair in fragment.sql.split("; "):
            m = re.match(r"CAST\((\w+) AS (.+)\)", pair.strip())
            if m:
                result.casts[m.group(1)] = m.group(2)

    elif fragment.clause_type == "remove":
        result.removed_columns.extend(c.strip() for c in fragment.sql.split(","))

    elif fragment.clause_type == "select_cols":
        result.select_columns = [c.strip() for c in fragment.sql.split(",")]

    elif fragment.clause_type == "select" and fragment.sql:
        m = re.match(r"(.+) AS (.+)", fragment.sql)
        if m:
            result.added_columns.append((m.group(2).strip(), m.group(1).strip()))

    elif fragment.clause_type == "group_by":
        group_match = re.match(r"GROUP BY (.+?)(?:\s*\|\s*AGGS:\s*(.+))?$", fragment.sql)
        if group_match:
            result.group_by = [c.strip() for c in group_match.group(1).split(",")]
            if group_match.group(2):
                result.aggregations = [a.strip() for a in group_match.group(2).split(",")]

    elif fragment.clause_type == "order_by" and fragment.sql:
        result.order_by = [c.strip() for c in fragment.sql.split(",")]

    elif fragment.clause_type == "join" and fragment.sql:
        result.joins.append(fragment.sql)

    elif fragment.clause_type == "distinct":
        result.distinct = True

    elif fragment.clause_type == "limit":
        m = re.search(r"LIMIT\s+(\d+)", fragment.sql)
        if m:
            result.limit = int(m.group(1))

    elif fragment.clause_type == "offset":
        m = re.search(r"OFFSET\s+(\d+)", fragment.sql)
        if m:
            result.offset = int(m.group(1))

    if fragment.tier == ConversionTier.MANUAL:
        result.manual_steps.append(fragment.step_name)


# --- Helper utilities ---

def _safe_name(name: str) -> str:
    """Wrap name in backticks if it contains spaces or special chars."""
    if not name:
        return name
    if re.match(r"^[a-zA-Z_]\w*$", name):
        return name
    return f"`{name}`"


def _is_literal(val: str) -> bool:
    """Check if a value looks like an M literal (number, string, null, bool)."""
    val = val.strip()
    if val.lower() in ("null", "true", "false"):
        return True
    if re.match(r"^-?\d+(\.\d+)?$", val):
        return True
    if val.startswith('"') and val.endswith('"'):
        return True
    return False


def _m_condition_to_sql(cond: str) -> str | None:
    """Translate an M condition (from 'each ...') to SQL WHERE clause."""
    sql = cond
    sql = re.sub(r"\[(\w+)\]", r"\1", sql)
    sql = sql.replace(" and ", " AND ").replace(" or ", " OR ")
    sql = sql.replace("<>", "!=")
    sql = re.sub(r'\bnull\b', "NULL", sql, flags=re.IGNORECASE)
    sql = sql.replace("= NULL", "IS NULL").replace("!= NULL", "IS NOT NULL")
    sql = re.sub(r"<> null", "IS NOT NULL", sql, flags=re.IGNORECASE)
    sql = sql.replace("= null", "IS NULL")
    return sql


def _m_each_expr_to_sql(expr: str) -> str | None:
    """Translate an M 'each' expression body to a SQL expression."""
    sql = expr.strip()
    sql = re.sub(r"\[(\w+)\]", r"\1", sql)
    sql = sql.replace(" & ", " || ")
    sql = sql.replace("Text.Upper", "UPPER")
    sql = sql.replace("Text.Lower", "LOWER")
    sql = sql.replace("Text.Trim", "TRIM")
    sql = sql.replace("Text.Start", "LEFT")
    sql = sql.replace("Text.End", "RIGHT")
    sql = sql.replace("Text.Length", "LENGTH")
    sql = sql.replace("Number.Round", "ROUND")
    sql = sql.replace("Number.Abs", "ABS")
    sql = sql.replace("Date.Year", "YEAR")
    sql = sql.replace("Date.Month", "MONTH")
    sql = sql.replace("Date.Day", "DAY")
    sql = re.sub(r"if\s+(.+?)\s+then\s+(.+?)\s+else\s+(.+)", r"CASE WHEN \1 THEN \2 ELSE \3 END", sql)
    return sql
