"""DAX-to-SQL translation patterns for UC Metric View conversion.

Each pattern category maps a DAX regex to a SQL template or classification.
Column references in DAX use [ColumnName] or Table[ColumnName] syntax.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

Tier = Literal["direct", "translatable", "manual"]

COL_RE = r"""(?:\w+\s*)?[\['](\w[\w\s]*)[\]']"""


@dataclass(frozen=True)
class DaxPattern:
    name: str
    tier: Tier
    dax_regex: re.Pattern[str]
    sql_template: str | None
    notes: str

    def match(self, expr: str) -> re.Match[str] | None:
        return self.dax_regex.search(expr)


def _p(name: str, tier: Tier, pattern: str, sql: str | None, notes: str) -> DaxPattern:
    return DaxPattern(name, tier, re.compile(pattern, re.IGNORECASE | re.DOTALL), sql, notes)


DIRECT_PATTERNS: list[DaxPattern] = [
    _p("SUM", "direct",
       rf"\bSUM\s*\(\s*{COL_RE}\s*\)",
       "SUM({col})",
       "Direct SUM aggregate"),
    _p("COUNT", "direct",
       rf"\bCOUNT\s*\(\s*{COL_RE}\s*\)",
       "COUNT({col})",
       "Direct COUNT aggregate"),
    _p("COUNTROWS", "direct",
       r"\bCOUNTROWS\s*\(\s*(?:'?\w[\w\s]*'?)?\s*\)",
       "COUNT(1)",
       "COUNTROWS maps to COUNT(1)"),
    _p("DISTINCTCOUNT", "direct",
       rf"\bDISTINCTCOUNT\s*\(\s*{COL_RE}\s*\)",
       "COUNT(DISTINCT {col})",
       "DISTINCTCOUNT maps to COUNT(DISTINCT)"),
    _p("AVERAGE", "direct",
       rf"\bAVERAGE\s*\(\s*{COL_RE}\s*\)",
       "AVG({col})",
       "AVERAGE maps to AVG"),
    _p("MIN", "direct",
       rf"\bMIN\s*\(\s*{COL_RE}\s*\)",
       "MIN({col})",
       "Direct MIN aggregate"),
    _p("MAX", "direct",
       rf"\bMAX\s*\(\s*{COL_RE}\s*\)",
       "MAX({col})",
       "Direct MAX aggregate"),
    _p("COUNTA", "direct",
       rf"\bCOUNTA\s*\(\s*{COL_RE}\s*\)",
       "COUNT({col})",
       "COUNTA (non-blank count) maps to COUNT"),
    _p("COUNTBLANK", "direct",
       rf"\bCOUNTBLANK\s*\(\s*{COL_RE}\s*\)",
       "COUNT(1) FILTER (WHERE {col} IS NULL)",
       "COUNTBLANK maps to filtered count of NULLs"),
]

TRANSLATABLE_PATTERNS: list[DaxPattern] = [
    _p("DIVIDE", "translatable",
       r"\bDIVIDE\s*\(",
       None,
       "DIVIDE(num, den) translates to num / NULLIF(den, 0)"),
    _p("CALCULATE_FILTER", "translatable",
       r"\bCALCULATE\s*\([^,]+,\s*.+",
       None,
       "Simple CALCULATE with filter translates to aggregate FILTER (WHERE ...)"),
    _p("IF_SWITCH", "translatable",
       r"\b(IF|SWITCH)\s*\(",
       None,
       "IF/SWITCH can map to CASE WHEN in a derived dimension or filtered measure"),
    _p("TOTALYTD", "translatable",
       r"\bTOTALYTD\s*\(",
       None,
       "TOTALYTD translates to UC window measure with cumulative + current year"),
    _p("TOTALQTD", "translatable",
       r"\bTOTALQTD\s*\(",
       None,
       "TOTALQTD translates to UC window measure with cumulative + current quarter"),
    _p("TOTALMTD", "translatable",
       r"\bTOTALMTD\s*\(",
       None,
       "TOTALMTD translates to UC window measure with cumulative + current month"),
    _p("SAMEPERIODLASTYEAR", "translatable",
       r"\bSAMEPERIODLASTYEAR\s*\(",
       None,
       "SAMEPERIODLASTYEAR translates to trailing 1 year window measure"),
    _p("DATEADD", "translatable",
       r"\bDATEADD\s*\(",
       None,
       "DATEADD can translate to trailing/leading window measure"),
    _p("DATESYTD", "translatable",
       r"\bDATESYTD\s*\(",
       None,
       "DATESYTD translates to cumulative window within current year"),
    _p("FORMAT_DATE", "translatable",
       r"\bFORMAT\s*\(\s*\w+(?:\[\w+\])?\s*,\s*['\"](?:yyyy|MMMM|mm|dd)",
       None,
       "FORMAT date patterns can translate to DATE_TRUNC or date_format SQL"),
    _p("RELATED", "translatable",
       r"\bRELATED\s*\(",
       None,
       "RELATED lookups translate to join column references in metric view"),
    _p("HASONEVALUE", "translatable",
       r"\bHASONEVALUE\s*\(",
       None,
       "HASONEVALUE guard pattern can be simplified in metric context"),
    _p("ISBLANK", "translatable",
       r"\bISBLANK\s*\(",
       None,
       "ISBLANK translates to IS NULL check"),
    _p("RATIO", "translatable",
       r"\b(SUM|COUNT|DISTINCTCOUNT|AVERAGE)\s*\([^)]+\)\s*/\s*(SUM|COUNT|DISTINCTCOUNT|AVERAGE)\s*\(",
       None,
       "Ratio of two aggregates maps directly to UC ratio measure"),
]

MANUAL_PATTERNS: list[DaxPattern] = [
    _p("SUMX_ITERATOR", "manual",
       r"\b(SUMX|AVERAGEX|MINX|MAXX|COUNTX)\s*\(",
       None,
       "Iterator functions require row-by-row evaluation not available in metric view aggregates"),
    _p("ADDCOLUMNS", "manual",
       r"\bADDCOLUMNS\s*\(",
       None,
       "ADDCOLUMNS creates virtual tables -- not expressible as a metric view measure"),
    _p("GENERATE", "manual",
       r"\bGENERATE\s*\(",
       None,
       "GENERATE produces cross-joins of table expressions -- manual rewrite needed"),
    _p("USERELATIONSHIP", "manual",
       r"\bUSERELATIONSHIP\s*\(",
       None,
       "USERELATIONSHIP activates inactive relationships -- UC metric views use a fixed join model"),
    _p("ALLEXCEPT", "manual",
       r"\bALLEXCEPT\s*\(",
       None,
       "ALLEXCEPT modifies filter context -- no direct UC equivalent"),
    _p("ALL_MODIFIER", "manual",
       r"\bALL\s*\(\s*\w+",
       None,
       "ALL removes filters from context -- manual rewrite required"),
    _p("VALUES_FILTER", "manual",
       r"\bVALUES\s*\(",
       None,
       "VALUES returns distinct values as a table -- no metric view equivalent"),
    _p("CALCULATETABLE", "manual",
       r"\bCALCULATETABLE\s*\(",
       None,
       "CALCULATETABLE produces table expressions -- not a scalar aggregate"),
    _p("VAR_RETURN", "manual",
       r"\bVAR\b.*\bRETURN\b",
       None,
       "VAR/RETURN multi-step logic requires manual decomposition"),
    _p("NESTED_CALCULATE", "manual",
       r"\bCALCULATE\s*\([^)]*\bCALCULATE\s*\(",
       None,
       "Nested CALCULATE with context transition requires manual rewrite"),
    _p("EARLIER", "manual",
       r"\bEARLIER\s*\(",
       None,
       "EARLIER references outer row context -- not available in SQL aggregates"),
    _p("RANKX", "manual",
       r"\bRANKX\s*\(",
       None,
       "RANKX ranking requires SQL window functions -- not supported in metric view measures"),
    _p("TOPN", "manual",
       r"\bTOPN\s*\(",
       None,
       "TOPN table filtering requires manual SQL rewrite"),
    _p("SELECTEDVALUE", "manual",
       r"\bSELECTEDVALUE\s*\(",
       None,
       "SELECTEDVALUE depends on slicer context -- convert to dimension or parameterized filter"),
    _p("LOOKUPVALUE", "manual",
       r"\bLOOPUPVALUE\s*\(",
       None,
       "LOOKUPVALUE performs row-level lookup -- use a join instead"),
]

ALL_PATTERNS: list[DaxPattern] = MANUAL_PATTERNS + TRANSLATABLE_PATTERNS + DIRECT_PATTERNS


def extract_column_ref(expr: str) -> str | None:
    """Extract the first column reference from a DAX expression."""
    m = re.search(COL_RE, expr, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _extract_table_col_ref(expr: str) -> tuple[str, str] | None:
    """Extract Table[Column] style reference, returning (table, column)."""
    m = re.match(r"\s*'?(\w[\w\s]*?)'?\s*\[(\w[\w\s]*)\]\s*$", expr.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = re.match(r"\s*\[(\w[\w\s]*)\]\s*$", expr.strip())
    if m:
        return "", m.group(1).strip()
    return None


def _split_args(text: str) -> list[str]:
    """Split comma-separated arguments respecting nested parentheses and quotes."""
    args: list[str] = []
    depth = 0
    current: list[str] = []
    in_str: str | None = None

    for ch in text:
        if in_str:
            current.append(ch)
            if ch == in_str:
                in_str = None
            continue

        if ch in ('"', "'"):
            in_str = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    tail = "".join(current).strip()
    if tail:
        args.append(tail)
    return args


def _extract_func_args(expr: str, func_name: str) -> list[str] | None:
    """Extract the arguments of a top-level DAX function call."""
    pattern = re.compile(rf"\b{func_name}\s*\(", re.IGNORECASE)
    m = pattern.search(expr)
    if not m:
        return None
    start = m.end()
    depth = 1
    pos = start
    while pos < len(expr) and depth > 0:
        if expr[pos] == "(":
            depth += 1
        elif expr[pos] == ")":
            depth -= 1
        pos += 1
    if depth != 0:
        return None
    inner = expr[start:pos - 1]
    return _split_args(inner)


def translate_direct_aggregate(expr: str) -> tuple[str, str] | None:
    """Attempt to translate a simple DAX aggregate to SQL.

    Returns (sql_expression, pattern_name) or None if no direct match.
    """
    stripped = expr.strip()
    for pat in DIRECT_PATTERNS:
        m = pat.dax_regex.fullmatch(stripped)
        if not m:
            m = pat.dax_regex.search(stripped)
        if m and pat.sql_template:
            col = extract_column_ref(stripped) or "?"
            sql = pat.sql_template.replace("{col}", col)
            return sql, pat.name
    return None


# ---------------------------------------------------------------------------
# Translatable-pattern SQL generators
# ---------------------------------------------------------------------------

def _col_to_sql(ref: str) -> str:
    """Convert a DAX column reference to a plain SQL column name."""
    ref = ref.strip()
    tc = _extract_table_col_ref(ref)
    if tc:
        _, col = tc
        return col
    ref = ref.strip("[]' ")
    return ref


def translate_expression(expr: str) -> str | None:
    """Recursively translate a DAX expression to SQL. Returns None if not possible."""
    expr = expr.strip()
    if not expr:
        return None

    # Check for manual patterns first -- bail out
    for pat in MANUAL_PATTERNS:
        if pat.match(expr):
            return None

    # Try translatable translators FIRST (they handle compound expressions
    # that contain simple aggregates inside, e.g. DIVIDE(SUM(...), COUNT(...)))
    for name, fn in _TRANSLATORS:
        result = fn(expr)
        if result is not None:
            return result

    # Then try direct aggregate (pure simple expressions like SUM([X]))
    direct = translate_direct_aggregate(expr)
    if direct:
        return direct[0]

    # Bare column or measure reference
    if re.match(r"^\[?\w[\w\s]*\]?$", expr):
        return _col_to_sql(expr)

    # Numeric literal
    if re.match(r"^-?\d+(\.\d+)?$", expr):
        return expr

    # String literal
    if re.match(r'^"[^"]*"$', expr):
        return expr.replace('"', "'")

    return None


def _translate_divide(expr: str) -> str | None:
    args = _extract_func_args(expr, "DIVIDE")
    if not args or len(args) < 2:
        return None
    num = translate_expression(args[0])
    den = translate_expression(args[1])
    if num and den:
        return f"({num}) / NULLIF(({den}), 0)"
    # Partial: at least show the structure
    num = num or f"/* {args[0].strip()} */"
    den = den or f"/* {args[1].strip()} */"
    return f"({num}) / NULLIF(({den}), 0)"


def _translate_calculate(expr: str) -> str | None:
    args = _extract_func_args(expr, "CALCULATE")
    if not args or len(args) < 2:
        return None
    agg_sql = translate_expression(args[0])
    if not agg_sql:
        return None
    filters: list[str] = []
    for farg in args[1:]:
        fsql = _translate_filter_arg(farg.strip())
        if fsql:
            filters.append(fsql)
        else:
            filters.append(f"/* {farg.strip()} */")
    if filters:
        filter_clause = " AND ".join(filters)
        return f"{agg_sql} FILTER (WHERE {filter_clause})"
    return agg_sql


def _translate_filter_arg(farg: str) -> str | None:
    """Translate a single CALCULATE filter argument to a SQL WHERE predicate."""
    # Simple comparison: Column = "Value" or Column > 5
    m = re.match(
        r"""(?:\w+\s*)?[\['](\w[\w\s]*)[\]']\s*(=|<>|!=|>=|<=|>|<)\s*(.+)$""",
        farg.strip(), re.IGNORECASE
    )
    if m:
        col = m.group(1).strip()
        op = m.group(2)
        val = m.group(3).strip()
        if op == "<>":
            op = "!="
        return f"{col} {op} {val}"

    # FILTER(table, predicate) -- extract the predicate
    filter_args = _extract_func_args(farg, "FILTER")
    if filter_args and len(filter_args) >= 2:
        pred = _translate_filter_arg(filter_args[1])
        return pred

    return None


def _translate_if(expr: str) -> str | None:
    args = _extract_func_args(expr, "IF")
    if not args or len(args) < 2:
        return None
    cond = args[0].strip()
    true_val = _translate_value(args[1])
    false_val = _translate_value(args[2]) if len(args) > 2 else "NULL"
    if true_val is None:
        return None
    cond_sql = _translate_condition(cond)
    return f"CASE WHEN {cond_sql} THEN {true_val} ELSE {false_val or 'NULL'} END"


def _translate_value(expr: str) -> str | None:
    """Translate a DAX value expression (may be a literal, column, aggregate, or nested IF)."""
    expr = expr.strip()
    if re.match(r'^"[^"]*"$', expr):
        return expr.replace('"', "'")
    if re.match(r"^-?\d+(\.\d+)?$", expr):
        return expr
    return translate_expression(expr)


def _translate_condition(cond: str) -> str:
    """Best-effort translation of a DAX condition to SQL."""
    # ISBLANK(x) -> x IS NULL
    isblank_args = _extract_func_args(cond, "ISBLANK")
    if isblank_args:
        col = _col_to_sql(isblank_args[0])
        return f"{col} IS NULL"
    # Simple comparison
    m = re.match(
        r"""(?:\w+\s*)?[\['](\w[\w\s]*)[\]']\s*(=|<>|!=|>=|<=|>|<)\s*(.+)$""",
        cond.strip(), re.IGNORECASE
    )
    if m:
        col = m.group(1).strip()
        op = "!=" if m.group(2) == "<>" else m.group(2)
        val = m.group(3).strip()
        return f"{col} {op} {val}"
    return cond.replace("[", "").replace("]", "")


def _translate_isblank(expr: str) -> str | None:
    args = _extract_func_args(expr, "ISBLANK")
    if not args:
        return None
    col = _col_to_sql(args[0])
    return f"{col} IS NULL"


def _translate_related(expr: str) -> str | None:
    args = _extract_func_args(expr, "RELATED")
    if not args:
        return None
    tc = _extract_table_col_ref(args[0])
    if tc:
        table, col = tc
        alias = table.lower().replace(" ", "_") if table else ""
        return f"{alias}.{col}" if alias else col
    return _col_to_sql(args[0])


def _translate_ratio(expr: str) -> str | None:
    """Translate explicit agg1 / agg2 patterns where '/' appears at top level."""
    stripped = expr.strip()
    # Split on '/' at depth 0 (outside all parentheses)
    depth = 0
    split_pos = -1
    for i, ch in enumerate(stripped):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "/" and depth == 0:
            split_pos = i
            break
    if split_pos < 0:
        return None
    num_expr = stripped[:split_pos].strip()
    den_expr = stripped[split_pos + 1:].strip()
    if not num_expr or not den_expr:
        return None
    num_sql = translate_expression(num_expr)
    den_sql = translate_expression(den_expr)
    if num_sql and den_sql:
        return f"({num_sql}) / NULLIF(({den_sql}), 0)"
    return None


def _translate_totalytd(expr: str) -> str | None:
    args = _extract_func_args(expr, "TOTALYTD")
    if not args or len(args) < 2:
        return None
    inner_expr = args[0].strip()
    # The first arg is often a measure reference [MeasureName] or an aggregate
    inner = translate_expression(inner_expr)
    date_col = _col_to_sql(args[1])
    if inner:
        return f"{inner} /* TOTALYTD: cumulative year-to-date on {date_col} */"
    return None


def _translate_totalqtd(expr: str) -> str | None:
    args = _extract_func_args(expr, "TOTALQTD")
    if not args or len(args) < 2:
        return None
    inner = translate_expression(args[0])
    date_col = _col_to_sql(args[1])
    if inner:
        return f"{inner} /* TOTALQTD: apply cumulative quarter-to-date filter on {date_col} */"
    return None


def _translate_totalmtd(expr: str) -> str | None:
    args = _extract_func_args(expr, "TOTALMTD")
    if not args or len(args) < 2:
        return None
    inner = translate_expression(args[0])
    date_col = _col_to_sql(args[1])
    if inner:
        return f"{inner} /* TOTALMTD: apply cumulative month-to-date filter on {date_col} */"
    return None


def _translate_sameperiodlastyear(expr: str) -> str | None:
    """Translate CALCULATE(..., SAMEPERIODLASTYEAR(date)) patterns."""
    calc_args = _extract_func_args(expr, "CALCULATE")
    if not calc_args or len(calc_args) < 2:
        return None
    inner = translate_expression(calc_args[0])
    if not inner:
        return None
    for farg in calc_args[1:]:
        sply_args = _extract_func_args(farg.strip(), "SAMEPERIODLASTYEAR")
        if sply_args:
            date_col = _col_to_sql(sply_args[0])
            return f"{inner} /* same period last year: offset {date_col} by -1 year */"
        da_args = _extract_func_args(farg.strip(), "DATEADD")
        if da_args and len(da_args) >= 3:
            date_col = _col_to_sql(da_args[0])
            offset = da_args[1].strip()
            period = da_args[2].strip().upper()
            return f"{inner} /* DATEADD: offset {date_col} by {offset} {period} */"
    return None


def _translate_hasonevalue(expr: str) -> str | None:
    """HASONEVALUE guard patterns are often wrapping IF -- try the parent IF."""
    return None


def _translate_format_date(expr: str) -> str | None:
    args = _extract_func_args(expr, "FORMAT")
    if not args or len(args) < 2:
        return None
    col = _col_to_sql(args[0])
    fmt = args[1].strip().strip("'\"")
    sql_fmt = fmt.replace("yyyy", "%Y").replace("MMMM", "%B").replace("MM", "%m").replace("dd", "%d")
    return f"date_format({col}, '{sql_fmt}')"


def _translate_dateadd(expr: str) -> str | None:
    """Translate CALCULATE(..., DATEADD(date, -N, PERIOD))."""
    calc_args = _extract_func_args(expr, "CALCULATE")
    if not calc_args or len(calc_args) < 2:
        return None
    inner = translate_expression(calc_args[0])
    if not inner:
        return None
    for farg in calc_args[1:]:
        da_args = _extract_func_args(farg.strip(), "DATEADD")
        if da_args and len(da_args) >= 3:
            date_col = _col_to_sql(da_args[0])
            offset = da_args[1].strip()
            period = da_args[2].strip().upper()
            return f"{inner} /* DATEADD: offset {date_col} by {offset} {period} */"
    return None


_TRANSLATORS: list[tuple[str, object]] = [
    ("DIVIDE", _translate_divide),
    # Time intelligence + SPLY before generic CALCULATE so they get better annotations
    ("SAMEPERIODLASTYEAR", _translate_sameperiodlastyear),
    ("DATEADD", _translate_dateadd),
    ("CALCULATE", _translate_calculate),
    ("IF", _translate_if),
    ("ISBLANK", _translate_isblank),
    ("RELATED", _translate_related),
    ("TOTALYTD", _translate_totalytd),
    ("TOTALQTD", _translate_totalqtd),
    ("TOTALMTD", _translate_totalmtd),
    ("FORMAT_DATE", _translate_format_date),
    ("RATIO", _translate_ratio),
]
