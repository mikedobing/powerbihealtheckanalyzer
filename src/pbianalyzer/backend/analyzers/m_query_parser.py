"""Parse Power Query M expressions into structured step representations."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class MStepType(str, Enum):
    SOURCE = "source"
    NAVIGATION = "navigation"
    FILTER = "filter"
    RENAME = "rename"
    TYPE_CAST = "type_cast"
    ADD_COLUMN = "add_column"
    REMOVE_COLUMNS = "remove_columns"
    SELECT_COLUMNS = "select_columns"
    GROUP = "group"
    SORT = "sort"
    JOIN = "join"
    EXPAND = "expand"
    DISTINCT = "distinct"
    SKIP = "skip"
    FIRST_N = "first_n"
    REPLACE_VALUE = "replace_value"
    FILL = "fill"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    COMBINE = "combine"
    CUSTOM = "custom"
    SQL_QUERY = "sql_query"
    UNKNOWN = "unknown"


class SourceType(str, Enum):
    DATABRICKS_CATALOG = "databricks_catalog"
    DATABRICKS_QUERY = "databricks_query"
    SQL_SERVER = "sql_server"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    ODBC = "odbc"
    OLEDB = "oledb"
    CSV = "csv"
    EXCEL = "excel"
    WEB = "web"
    SHAREPOINT = "sharepoint"
    ODATA = "odata"
    UNKNOWN = "unknown"


@dataclass
class MStep:
    """A single named step in an M let/in expression."""
    name: str
    expression: str
    step_type: MStepType = MStepType.UNKNOWN
    details: dict = field(default_factory=dict)


@dataclass
class MSource:
    """Extracted data source information from M expression."""
    source_type: SourceType = SourceType.UNKNOWN
    hostname: str = ""
    http_path: str = ""
    catalog: str = ""
    schema: str = ""
    table: str = ""
    database: str = ""
    sql_query: str = ""
    connection_string: str = ""
    file_path: str = ""


@dataclass
class FilterCondition:
    column: str
    operator: str
    value: str


@dataclass
class RenameMapping:
    old_name: str
    new_name: str


@dataclass
class TypeCast:
    column: str
    target_type: str


@dataclass
class AddedColumn:
    name: str
    expression: str


@dataclass
class GroupAggregation:
    output_name: str
    function: str
    source_column: str


@dataclass
class JoinInfo:
    right_table_step: str
    left_key: str
    right_key: str
    join_kind: str = "inner"
    expanded_columns: list[str] = field(default_factory=list)


@dataclass
class ParsedMQuery:
    """Full parsed representation of an M expression."""
    raw_expression: str
    steps: list[MStep] = field(default_factory=list)
    source: MSource = field(default_factory=MSource)
    final_step: str = ""
    parse_warnings: list[str] = field(default_factory=list)


# --- Regex patterns for M step classification ---

_SOURCE_PATTERNS: dict[str, SourceType] = {
    r"Databricks\.Catalogs": SourceType.DATABRICKS_CATALOG,
    r"Databricks\.Query": SourceType.DATABRICKS_QUERY,
    r"Sql\.Database": SourceType.SQL_SERVER,
    r"Oracle\.Database": SourceType.ORACLE,
    r"PostgreSQL\.Database": SourceType.POSTGRES,
    r"MySQL\.Database": SourceType.MYSQL,
    r"Odbc\.DataSource": SourceType.ODBC,
    r"OleDb\.DataSource": SourceType.OLEDB,
    r"Csv\.Document": SourceType.CSV,
    r"Excel\.Workbook": SourceType.EXCEL,
    r"Web\.Contents": SourceType.WEB,
    r"SharePoint\.": SourceType.SHAREPOINT,
    r"OData\.Feed": SourceType.ODATA,
}

_STEP_CLASSIFIERS: list[tuple[str, MStepType]] = [
    (r"Table\.SelectRows", MStepType.FILTER),
    (r"Table\.RenameColumns", MStepType.RENAME),
    (r"Table\.TransformColumnTypes", MStepType.TYPE_CAST),
    (r"Table\.AddColumn", MStepType.ADD_COLUMN),
    (r"Table\.RemoveColumns", MStepType.REMOVE_COLUMNS),
    (r"Table\.SelectColumns", MStepType.SELECT_COLUMNS),
    (r"Table\.Group", MStepType.GROUP),
    (r"Table\.Sort", MStepType.SORT),
    (r"Table\.NestedJoin", MStepType.JOIN),
    (r"Table\.Join", MStepType.JOIN),
    (r"Table\.ExpandTableColumn", MStepType.EXPAND),
    (r"Table\.Distinct", MStepType.DISTINCT),
    (r"Table\.Skip", MStepType.SKIP),
    (r"Table\.FirstN", MStepType.FIRST_N),
    (r"Table\.ReplaceValue", MStepType.REPLACE_VALUE),
    (r"Table\.FillDown", MStepType.FILL),
    (r"Table\.FillUp", MStepType.FILL),
    (r"Table\.Pivot", MStepType.PIVOT),
    (r"Table\.Unpivot", MStepType.UNPIVOT),
    (r"Table\.UnpivotOtherColumns", MStepType.UNPIVOT),
    (r"Table\.Combine", MStepType.COMBINE),
]


def parse_m_expression(raw: str) -> ParsedMQuery:
    """Parse a Power Query M expression into structured steps."""
    result = ParsedMQuery(raw_expression=raw)
    text = raw.strip()

    if not text:
        return result

    let_match = re.match(
        r'(?i)let\s+(.*?)\s+in\s+(#"[^"]+"|[A-Za-z_]\w*)\s*$', text, re.DOTALL
    )
    if not let_match:
        if _detect_source_type(text) != SourceType.UNKNOWN:
            source = _extract_source_info(text)
            result.source = source
            result.steps.append(MStep(
                name="Source",
                expression=text,
                step_type=MStepType.SOURCE,
            ))
        else:
            result.parse_warnings.append("Expression is not a let/in block")
            result.steps.append(MStep(name="raw", expression=text, step_type=MStepType.UNKNOWN))
        return result

    body = let_match.group(1)
    result.final_step = let_match.group(2).strip().rstrip(",")

    steps = _split_let_body(body)
    for name, expr in steps:
        step = _classify_step(name, expr)
        result.steps.append(step)

        if step.step_type == MStepType.SOURCE:
            result.source = _extract_source_info(expr)
        elif step.step_type == MStepType.SQL_QUERY:
            result.source = _extract_source_info(expr)
        elif step.step_type == MStepType.NAVIGATION:
            _enrich_source_from_navigation(result.source, expr)

    return result


def _split_let_body(body: str) -> list[tuple[str, str]]:
    """Split a let body into (name, expression) pairs.

    Handles #"quoted names" and respects nested parentheses, quotes, and
    multi-line expressions.
    """
    steps: list[tuple[str, str]] = []
    remaining = body.strip()

    while remaining:
        remaining = remaining.strip()
        if not remaining:
            break

        name_match = re.match(
            r'(#"[^"]+"|[A-Za-z_]\w*)\s*=\s*',
            remaining,
        )
        if not name_match:
            break

        name = name_match.group(1).strip('"').lstrip("#").strip('"')
        remaining = remaining[name_match.end():]

        expr, remaining = _extract_expression(remaining)
        steps.append((name, expr.strip()))

    return steps


def _extract_expression(text: str) -> tuple[str, str]:
    """Extract a single M expression, respecting nested parens/quotes/brackets."""
    depth_paren = 0
    depth_bracket = 0
    in_string: str | None = None
    i = 0

    while i < len(text):
        ch = text[i]

        if in_string:
            if ch == in_string:
                if ch == '"' and i + 1 < len(text) and text[i + 1] == '"':
                    i += 2
                    continue
                in_string = None
            i += 1
            continue

        if ch == '"':
            in_string = '"'
        elif ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "{":
            depth_bracket += 1
        elif ch == "}":
            depth_bracket -= 1
        elif ch == "," and depth_paren == 0 and depth_bracket == 0:
            return text[:i], text[i + 1:]

        i += 1

    return text, ""


def _detect_source_type(expr: str) -> SourceType:
    for pattern, src_type in _SOURCE_PATTERNS.items():
        if re.search(pattern, expr, re.IGNORECASE):
            return src_type
    return SourceType.UNKNOWN


def _classify_step(name: str, expr: str) -> MStep:
    """Classify an M step by its expression content."""
    src_type = _detect_source_type(expr)
    if src_type != SourceType.UNKNOWN:
        if src_type == SourceType.DATABRICKS_QUERY:
            return MStep(name=name, expression=expr, step_type=MStepType.SQL_QUERY)
        return MStep(name=name, expression=expr, step_type=MStepType.SOURCE)

    nav_pattern = re.compile(
        r'\{?\s*\[\s*Name\s*=\s*'
        r'(?:"[^"]*"(?:\s*&[^,\]]*)*)'  # literal or M concatenation expression
        r'(?:\s*,\s*Kind\s*=\s*"[^"]*")?\s*\]\s*\}?\s*\[\s*\w+\s*\]',
        re.IGNORECASE,
    )
    if nav_pattern.search(expr):
        return MStep(name=name, expression=expr, step_type=MStepType.NAVIGATION)

    for pattern, step_type in _STEP_CLASSIFIERS:
        if re.search(pattern, expr, re.IGNORECASE):
            details = _extract_step_details(step_type, expr)
            return MStep(name=name, expression=expr, step_type=step_type, details=details)

    if re.search(r"each\s+", expr) and not re.search(r"Table\.\w+", expr):
        return MStep(name=name, expression=expr, step_type=MStepType.CUSTOM)

    return MStep(name=name, expression=expr, step_type=MStepType.UNKNOWN)


def _extract_source_info(expr: str) -> MSource:
    """Extract connection details from a source step expression."""
    source = MSource()
    source.source_type = _detect_source_type(expr)

    if source.source_type == SourceType.DATABRICKS_CATALOG:
        host_match = re.search(r'Databricks\.Catalogs\s*\(\s*"([^"]*)"', expr)
        if host_match:
            source.hostname = host_match.group(1)
        else:
            var_match = re.search(r'Databricks\.Catalogs\s*\(\s*([A-Za-z_]\w*)', expr)
            if var_match:
                source.hostname = f"${{{var_match.group(1)}}}"

        path_match = re.search(r'Databricks\.Catalogs\s*\(\s*"[^"]*"\s*,\s*"([^"]*)"', expr)
        if path_match:
            source.http_path = path_match.group(1)
        elif not path_match:
            var_path = re.search(r'Databricks\.Catalogs\s*\([^,]+,\s*([A-Za-z_]\w*)', expr)
            if var_path:
                source.http_path = f"${{{var_path.group(1)}}}"

        cat_match = re.search(r'\[Catalog\s*=\s*"([^"]*)"', expr)
        if cat_match:
            source.catalog = cat_match.group(1)

    elif source.source_type == SourceType.DATABRICKS_QUERY:
        host_match = re.search(r'Databricks\.Query\s*\(\s*"([^"]*)"', expr)
        if host_match:
            source.hostname = host_match.group(1)
        path_match = re.search(r'Databricks\.Query\s*\(\s*"[^"]*"\s*,\s*"([^"]*)"', expr)
        if path_match:
            source.http_path = path_match.group(1)
        sql_match = re.search(
            r'Databricks\.Query\s*\(\s*"[^"]*"\s*,\s*"[^"]*"\s*,\s*"((?:[^"\\]|"")*)"',
            expr,
        )
        if sql_match:
            source.sql_query = sql_match.group(1).replace('""', '"')

    elif source.source_type in (SourceType.SQL_SERVER, SourceType.POSTGRES, SourceType.MYSQL, SourceType.ORACLE):
        host_match = re.search(r'\w+\.Database\s*\(\s*"([^"]*)"', expr)
        if host_match:
            source.hostname = host_match.group(1)
        db_match = re.search(r'\w+\.Database\s*\(\s*"[^"]*"\s*,\s*"([^"]*)"', expr)
        if db_match:
            source.database = db_match.group(1)

    return source


def _enrich_source_from_navigation(source: MSource, expr: str) -> None:
    """Extract catalog/schema/table from navigation steps like Source{[Name="x"]}[Data].

    Handles parameterized M expressions like Name="prefix" & variable & "suffix"
    by extracting and joining the literal string parts.
    """
    name_value_match = re.search(r'\[\s*Name\s*=\s*("(?:[^"]*)"(?:\s*&\s*(?:"[^"]*"|[^,\]]+))*)', expr)
    if not name_value_match:
        return

    raw_value = name_value_match.group(1)

    literals = re.findall(r'"([^"]*)"', raw_value)
    has_variable = bool(re.search(r'&\s*[A-Za-z_]\w*\s*', raw_value))

    if has_variable:
        var_names = re.findall(r'&\s*([A-Za-z_]\w*)\s*', raw_value)
        value = ""
        for part in re.split(r'&', raw_value):
            part = part.strip()
            str_match = re.match(r'"([^"]*)"', part)
            if str_match:
                value += str_match.group(1)
            else:
                value += f"${{{part.strip()}}}"
    elif literals:
        value = literals[0]
    else:
        return

    kind_match = re.search(r'Kind\s*=\s*"([^"]*)"', expr)
    kind = kind_match.group(1).lower() if kind_match else ""

    if kind == "database":
        if not source.catalog:
            source.catalog = value
        elif not source.schema:
            source.schema = value
    elif kind == "schema":
        source.schema = value
    elif kind == "table" or kind == "view":
        source.table = value
    elif not kind:
        if not source.catalog:
            source.catalog = value
        elif source.catalog and not source.schema:
            source.schema = value
        elif source.catalog and source.schema and not source.table:
            source.table = value


def _extract_step_details(step_type: MStepType, expr: str) -> dict:
    """Extract structured details from common M step types."""
    details: dict = {}

    if step_type == MStepType.FILTER:
        cond = _extract_each_body(expr)
        if cond:
            details["condition"] = cond
            parsed = _parse_filter_condition(cond)
            if parsed:
                details["column"] = parsed.column
                details["operator"] = parsed.operator
                details["value"] = parsed.value

    elif step_type == MStepType.RENAME:
        renames = _extract_rename_pairs(expr)
        if renames:
            details["renames"] = [{"old": r.old_name, "new": r.new_name} for r in renames]

    elif step_type == MStepType.TYPE_CAST:
        casts = _extract_type_casts(expr)
        if casts:
            details["casts"] = [{"column": c.column, "type": c.target_type} for c in casts]

    elif step_type == MStepType.ADD_COLUMN:
        col_info = _extract_added_column(expr)
        if col_info:
            details["column_name"] = col_info.name
            details["expression"] = col_info.expression

    elif step_type == MStepType.REMOVE_COLUMNS or step_type == MStepType.SELECT_COLUMNS:
        cols = _extract_column_list(expr)
        if cols:
            details["columns"] = cols

    elif step_type == MStepType.GROUP:
        group_info = _extract_group_info(expr)
        details.update(group_info)

    elif step_type == MStepType.SORT:
        sort_info = _extract_sort_info(expr)
        details.update(sort_info)

    elif step_type == MStepType.JOIN:
        join_info = _extract_join_info(expr)
        if join_info:
            details["right_table"] = join_info.right_table_step
            details["left_key"] = join_info.left_key
            details["right_key"] = join_info.right_key
            details["join_kind"] = join_info.join_kind

    elif step_type == MStepType.EXPAND:
        expanded = _extract_expanded_columns(expr)
        if expanded:
            details["expanded_columns"] = expanded

    return details


def _extract_each_body(expr: str) -> str | None:
    """Extract the body of 'each ...' from a Table.SelectRows call."""
    match = re.search(r"each\s+(.+?)(?:\)\s*$|\)\s*,)", expr, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"each\s+(.+)", expr, re.DOTALL)
    if match:
        body = match.group(1).strip().rstrip(")")
        return body
    return None


def _parse_filter_condition(cond: str) -> FilterCondition | None:
    """Parse a simple M filter condition like [Col] <> value."""
    m = re.match(
        r'\[(\w+)\]\s*(=|<>|>|<|>=|<=)\s*(.+)',
        cond.strip(),
    )
    if m:
        return FilterCondition(column=m.group(1), operator=m.group(2), value=m.group(3).strip())
    return None


def _extract_rename_pairs(expr: str) -> list[RenameMapping]:
    """Extract rename pairs from Table.RenameColumns(source, {{old, new}, ...})."""
    pairs: list[RenameMapping] = []
    for m in re.finditer(r'\{\s*"([^"]+)"\s*,\s*"([^"]+)"\s*\}', expr):
        pairs.append(RenameMapping(old_name=m.group(1), new_name=m.group(2)))
    return pairs


def _extract_type_casts(expr: str) -> list[TypeCast]:
    """Extract type cast info from Table.TransformColumnTypes."""
    casts: list[TypeCast] = []
    for m in re.finditer(r'\{\s*"([^"]+)"\s*,\s*(type\s+\w+|Int64\.Type|Currency\.Type|Percentage\.Type)', expr):
        col = m.group(1)
        raw_type = m.group(2).strip()
        casts.append(TypeCast(column=col, target_type=_m_type_to_sql(raw_type)))
    return casts


def _m_type_to_sql(m_type: str) -> str:
    """Map M type names to SQL types."""
    mapping = {
        "type text": "STRING",
        "type number": "DOUBLE",
        "type date": "DATE",
        "type datetime": "TIMESTAMP",
        "type datetimezone": "TIMESTAMP",
        "type time": "STRING",
        "type logical": "BOOLEAN",
        "type binary": "BINARY",
        "Int64.Type": "BIGINT",
        "Currency.Type": "DECIMAL(19,4)",
        "Percentage.Type": "DOUBLE",
    }
    return mapping.get(m_type.strip(), "STRING")


def _extract_added_column(expr: str) -> AddedColumn | None:
    """Extract column name and expression from Table.AddColumn."""
    m = re.search(r'Table\.AddColumn\s*\([^,]+,\s*"([^"]+)"\s*,\s*each\s+(.+?)(?:\)\s*$|\)\s*,)', expr, re.DOTALL)
    if not m:
        m = re.search(r'Table\.AddColumn\s*\([^,]+,\s*"([^"]+)"\s*,\s*each\s+(.+)', expr, re.DOTALL)
    if m:
        name = m.group(1)
        body = m.group(2).strip().rstrip(")")
        return AddedColumn(name=name, expression=body)
    return None


def _extract_column_list(expr: str) -> list[str]:
    """Extract column names from Table.RemoveColumns/SelectColumns."""
    cols: list[str] = []
    for m in re.finditer(r'"([^"]+)"', expr):
        val = m.group(1)
        if not val.startswith("Table.") and not val.startswith("type "):
            cols.append(val)
    return cols


def _extract_group_info(expr: str) -> dict:
    """Extract grouping columns and aggregations from Table.Group."""
    info: dict = {"group_columns": [], "aggregations": []}
    group_cols_match = re.search(r'Table\.Group\s*\([^,]+,\s*\{([^}]*)\}', expr)
    if group_cols_match:
        for m in re.finditer(r'"([^"]+)"', group_cols_match.group(1)):
            info["group_columns"].append(m.group(1))

    for m in re.finditer(
        r'\{\s*"([^"]+)"\s*,\s*each\s+(List\.\w+|Table\.\w+)\s*\(\s*\[(\w+)\]',
        expr,
    ):
        info["aggregations"].append({
            "name": m.group(1),
            "function": m.group(2),
            "column": m.group(3),
        })

    return info


def _extract_sort_info(expr: str) -> dict:
    """Extract sort columns and directions from Table.Sort."""
    info: dict = {"sort_columns": []}
    for m in re.finditer(r'\{\s*"([^"]+)"\s*,\s*Order\.(Ascending|Descending)', expr):
        info["sort_columns"].append({
            "column": m.group(1),
            "direction": "ASC" if m.group(2) == "Ascending" else "DESC",
        })
    if not info["sort_columns"]:
        for m in re.finditer(r'"([^"]+)"', expr):
            if m.group(1) not in ("Table.Sort",):
                info["sort_columns"].append({"column": m.group(1), "direction": "ASC"})
    return info


def _extract_join_info(expr: str) -> JoinInfo | None:
    """Extract join details from Table.NestedJoin or Table.Join."""
    kind_match = re.search(r'JoinKind\.(\w+)', expr)
    join_kind = kind_match.group(1).lower() if kind_match else "inner"
    kind_map = {
        "inner": "inner",
        "leftouter": "left",
        "rightouter": "right",
        "fullouter": "full",
        "leftanti": "left anti",
        "righanti": "right anti",
    }
    join_kind = kind_map.get(join_kind, join_kind)

    parts = re.findall(r'(?:#"[^"]+"|[A-Za-z_]\w+)', expr)
    right_table = ""
    if len(parts) >= 3:
        right_table = parts[2].strip('"').lstrip("#").strip('"') if parts[2].startswith('#') else parts[2]

    key_matches = re.findall(r'\{\s*"(\w+)"\s*\}', expr)
    left_key = key_matches[0] if len(key_matches) >= 1 else ""
    right_key = key_matches[1] if len(key_matches) >= 2 else left_key

    if right_table:
        return JoinInfo(
            right_table_step=right_table,
            left_key=left_key,
            right_key=right_key,
            join_kind=join_kind,
        )
    return None


def _extract_expanded_columns(expr: str) -> list[str]:
    """Extract column names from Table.ExpandTableColumn."""
    cols: list[str] = []
    brace_match = re.search(r'Table\.ExpandTableColumn\s*\([^,]+,\s*"[^"]*"\s*,\s*\{([^}]*)\}', expr)
    if brace_match:
        for m in re.finditer(r'"([^"]+)"', brace_match.group(1)):
            cols.append(m.group(1))
    return cols
