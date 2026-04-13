"""Parse TMDL (Tabular Model Definition Language) files.

TMDL is an indentation-based text format used in Power BI Project (PBIP) files.
Each table gets its own .tmdl file; relationships, model metadata, data sources,
and expressions each have a root-level file.

Reference: https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .models import (
    Column,
    CrossFilterDirection,
    DataSource,
    Measure,
    Partition,
    Relationship,
    StorageMode,
    Table,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OBJECT_KEYWORDS = frozenset({
    "database", "model", "table", "column", "measure", "partition",
    "hierarchy", "level", "role", "relationship", "expression",
    "annotation", "dataSource", "datasource", "perspectiveTable",
    "perspectiveMeasure", "tablePermission", "calculationItem",
    "calculationGroup",
})

_STORAGE_MODE_MAP = {
    "import": StorageMode.IMPORT,
    "directquery": StorageMode.DIRECT_QUERY,
    "dual": StorageMode.DUAL,
    "default": StorageMode.DEFAULT,
}

_CROSSFILTER_MAP = {
    "onedirection": CrossFilterDirection.ONE,
    "single": CrossFilterDirection.ONE,
    "bothdirections": CrossFilterDirection.BOTH,
    "both": CrossFilterDirection.BOTH,
    "automatic": CrossFilterDirection.AUTOMATIC,
}


def _indent_level(line: str) -> int:
    """Count leading tabs (TMDL standard indent unit)."""
    count = 0
    for ch in line:
        if ch == "\t":
            count += 1
        elif ch == " ":
            count += 1
        else:
            break
    # TMDL uses tabs, but some serialisers emit spaces. Normalise: 4 spaces = 1 tab.
    raw_spaces = len(line) - len(line.lstrip())
    tabs = line[:raw_spaces].count("\t")
    spaces = raw_spaces - tabs
    return tabs + spaces // 4


def _unquote_name(raw: str) -> str:
    """Remove surrounding single-quotes and unescape doubled quotes."""
    raw = raw.strip()
    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        raw = raw[1:-1].replace("''", "'")
    return raw


def _split_declaration(line: str) -> tuple[str, str, str]:
    """Split an object declaration line into (keyword, name, default_expr).

    Examples:
        "table Sales"                   -> ("table", "Sales", "")
        "measure 'Sales Amount' = SUM(…)"  -> ("measure", "Sales Amount", "SUM(…)")
        "partition P1 = m"              -> ("partition", "P1", "m")
        "column 'Product Key'"          -> ("column", "Product Key", "")
    """
    stripped = line.strip()

    # Split off keyword (first token)
    parts = stripped.split(None, 1)
    if not parts:
        return ("", "", "")
    keyword = parts[0].lower()
    rest = parts[1] if len(parts) > 1 else ""

    # Check for default expression after '='
    # But be careful: the name itself can contain '=' inside quotes
    name = ""
    default_expr = ""

    if "'" in rest:
        # Quoted name: find matching close quote ('' is escape)
        if rest.startswith("'"):
            i = 1
            while i < len(rest):
                if rest[i] == "'":
                    if i + 1 < len(rest) and rest[i + 1] == "'":
                        i += 2
                        continue
                    # End of quoted name
                    name = rest[1:i].replace("''", "'")
                    remainder = rest[i + 1:].strip()
                    if remainder.startswith("="):
                        default_expr = remainder[1:].strip()
                    break
                i += 1
            else:
                name = _unquote_name(rest)
        else:
            # No leading quote but contains quote somewhere - split on '='
            if "=" in rest:
                name_part, _, expr_part = rest.partition("=")
                name = _unquote_name(name_part.strip())
                default_expr = expr_part.strip()
            else:
                name = _unquote_name(rest)
    elif "=" in rest:
        name_part, _, expr_part = rest.partition("=")
        name = name_part.strip()
        default_expr = expr_part.strip()
    else:
        name = rest.strip()

    return (keyword, name, default_expr)


def _parse_property(line: str) -> tuple[str, str]:
    """Parse a 'key: value' property line. Returns (key, value)."""
    stripped = line.strip()
    idx = stripped.find(":")
    if idx < 0:
        # Boolean shorthand: just the property name = true
        return (stripped, "true")
    key = stripped[:idx].strip()
    value = stripped[idx + 1:].strip()
    return (key, value)


def _parse_column_ref(ref: str) -> tuple[str, str]:
    """Parse a dotted Table.Column reference like  Sales.'Product Key'."""
    ref = ref.strip()
    dot_idx = -1
    in_quote = False
    for i, ch in enumerate(ref):
        if ch == "'":
            in_quote = not in_quote
        elif ch == "." and not in_quote:
            dot_idx = i
            break
    if dot_idx < 0:
        return ("", _unquote_name(ref))
    table = _unquote_name(ref[:dot_idx])
    column = _unquote_name(ref[dot_idx + 1:])
    return (table, column)


# ---------------------------------------------------------------------------
# Structured intermediate representation
# ---------------------------------------------------------------------------

@dataclass
class _TmdlObject:
    """Intermediate representation of a parsed TMDL object."""
    keyword: str = ""
    name: str = ""
    default_expr: str = ""
    description: str = ""
    properties: dict[str, str] = field(default_factory=dict)
    children: list[_TmdlObject] = field(default_factory=list)


def _parse_tmdl_text(text: str) -> list[_TmdlObject]:
    """Parse raw TMDL text into a flat/nested list of _TmdlObject."""
    lines = text.splitlines()
    root_objects: list[_TmdlObject] = []
    stack: list[tuple[int, _TmdlObject]] = []  # (indent, obj)
    pending_description: list[str] = []
    expr_target: _TmdlObject | None = None
    expr_indent: int = -1
    expr_prop_key: str = ""

    i = 0
    while i < len(lines):
        raw_line = lines[i]
        stripped = raw_line.strip()
        indent = _indent_level(raw_line)

        # Skip blank lines and ref lines
        if not stripped or stripped.startswith("ref "):
            if expr_target and stripped:
                # Blank lines inside expressions are preserved
                pass
            else:
                i += 1
                continue

        # Collecting multi-line expression?
        if expr_target is not None:
            if indent > expr_indent or (not stripped):
                if expr_prop_key:
                    prev = expr_target.properties.get(expr_prop_key, "")
                    expr_target.properties[expr_prop_key] = (
                        (prev + "\n" + stripped) if prev else stripped
                    )
                else:
                    expr_target.default_expr += "\n" + stripped
                i += 1
                continue
            else:
                expr_target = None
                expr_indent = -1
                expr_prop_key = ""
                # Fall through to process this line normally

        # Description lines (/// ...)
        if stripped.startswith("///"):
            desc_text = stripped[3:].strip()
            pending_description.append(desc_text)
            i += 1
            continue

        # Detect if this is an object declaration
        first_word = stripped.split(None, 1)[0].lower() if stripped else ""
        is_object_decl = first_word in _OBJECT_KEYWORDS

        if is_object_decl:
            keyword, name, default_expr = _split_declaration(stripped)
            obj = _TmdlObject(
                keyword=keyword,
                name=name,
                default_expr=default_expr,
                description="\n".join(pending_description),
            )
            pending_description.clear()

            # If default expression is empty and line ends with '=', next
            # lines are a multi-line expression
            if stripped.rstrip().endswith("=") and not default_expr:
                expr_target = obj
                expr_indent = indent
                expr_prop_key = ""

            # Place in tree based on indent
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                stack[-1][1].children.append(obj)
            else:
                root_objects.append(obj)

            stack.append((indent, obj))
            i += 1
            continue

        # Property line (key: value or key = expression)
        if ":" in stripped or "=" in stripped:
            # Determine parent object
            parent = stack[-1][1] if stack else None
            if parent:
                if "=" in stripped and ":" not in stripped.split("=", 1)[0]:
                    # Expression property: source = ...
                    key, _, val = stripped.partition("=")
                    key = key.strip()
                    val = val.strip()
                    parent.properties[key] = val
                    if not val or val == "```":
                        expr_target = parent
                        expr_indent = indent
                        expr_prop_key = key
                else:
                    key, val = _parse_property(stripped)
                    parent.properties[key] = val
        elif stack:
            # Boolean shorthand
            parent = stack[-1][1]
            parent.properties[stripped] = "true"

        i += 1

    return root_objects


# ---------------------------------------------------------------------------
# Public API: parse individual TMDL file types
# ---------------------------------------------------------------------------

def parse_tmdl_table(text: str) -> Table:
    """Parse a single table .tmdl file into a Table object."""
    objects = _parse_tmdl_text(text)

    table_obj: _TmdlObject | None = None
    for obj in objects:
        if obj.keyword == "table":
            table_obj = obj
            break

    if table_obj is None:
        return Table(name="(unknown)")

    columns: list[Column] = []
    measures: list[Measure] = []
    partitions: list[Partition] = []
    is_calculated = False
    calc_expr: str | None = None
    storage_mode = StorageMode.DEFAULT

    # Table-level properties
    mode_raw = table_obj.properties.get("mode", "").lower()
    if mode_raw in _STORAGE_MODE_MAP:
        storage_mode = _STORAGE_MODE_MAP[mode_raw]

    is_hidden = table_obj.properties.get("isHidden", "").lower() == "true"

    for child in table_obj.children:
        if child.keyword == "column":
            dt = child.properties.get("dataType", child.properties.get("datatype", ""))
            columns.append(Column(
                name=child.name,
                data_type=dt,
                is_hidden=child.properties.get("isHidden", "").lower() == "true",
                sort_by_column=child.properties.get("sortByColumn") or None,
                source_column=child.properties.get("sourceColumn") or None,
            ))

        elif child.keyword == "measure":
            expr = child.default_expr.strip()
            measures.append(Measure(
                name=child.name,
                expression=expr,
                description=child.description,
                format_string=child.properties.get("formatString", ""),
                is_hidden=child.properties.get("isHidden", "").lower() == "true",
            ))

        elif child.keyword == "partition":
            p_mode_raw = child.properties.get("mode", "").lower()
            p_mode = _STORAGE_MODE_MAP.get(p_mode_raw, StorageMode.DEFAULT)
            if p_mode != StorageMode.DEFAULT and storage_mode == StorageMode.DEFAULT:
                storage_mode = p_mode

            source_type = child.default_expr.strip() if child.default_expr else ""
            query = child.properties.get("source", "").strip()

            # Detect calculated partitions (DAX source type)
            if source_type.lower() in ("calculated", "dax"):
                is_calculated = True
                calc_expr = query or child.default_expr

            partitions.append(Partition(
                name=child.name,
                source_type=source_type,
                query=query,
                mode=p_mode,
            ))

    return Table(
        name=table_obj.name,
        columns=columns,
        measures=measures,
        partitions=partitions,
        storage_mode=storage_mode,
        is_hidden=is_hidden,
        is_calculated=is_calculated,
        calculated_table_expression=calc_expr,
    )


def parse_tmdl_relationships(text: str) -> list[Relationship]:
    """Parse relationships.tmdl into a list of Relationship objects."""
    objects = _parse_tmdl_text(text)
    relationships: list[Relationship] = []

    for obj in objects:
        if obj.keyword != "relationship":
            continue

        from_ref = obj.properties.get("fromColumn", "")
        to_ref = obj.properties.get("toColumn", "")

        from_table, from_col = _parse_column_ref(from_ref)
        to_table, to_col = _parse_column_ref(to_ref)

        if not from_table or not to_table:
            continue

        cf_raw = obj.properties.get(
            "crossFilteringBehavior",
            obj.properties.get("crossfilteringbehavior", "oneDirection"),
        ).lower()
        cross_dir = _CROSSFILTER_MAP.get(cf_raw, CrossFilterDirection.ONE)

        card_raw = obj.properties.get("cardinality", "").lower()
        if card_raw == "manytomany" or card_raw == "m:m":
            from_card, to_card = "many", "many"
        elif card_raw == "onetoone" or card_raw == "1:1":
            from_card, to_card = "one", "one"
        elif card_raw == "onetomany" or card_raw == "1:m":
            from_card, to_card = "one", "many"
        else:
            from_card, to_card = "many", "one"

        is_active_str = obj.properties.get("isActive", "true").lower()
        is_active = is_active_str != "false" and is_active_str != "0"

        relationships.append(Relationship(
            name=obj.name,
            from_table=from_table,
            from_column=from_col,
            to_table=to_table,
            to_column=to_col,
            cross_filter_direction=cross_dir,
            from_cardinality=from_card,
            to_cardinality=to_card,
            is_active=is_active,
        ))

    return relationships


@dataclass
class TmdlDatabaseInfo:
    name: str = ""
    compatibility_level: int = 0


def parse_tmdl_database(text: str) -> TmdlDatabaseInfo:
    """Parse database.tmdl for name and compatibilityLevel."""
    objects = _parse_tmdl_text(text)
    info = TmdlDatabaseInfo()
    for obj in objects:
        if obj.keyword == "database":
            info.name = obj.name
            cl = obj.properties.get("compatibilityLevel", "0")
            try:
                info.compatibility_level = int(cl)
            except ValueError:
                pass
            break
    return info


@dataclass
class TmdlModelInfo:
    culture: str = ""


def parse_tmdl_model(text: str) -> TmdlModelInfo:
    """Parse model.tmdl for model-level configuration."""
    objects = _parse_tmdl_text(text)
    info = TmdlModelInfo()
    for obj in objects:
        if obj.keyword == "model":
            info.culture = obj.properties.get("culture", "")
            break
    return info


def parse_tmdl_datasources(text: str) -> list[DataSource]:
    """Parse dataSources.tmdl for connection info."""
    objects = _parse_tmdl_text(text)
    sources: list[DataSource] = []
    for obj in objects:
        if obj.keyword in ("datasource", "dataSource"):
            sources.append(DataSource(
                name=obj.name,
                connection_string=obj.properties.get("connectionString", ""),
                provider=obj.properties.get("provider", ""),
            ))
    return sources
