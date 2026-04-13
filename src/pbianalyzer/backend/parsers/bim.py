"""Parse .bim files (Tabular Model Definition Language JSON)."""

from __future__ import annotations

import json
from .models import (
    Column, CrossFilterDirection, DataSource, Measure, ModelAnnotation,
    Partition, PBIModel, Relationship, StorageMode, Table,
)


def _parse_storage_mode(raw: str | None) -> StorageMode:
    if not raw:
        return StorageMode.DEFAULT
    mapping = {
        "import": StorageMode.IMPORT,
        "directquery": StorageMode.DIRECT_QUERY,
        "directlake": StorageMode.DIRECT_QUERY,
        "dual": StorageMode.DUAL,
        "default": StorageMode.DEFAULT,
    }
    return mapping.get(raw.lower(), StorageMode.DEFAULT)


def _parse_column(col: dict) -> Column:
    return Column(
        name=col.get("name", ""),
        data_type=col.get("dataType", ""),
        is_hidden=col.get("isHidden", False),
        sort_by_column=col.get("sortByColumn"),
        source_column=col.get("sourceColumn"),
    )


def _parse_measure(m: dict) -> Measure:
    expression = m.get("expression", "")
    if isinstance(expression, list):
        expression = "\n".join(expression)
    return Measure(
        name=m.get("name", ""),
        expression=expression,
        description=m.get("description", ""),
        format_string=m.get("formatString", ""),
        is_hidden=m.get("isHidden", False),
    )


def _parse_partition(p: dict) -> Partition:
    source = p.get("source", {})
    query = source.get("expression", "")
    if isinstance(query, list):
        query = "\n".join(query)
    return Partition(
        name=p.get("name", ""),
        source_type=source.get("type", ""),
        query=query,
        mode=_parse_storage_mode(p.get("mode")),
    )


def _detect_calculated_table(table: dict) -> tuple[bool, str | None]:
    """Detect if a table is a DAX calculated table."""
    for p in table.get("partitions", []):
        source = p.get("source", {})
        if source.get("type") == "calculated":
            expr = source.get("expression", "")
            if isinstance(expr, list):
                expr = "\n".join(expr)
            return True, expr
    return False, None


def _parse_table(t: dict) -> Table:
    is_calc, calc_expr = _detect_calculated_table(t)
    storage = _parse_storage_mode(t.get("mode"))
    if storage == StorageMode.DEFAULT:
        for p in t.get("partitions", []):
            p_mode = _parse_storage_mode(p.get("mode"))
            if p_mode != StorageMode.DEFAULT:
                storage = p_mode
                break
    return Table(
        name=t.get("name", ""),
        columns=[_parse_column(c) for c in t.get("columns", [])],
        measures=[_parse_measure(m) for m in t.get("measures", [])],
        partitions=[_parse_partition(p) for p in t.get("partitions", [])],
        storage_mode=storage,
        is_hidden=t.get("isHidden", False),
        is_calculated=is_calc,
        calculated_table_expression=calc_expr,
    )


def _parse_relationship(r: dict) -> Relationship:
    cross_dir = r.get("crossFilteringBehavior", "oneDirection")
    if cross_dir == "bothDirections":
        direction = CrossFilterDirection.BOTH
    elif cross_dir == "automatic":
        direction = CrossFilterDirection.AUTOMATIC
    else:
        direction = CrossFilterDirection.ONE

    return Relationship(
        name=r.get("name", ""),
        from_table=r.get("fromTable", ""),
        from_column=r.get("fromColumn", ""),
        to_table=r.get("toTable", ""),
        to_column=r.get("toColumn", ""),
        cross_filter_direction=direction,
        from_cardinality=r.get("fromCardinality", "many"),
        to_cardinality=r.get("toCardinality", "one"),
        is_active=r.get("isActive", True),
    )


def parse_bim(content: bytes | str) -> PBIModel:
    """Parse a .bim file and return a normalized PBIModel."""
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig")
    data = json.loads(content)

    model_data = data.get("model", data)

    tables_raw = model_data.get("tables", [])
    tables = [_parse_table(t) for t in tables_raw]

    relationships = [
        _parse_relationship(r)
        for r in model_data.get("relationships", [])
    ]

    data_sources = []
    for ds in model_data.get("dataSources", []):
        data_sources.append(DataSource(
            name=ds.get("name", ""),
            connection_string=ds.get("connectionString", ""),
            provider=ds.get("provider", ""),
        ))

    annotations = []
    for ann in model_data.get("annotations", []):
        annotations.append(ModelAnnotation(
            name=ann.get("name", ""),
            value=str(ann.get("value", "")),
        ))

    return PBIModel(
        name=data.get("name", ""),
        compatibility_level=data.get("compatibilityLevel", 0),
        tables=tables,
        relationships=relationships,
        data_sources=data_sources,
        annotations=annotations,
    )
