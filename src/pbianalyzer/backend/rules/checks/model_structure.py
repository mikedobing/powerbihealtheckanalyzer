"""Data model structure checks."""

from __future__ import annotations

import re
from ...parsers.models import Finding, Impact, PBIModel, Severity, StorageMode


def run_model_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    real_tables = [t for t in model.tables if not t.is_hidden and not t.name.startswith("LocalDateTable_")]

    # Wide table detection
    max_cols = thresholds.get("model_wide_table", {}).get("max_columns", 30)
    for table in real_tables:
        if len(table.columns) > max_cols:
            findings.append(Finding(
                rule_id="model_wide_table",
                category="data_model",
                name="Wide Table Detected",
                severity=Severity.WARNING,
                description=f"Table '{table.name}' has {len(table.columns)} columns (threshold: {max_cols})",
                recommendation="Split into a star schema with fact and dimension tables.",
                impact=Impact.HIGH,
                reference_url="https://learn.microsoft.com/power-bi/guidance/star-schema",
                details={"table": table.name, "column_count": len(table.columns)},
            ))

    # Single flat table model
    if len(real_tables) == 1 and len(model.relationships) == 0:
        findings.append(Finding(
            rule_id="model_single_flat_table",
            category="data_model",
            name="Single Flat Table Model",
            severity=Severity.WARNING,
            description=f"Model contains only '{real_tables[0].name}' with no relationships",
            recommendation="Refactor into a star schema with fact and dimension tables.",
            impact=Impact.HIGH,
            reference_url="https://learn.microsoft.com/power-bi/guidance/star-schema",
        ))

    # Bidirectional cross-filter
    for rel in model.relationships:
        if rel.cross_filter_direction.value == "bothDirections":
            findings.append(Finding(
                rule_id="model_bidir_crossfilter",
                category="data_model",
                name="Bidirectional Cross-Filter",
                severity=Severity.WARNING,
                description=f"Relationship {rel.from_table} → {rel.to_table} uses bidirectional cross-filtering",
                recommendation="Change to single-direction unless bidirectional is strictly required.",
                impact=Impact.MEDIUM,
                reference_url="https://learn.microsoft.com/power-bi/guidance/relationships-bidirectional-filtering",
                details={"from_table": rel.from_table, "to_table": rel.to_table},
            ))

    # Many-to-many relationships
    for rel in model.relationships:
        if rel.from_cardinality == "many" and rel.to_cardinality == "many":
            findings.append(Finding(
                rule_id="model_many_to_many",
                category="data_model",
                name="Many-to-Many Relationship",
                severity=Severity.WARNING,
                description=f"Many-to-many relationship between {rel.from_table} and {rel.to_table}",
                recommendation="Introduce a bridge table or refactor to one-to-many relationships.",
                impact=Impact.MEDIUM,
                reference_url="https://learn.microsoft.com/power-bi/guidance/relationships-many-to-many",
                details={"from_table": rel.from_table, "to_table": rel.to_table},
            ))

    # Orphan tables
    connected = set()
    for rel in model.relationships:
        connected.add(rel.from_table)
        connected.add(rel.to_table)
    for table in real_tables:
        if table.name not in connected and len(real_tables) > 1:
            findings.append(Finding(
                rule_id="model_orphan_table",
                category="data_model",
                name="Orphan Table",
                severity=Severity.INFO,
                description=f"Table '{table.name}' has no relationships to other tables",
                recommendation="Connect via relationships, hide if a helper table, or remove if unused.",
                impact=Impact.LOW,
                details={"table": table.name},
            ))

    # DAX-generated calendar/date tables
    calendar_pattern = re.compile(r"\b(CALENDAR|CALENDARAUTO)\s*\(", re.IGNORECASE)
    for table in model.tables:
        if table.is_calculated and table.calculated_table_expression:
            if calendar_pattern.search(table.calculated_table_expression):
                findings.append(Finding(
                    rule_id="model_generated_date_table",
                    category="data_model",
                    name="DAX-Generated Date Table",
                    severity=Severity.WARNING,
                    description=f"Table '{table.name}' is a DAX CALENDAR/CALENDARAUTO calculated table",
                    recommendation="Replace with a persisted Delta table in Databricks set to Dual mode.",
                    impact=Impact.HIGH,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/11.%20Generated%20vs%20Persisted%20dimension",
                    details={"table": table.name},
                ))

    # No aggregation tables for large DQ models
    dq_fact_tables = [
        t for t in model.fact_tables
        if t.storage_mode in (StorageMode.DIRECT_QUERY, StorageMode.DEFAULT)
    ]
    has_agg_table = any("agg" in t.name.lower() for t in model.tables)
    if len(dq_fact_tables) >= 1 and not has_agg_table and len(real_tables) > 3:
        findings.append(Finding(
            rule_id="model_no_aggregation_tables",
            category="data_model",
            name="No Aggregation Tables",
            severity=Severity.WARNING,
            description="DirectQuery fact tables found with no apparent aggregation tables",
            recommendation="Create pre-aggregated tables in Databricks and configure user-defined aggregations.",
            impact=Impact.HIGH,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/05.%20User-defined%20Aggregations",
            details={"dq_fact_tables": [t.name for t in dq_fact_tables]},
        ))

    return findings
