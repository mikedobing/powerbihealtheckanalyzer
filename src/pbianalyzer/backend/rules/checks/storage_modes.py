"""Storage mode checks."""

from __future__ import annotations

from ...parsers.models import Finding, Impact, PBIModel, Severity, StorageMode


def run_storage_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    dim_tables = model.dimension_tables
    fact_tables = model.fact_tables

    dq_fact_names = {
        t.name for t in fact_tables
        if t.storage_mode in (StorageMode.DIRECT_QUERY, StorageMode.DEFAULT)
    }

    # Dimension tables on DirectQuery that should be Dual
    for dim in dim_tables:
        if dim.storage_mode in (StorageMode.DIRECT_QUERY, StorageMode.DEFAULT):
            connected_to_dq_fact = any(
                (rel.from_table == dim.name and rel.to_table in dq_fact_names)
                or (rel.to_table == dim.name and rel.from_table in dq_fact_names)
                for rel in model.relationships
            )
            if connected_to_dq_fact:
                findings.append(Finding(
                    rule_id="storage_dim_not_dual",
                    category="storage_modes",
                    name="Dimension Not Using Dual Mode",
                    severity=Severity.WARNING,
                    description=(
                        f"Dimension '{dim.name}' is DirectQuery but connected to "
                        f"DirectQuery fact table(s) — should be Dual mode"
                    ),
                    recommendation=(
                        "Set to Dual storage mode. Power BI will use Import for slicers "
                        "and DirectQuery for joins with fact tables."
                    ),
                    impact=Impact.HIGH,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/02.%20Storage%20Modes",
                    details={"table": dim.name, "current_mode": dim.storage_mode.value},
                ))

    # Import dimensions connected to DQ facts (full-table pull risk)
    for dim in dim_tables:
        if dim.storage_mode == StorageMode.IMPORT:
            connected_to_dq_fact = any(
                (rel.from_table == dim.name and rel.to_table in dq_fact_names)
                or (rel.to_table == dim.name and rel.from_table in dq_fact_names)
                for rel in model.relationships
            )
            if connected_to_dq_fact:
                findings.append(Finding(
                    rule_id="storage_import_dim_with_dq_fact",
                    category="storage_modes",
                    name="Import Dimension + DQ Fact",
                    severity=Severity.WARNING,
                    description=(
                        f"Import dimension '{dim.name}' connected to DirectQuery fact table — "
                        f"can cause large result set pulls"
                    ),
                    recommendation=(
                        "Switch to Dual mode. Import dimensions with DQ facts cause Power BI "
                        "to retrieve excessive rows from the backend."
                    ),
                    impact=Impact.HIGH,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/02.%20Storage%20Modes",
                    details={"table": dim.name},
                ))

    # All DQ with complex DAX
    all_dq = all(
        t.storage_mode in (StorageMode.DIRECT_QUERY, StorageMode.DEFAULT)
        for t in model.tables
        if not t.is_hidden and not t.name.startswith("LocalDateTable_")
    )
    total_measures = sum(len(t.measures) for t in model.tables)
    if all_dq and total_measures > 10:
        findings.append(Finding(
            rule_id="storage_all_dq_complex_dax",
            category="storage_modes",
            name="Full DirectQuery with Complex DAX",
            severity=Severity.WARNING,
            description=(
                f"All tables are DirectQuery with {total_measures} measures — "
                f"high risk of poor query performance"
            ),
            recommendation="Consider Composite model with dimension tables in Dual mode.",
            impact=Impact.HIGH,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/02.%20Storage%20Modes",
            details={"total_measures": total_measures},
        ))

    return findings
