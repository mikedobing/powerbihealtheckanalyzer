"""Generate UC Metric View YAML from a PBI model analysis."""

from __future__ import annotations

import re
from textwrap import indent

from ..parsers.models import PBIModel
from .dax_to_metrics import MetricsAnalysis, _alias, _snake


def generate_metric_view_yaml(
    analysis: MetricsAnalysis,
    model: PBIModel,
    catalog: str = "my_catalog",
    schema: str = "my_schema",
    view_name: str | None = None,
) -> tuple[str, str, list[str]]:
    """Generate UC Metric View YAML and the wrapping SQL statement.

    Returns (yaml_body, full_sql_statement, warnings).
    """
    warnings: list[str] = list(analysis.warnings)

    if not view_name:
        view_name = _snake(analysis.source_table or model.name or "pbi_metrics") + "_metrics"

    full_name = f"{catalog}.{schema}.{view_name}"
    source_fqn = f"{catalog}.{schema}.{_snake(analysis.source_table)}"

    lines: list[str] = []
    lines.append("version: 1.1")
    lines.append(f'comment: "Metric view migrated from Power BI model: {_esc(model.name or analysis.source_table)}"')
    lines.append(f"source: {source_fqn}")

    # Joins
    if analysis.proposed_joins:
        lines.append("")
        lines.append("joins:")
        for j in analysis.proposed_joins:
            fqn = j.source.format(catalog=catalog, schema=schema)
            lines.append(f"  - name: {j.name}")
            lines.append(f"    source: {fqn}")
            lines.append(f"    on: {j.on_clause}")

    # Dimensions
    dims = analysis.proposed_dimensions
    if dims:
        lines.append("")
        lines.append("dimensions:")
        for d in dims:
            lines.append(f"  - name: {d.name}")
            lines.append(f"    expr: {d.expr}")
            if d.comment:
                lines.append(f'    comment: "{_esc(d.comment)}"')
    else:
        lines.append("")
        lines.append("dimensions:")
        lines.append("  - name: placeholder_dimension")
        lines.append("    expr: 'TODO: add a dimension column'")
        lines.append("    comment: \"No dimensions auto-detected -- replace with actual columns\"")
        warnings.append("No dimensions auto-detected. Edit the 'dimensions' section manually.")

    # Measures -- only direct and translatable
    convertible = [c for c in analysis.classifications if c.tier in ("direct", "translatable") and c.sql_expression]
    manual_measures = [c for c in analysis.classifications if c.tier == "manual" or (c.tier == "translatable" and not c.sql_expression)]

    if convertible:
        lines.append("")
        lines.append("measures:")
        for c in convertible:
            lines.append(f"  - name: {c.measure_name}")
            lines.append(f"    expr: {c.sql_expression}")
            lines.append(f'    comment: "Converted from DAX: {_esc_short(c.dax_expression)}"')
    else:
        lines.append("")
        lines.append("measures:")
        lines.append("  - name: placeholder_measure")
        lines.append("    expr: COUNT(1)")
        lines.append("    comment: \"No measures auto-converted -- add measures manually\"")
        warnings.append("No measures could be auto-converted. Add measures manually.")

    # Comment block for manual measures
    if manual_measures:
        lines.append("")
        lines.append("# --- Measures requiring manual conversion ---")
        for c in manual_measures:
            dax_short = c.dax_expression.replace("\n", " ")[:120]
            lines.append(f"# {c.measure_name}: {c.notes}")
            lines.append(f"#   DAX: {dax_short}")

    yaml_body = "\n".join(lines)

    sql = (
        f"CREATE OR REPLACE VIEW {full_name}\n"
        f"WITH METRICS\n"
        f"LANGUAGE YAML\n"
        f"AS $$\n"
        f"{indent(yaml_body, '  ')}\n"
        f"$$"
    )

    return yaml_body, sql, warnings


def _esc(text: str) -> str:
    return text.replace('"', '\\"').replace("\n", " ")


def _esc_short(text: str) -> str:
    s = text.replace('"', "'").replace("\n", " ").strip()
    if len(s) > 80:
        return s[:77] + "..."
    return s
