"""Generate Databricks Asset Bundle (DABs) with SDP pipeline from PBI model analysis."""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass, field

from ..parsers.models import PBIModel
from .m_query_analyzer import MQueryAnalysis, TableMigrationResult, analyze_m_queries
from .m_to_sql import ConversionTier
from .dax_to_metrics import analyze_model_for_metrics
from .metrics_converter import generate_metric_view_yaml


@dataclass
class PipelineFile:
    """A single file to include in the DABs bundle."""
    path: str
    content: str


@dataclass
class GeneratedBundle:
    """Complete DABs bundle output."""
    files: list[PipelineFile] = field(default_factory=list)
    pipeline_name: str = ""
    table_count: int = 0
    bronze_count: int = 0
    silver_count: int = 0
    gold_count: int = 0
    manual_count: int = 0
    warnings: list[str] = field(default_factory=list)


def generate_pipeline_bundle(
    model: PBIModel,
    m_analysis: MQueryAnalysis,
    catalog: str = "my_catalog",
    schema: str = "my_schema",
    bundle_name: str = "",
    include_metric_view: bool = True,
) -> GeneratedBundle:
    """Generate a complete DABs bundle from PBI model analysis."""
    if not bundle_name:
        safe_name = re.sub(r"[^\w]+", "_", model.name or "pbi_pipeline").strip("_").lower()
        bundle_name = safe_name or "pbi_pipeline"

    bundle = GeneratedBundle(pipeline_name=bundle_name)

    bronze_files: list[PipelineFile] = []
    silver_files: list[PipelineFile] = []
    gold_files: list[PipelineFile] = []
    manual_tables: list[TableMigrationResult] = []

    for table_result in m_analysis.tables:
        if not table_result.has_m_query and not table_result.is_calculated_table:
            continue

        if table_result.is_calculated_table:
            manual_tables.append(table_result)
            bundle.manual_count += 1
            continue

        if table_result.tier == ConversionTier.MANUAL and not table_result.generated_sql:
            manual_tables.append(table_result)
            bundle.manual_count += 1
            continue

        safe_table = _snake(table_result.table_name)
        layer = table_result.suggested_layer

        if layer == "bronze":
            sql = _build_bronze_sql(table_result, catalog, schema)
            bronze_files.append(PipelineFile(
                path=f"src/transformations/bronze_{safe_table}.sql",
                content=sql,
            ))
            bundle.bronze_count += 1
        elif layer == "silver":
            bronze_sql = _build_bronze_sql(table_result, catalog, schema, raw=True)
            bronze_files.append(PipelineFile(
                path=f"src/transformations/bronze_{safe_table}_raw.sql",
                content=bronze_sql,
            ))
            bundle.bronze_count += 1

            silver_sql = _build_silver_sql(table_result, safe_table)
            silver_files.append(PipelineFile(
                path=f"src/transformations/silver_{safe_table}.sql",
                content=silver_sql,
            ))
            bundle.silver_count += 1
        elif layer == "gold":
            bronze_sql = _build_bronze_sql(table_result, catalog, schema, raw=True)
            bronze_files.append(PipelineFile(
                path=f"src/transformations/bronze_{safe_table}_raw.sql",
                content=bronze_sql,
            ))
            bundle.bronze_count += 1

            gold_sql = _build_gold_sql(table_result, safe_table)
            gold_files.append(PipelineFile(
                path=f"src/transformations/gold_{safe_table}.sql",
                content=gold_sql,
            ))
            bundle.gold_count += 1

    if include_metric_view:
        metrics_analysis = analyze_model_for_metrics(model)
        if metrics_analysis.direct_count + metrics_analysis.translatable_count > 0:
            _, metric_sql, _ = generate_metric_view_yaml(
                metrics_analysis, model,
                catalog=catalog,
                schema=schema,
                view_name=f"{bundle_name}_metrics",
            )
            if metric_sql:
                gold_files.append(PipelineFile(
                    path=f"src/transformations/gold_{bundle_name}_metrics.sql",
                    content=f"-- UC Metric View generated from PBI DAX measures\n"
                            f"-- Deploy separately: this is a UC metric view, not an SDP table\n"
                            f"-- Run this SQL directly against your catalog\n\n"
                            f"{metric_sql}\n",
                ))

    bundle.files.extend(bronze_files)
    bundle.files.extend(silver_files)
    bundle.files.extend(gold_files)
    bundle.table_count = bundle.bronze_count + bundle.silver_count + bundle.gold_count

    if manual_tables:
        manual_md = _build_manual_migration_doc(manual_tables)
        bundle.files.append(PipelineFile(
            path="docs/manual_migration_notes.md",
            content=manual_md,
        ))
        bundle.warnings.append(
            f"{len(manual_tables)} table(s) require manual migration (see docs/manual_migration_notes.md)"
        )

    databricks_yml = _build_databricks_yml(bundle_name, catalog, schema)
    bundle.files.append(PipelineFile(path="databricks.yml", content=databricks_yml))

    pipeline_yml = _build_pipeline_yml(bundle_name)
    bundle.files.append(PipelineFile(path=f"resources/{bundle_name}_pipeline.yml", content=pipeline_yml))

    readme = _build_readme(bundle_name, bundle, m_analysis)
    bundle.files.append(PipelineFile(path="README.md", content=readme))

    return bundle


def bundle_to_zip(bundle: GeneratedBundle) -> bytes:
    """Serialize a generated bundle to a ZIP file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in bundle.files:
            zf.writestr(f"{bundle.pipeline_name}/{f.path}", f.content)
    return buf.getvalue()


# --- SQL generators ---

def _best_sql(table: TableMigrationResult) -> str:
    """Return the best available SQL — AI-enhanced if available, otherwise heuristic."""
    if table.ai_enhanced and table.ai_sql:
        return table.ai_sql
    return table.generated_sql


def _build_bronze_sql(
    table: TableMigrationResult,
    catalog: str,
    schema: str,
    raw: bool = False,
) -> str:
    """Generate bronze layer SDP SQL (raw ingestion from source table)."""
    safe_name = _snake(table.table_name)
    table_name = f"bronze_{safe_name}_raw" if raw else f"bronze_{safe_name}"

    source_fqn = table.source_fqn
    if not source_fqn:
        source_fqn = f"{catalog}.{schema}.{safe_name}"

    best = _best_sql(table)
    if best and not raw:
        select = best
    else:
        select = f"SELECT\n  *\nFROM {source_fqn}"

    lines = [
        f"-- Bronze: raw ingestion of {table.table_name}",
        f"-- Source: {table.source_type or 'unknown'}",
        f"CREATE OR REFRESH MATERIALIZED VIEW {table_name}",
        f"AS",
        select,
        ";",
    ]
    return "\n".join(lines) + "\n"


def _build_silver_sql(table: TableMigrationResult, safe_name: str) -> str:
    """Generate silver layer SDP SQL with transformations from M query."""
    source_ref = f"bronze_{safe_name}_raw"
    best = _best_sql(table)

    if best:
        inner_sql = best
        inner_sql = re.sub(
            r"FROM\s+\S+",
            f"FROM LIVE.{source_ref}",
            inner_sql,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        inner_sql = f"SELECT\n  *\nFROM LIVE.{source_ref}"

    lines = [
        f"-- Silver: cleaned & transformed {table.table_name}",
        f"CREATE OR REFRESH MATERIALIZED VIEW silver_{safe_name}",
        f"AS",
        inner_sql,
        ";",
    ]

    if table.warnings:
        lines.insert(1, f"-- Warnings: {'; '.join(table.warnings)}")

    return "\n".join(lines) + "\n"


def _build_gold_sql(table: TableMigrationResult, safe_name: str) -> str:
    """Generate gold layer SDP SQL with aggregations."""
    source_ref = f"bronze_{safe_name}_raw"
    best = _best_sql(table)

    if best:
        inner_sql = best
        inner_sql = re.sub(
            r"FROM\s+\S+",
            f"FROM LIVE.{source_ref}",
            inner_sql,
            count=1,
            flags=re.IGNORECASE,
        )
    else:
        inner_sql = f"SELECT\n  *\nFROM LIVE.{source_ref}"

    lines = [
        f"-- Gold: aggregated view of {table.table_name}",
        f"CREATE OR REFRESH MATERIALIZED VIEW gold_{safe_name}",
        f"AS",
        inner_sql,
        ";",
    ]
    return "\n".join(lines) + "\n"


# --- Bundle config generators ---

def _build_databricks_yml(bundle_name: str, catalog: str, schema: str) -> str:
    return f"""bundle:
  name: {bundle_name}

include:
  - resources/*.yml

variables:
  catalog:
    default: "{catalog}"
  schema:
    default: "{schema}"

targets:
  dev:
    default: true
    mode: development
    variables:
      catalog: "{catalog}"
      schema: "{schema}"

  prod:
    mode: production
    variables:
      catalog: "{catalog}"
      schema: "{schema}"
"""


def _build_pipeline_yml(bundle_name: str) -> str:
    lines = [
        "resources:",
        "  pipelines:",
        f"    {bundle_name}_etl:",
        '      name: "[${bundle.target}] ' + bundle_name + '"',
        "      catalog: ${var.catalog}",
        "      target: ${var.schema}",
        "      libraries:",
        "        - glob:",
        "            include: ../src/transformations/**",
        "      root_path: ../src",
        "      serverless: true",
        "      channel: CURRENT",
        "      development: true",
        "      continuous: false",
        "      photon: true",
    ]
    return "\n".join(lines) + "\n"


def _build_manual_migration_doc(tables: list[TableMigrationResult]) -> str:
    lines = [
        "# Manual Migration Notes",
        "",
        "The following tables could not be fully auto-converted and require manual attention.",
        "",
    ]
    for t in tables:
        lines.append(f"## {t.table_name}")
        lines.append("")
        if t.is_calculated_table:
            lines.append("**Type:** DAX Calculated Table")
            lines.append("")
            if t.original_m:
                lines.append("**DAX Expression:**")
                lines.append("```dax")
                lines.append(t.original_m)
                lines.append("```")
        else:
            lines.append(f"**Source:** {t.source_type or 'Unknown'}")
            lines.append(f"**FQN:** {t.source_fqn or 'N/A'}")
            lines.append("")
            if t.manual_step_names:
                lines.append(f"**Manual steps:** {', '.join(t.manual_step_names)}")
            if t.warnings:
                lines.append(f"**Warnings:** {'; '.join(t.warnings)}")
            if t.original_m:
                lines.append("")
                lines.append("**Original M Query:**")
                lines.append("```powerquery")
                lines.append(t.original_m)
                lines.append("```")
        lines.append("")
    return "\n".join(lines)


def _build_readme(
    bundle_name: str,
    bundle: GeneratedBundle,
    analysis: MQueryAnalysis,
) -> str:
    return f"""# {bundle_name}

Auto-generated Databricks Asset Bundle from Power BI model migration.

## Pipeline Structure

| Layer | Tables | Description |
|-------|--------|-------------|
| Bronze | {bundle.bronze_count} | Raw data ingestion from source tables |
| Silver | {bundle.silver_count} | Cleaned and transformed data |
| Gold | {bundle.gold_count} | Aggregated business-ready views |
| Manual | {bundle.manual_count} | Requires manual migration |

## Migration Score: {analysis.migration_score}%

- **Auto-convertible:** {analysis.auto_count} tables
- **Partially convertible:** {analysis.partial_count} tables
- **Manual required:** {analysis.manual_count} tables
- **Databricks sources:** {analysis.databricks_source_count}
- **Non-Databricks sources:** {analysis.non_databricks_source_count}

## Deployment

```bash
# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy

# Run the pipeline
databricks bundle run {bundle_name}_etl

# Deploy to production
databricks bundle deploy -t prod
```

## Notes

- Bronze tables use `CREATE OR REFRESH MATERIALIZED VIEW` for batch ingestion
- Silver tables reference bronze via `LIVE.bronze_*` references
- Gold metric views may need to be deployed separately as UC Metric Views
- Review `docs/manual_migration_notes.md` for tables requiring manual attention
- Update `databricks.yml` variables with your target catalog and schema
"""


def _snake(name: str) -> str:
    s = re.sub(r"[^\w]+", "_", name).strip("_")
    return s.lower()
