"""Analyze M queries across a PBI model for pipeline migration feasibility."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from ..parsers.models import PBIModel, Table
from .m_query_parser import (
    MStepType,
    ParsedMQuery,
    SourceType,
    parse_m_expression,
)
from .m_to_sql import (
    ConversionTier,
    TranslatedQuery,
    build_sql_statement,
    translate_m_query,
)

logger = logging.getLogger(__name__)


@dataclass
class TableMigrationResult:
    """Migration analysis for a single PBI table's M query."""
    table_name: str
    has_m_query: bool = False
    source_type: str = ""
    source_fqn: str = ""
    tier: str = ConversionTier.AUTO
    generated_sql: str = ""
    step_count: int = 0
    auto_steps: int = 0
    partial_steps: int = 0
    manual_steps: int = 0
    manual_step_names: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    is_calculated_table: bool = False
    is_databricks_source: bool = False
    suggested_layer: str = "bronze"
    original_m: str = ""
    # AI-enhanced fields
    ai_enhanced: bool = False
    ai_sql: str = ""
    ai_changes: list[str] = field(default_factory=list)
    ai_confidence: str = ""
    ai_step_translations: list[dict] = field(default_factory=list)


@dataclass
class MQueryAnalysis:
    """Full analysis of all M queries in a PBI model."""
    tables: list[TableMigrationResult] = field(default_factory=list)
    total_tables: int = 0
    tables_with_m: int = 0
    auto_count: int = 0
    partial_count: int = 0
    manual_count: int = 0
    databricks_source_count: int = 0
    non_databricks_source_count: int = 0
    migration_score: float = 0.0
    unique_sources: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    ai_enhanced: bool = False


def analyze_m_queries(model: PBIModel) -> MQueryAnalysis:
    """Analyze all M queries in a PBI model for pipeline migration."""
    analysis = MQueryAnalysis()

    seen_sources: dict[str, dict] = {}

    for table in model.tables:
        if _is_system_table(table):
            continue

        analysis.total_tables += 1

        if table.is_calculated:
            result = TableMigrationResult(
                table_name=table.name,
                has_m_query=False,
                is_calculated_table=True,
                tier=ConversionTier.MANUAL,
                warnings=["DAX calculated table -- convert separately via UC Metrics tab"],
                original_m=table.calculated_table_expression or "",
            )
            analysis.tables.append(result)
            analysis.manual_count += 1
            continue

        m_expr = _get_m_expression(table)
        if not m_expr:
            result = TableMigrationResult(
                table_name=table.name,
                has_m_query=False,
                tier=ConversionTier.MANUAL,
                warnings=["No M query found for this table"],
            )
            analysis.tables.append(result)
            continue

        analysis.tables_with_m += 1
        result = _analyze_single_table(table.name, m_expr)
        analysis.tables.append(result)

        if result.source_fqn and result.source_fqn not in seen_sources:
            seen_sources[result.source_fqn] = {
                "fqn": result.source_fqn,
                "type": result.source_type,
                "is_databricks": result.is_databricks_source,
            }

        if result.tier == ConversionTier.AUTO:
            analysis.auto_count += 1
        elif result.tier == ConversionTier.PARTIAL:
            analysis.partial_count += 1
        else:
            analysis.manual_count += 1

        if result.is_databricks_source:
            analysis.databricks_source_count += 1
        elif result.has_m_query and result.source_type:
            analysis.non_databricks_source_count += 1

    analysis.unique_sources = list(seen_sources.values())

    scored = analysis.auto_count + analysis.partial_count + analysis.manual_count
    if scored > 0:
        auto_weight = analysis.auto_count * 1.0
        partial_weight = analysis.partial_count * 0.5
        analysis.migration_score = round(
            (auto_weight + partial_weight) / scored * 100, 1
        )

    if analysis.non_databricks_source_count > 0:
        analysis.warnings.append(
            f"{analysis.non_databricks_source_count} table(s) use non-Databricks sources "
            f"that must be ingested into Unity Catalog first"
        )

    calc_tables = [t for t in analysis.tables if t.is_calculated_table]
    if calc_tables:
        analysis.warnings.append(
            f"{len(calc_tables)} calculated table(s) need manual conversion "
            f"(DAX → SQL or materialized as Delta tables)"
        )

    return analysis


def _analyze_single_table(table_name: str, m_expr: str) -> TableMigrationResult:
    """Analyze a single table's M query."""
    parsed = parse_m_expression(m_expr)
    translated = translate_m_query(parsed)
    sql = build_sql_statement(translated)

    is_db = parsed.source.source_type in (
        SourceType.DATABRICKS_CATALOG,
        SourceType.DATABRICKS_QUERY,
    )

    auto = sum(1 for f in translated.fragments if f.tier == ConversionTier.AUTO)
    partial = sum(1 for f in translated.fragments if f.tier == ConversionTier.PARTIAL)
    manual = sum(1 for f in translated.fragments if f.tier == ConversionTier.MANUAL)

    non_trivial = [s for s in parsed.steps if s.step_type not in (MStepType.SOURCE, MStepType.NAVIGATION)]
    layer = _suggest_layer(parsed, translated, non_trivial)

    warnings: list[str] = list(translated.warnings)
    warnings.extend(parsed.parse_warnings)

    return TableMigrationResult(
        table_name=table_name,
        has_m_query=True,
        source_type=parsed.source.source_type.value,
        source_fqn=translated.source_fqn,
        tier=translated.tier,
        generated_sql=sql,
        step_count=len(non_trivial),
        auto_steps=auto,
        partial_steps=partial,
        manual_steps=manual,
        manual_step_names=translated.manual_steps,
        warnings=warnings,
        is_databricks_source=is_db,
        suggested_layer=layer,
        original_m=m_expr,
    )


def _suggest_layer(
    parsed: ParsedMQuery,
    translated: TranslatedQuery,
    non_trivial: list,
) -> str:
    """Suggest a medallion layer for this table's pipeline definition."""
    if not non_trivial:
        return "bronze"
    has_transform = any(
        s.step_type in (
            MStepType.FILTER, MStepType.RENAME, MStepType.TYPE_CAST,
            MStepType.ADD_COLUMN, MStepType.REPLACE_VALUE,
        )
        for s in non_trivial
    )
    has_aggregation = any(s.step_type == MStepType.GROUP for s in non_trivial)

    if has_aggregation:
        return "gold"
    if has_transform:
        return "silver"
    return "bronze"


def _get_m_expression(table: Table) -> str:
    """Extract the M query expression from a table's partitions."""
    for partition in table.partitions:
        if partition.source_type.lower() in ("m", "powerquery", ""):
            if partition.query.strip():
                return partition.query.strip()
    for partition in table.partitions:
        if partition.query.strip() and partition.source_type.lower() != "calculated":
            return partition.query.strip()
    return ""


def _is_system_table(table: Table) -> bool:
    """Skip Power BI system/internal tables."""
    name_lower = table.name.lower()
    return name_lower.startswith("localdate") or name_lower.startswith("dateautotemplate")


async def analyze_m_queries_with_llm(
    model: PBIModel,
    endpoint_name: str | None = None,
) -> MQueryAnalysis:
    """Two-pass analysis: heuristic first, then LLM for manual/partial steps.

    Pass 1 (instant): Run the heuristic engine on all tables
    Pass 2 (AI): For tables with manual or partial steps, call the LLM to:
      a) Translate individual unconverted steps
      b) Review and enhance the overall SQL translation
    """
    from .llm_m_translator import translate_m_steps_with_llm, enhance_full_query_with_llm

    analysis = analyze_m_queries(model)
    analysis.ai_enhanced = True

    tables_needing_ai = [
        t for t in analysis.tables
        if t.has_m_query and t.tier in (ConversionTier.PARTIAL, ConversionTier.MANUAL)
        and not t.is_calculated_table
    ]

    auto_tables_with_steps = [
        t for t in analysis.tables
        if t.has_m_query and t.tier == ConversionTier.AUTO
        and t.step_count > 0
        and not t.is_calculated_table
    ]

    if not tables_needing_ai and not auto_tables_with_steps:
        return analysis

    import asyncio
    tasks = []

    for table_result in tables_needing_ai:
        parsed = parse_m_expression(table_result.original_m)
        unconverted_steps = [
            {
                "name": step.name,
                "expression": step.expression,
                "step_type": step.step_type.value,
            }
            for step in parsed.steps
            if step.step_type not in (MStepType.SOURCE, MStepType.NAVIGATION)
            and step.name in table_result.manual_step_names
        ]

        if unconverted_steps:
            tasks.append((
                table_result,
                "translate_steps",
                translate_m_steps_with_llm(
                    table_name=table_result.table_name,
                    full_m_query=table_result.original_m,
                    steps_to_translate=unconverted_steps,
                    source_fqn=table_result.source_fqn,
                    heuristic_sql=table_result.generated_sql,
                    endpoint_name=endpoint_name,
                ),
            ))

        tasks.append((
            table_result,
            "enhance",
            enhance_full_query_with_llm(
                table_name=table_result.table_name,
                full_m_query=table_result.original_m,
                heuristic_sql=table_result.generated_sql,
                source_fqn=table_result.source_fqn,
                endpoint_name=endpoint_name,
            ),
        ))

    for table_result in auto_tables_with_steps:
        tasks.append((
            table_result,
            "enhance",
            enhance_full_query_with_llm(
                table_name=table_result.table_name,
                full_m_query=table_result.original_m,
                heuristic_sql=table_result.generated_sql,
                source_fqn=table_result.source_fqn,
                endpoint_name=endpoint_name,
            ),
        ))

    coros = [t[2] for t in tasks]
    results = await asyncio.gather(*coros, return_exceptions=True)

    for (table_result, task_type, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            logger.warning("AI task '%s' failed for %s: %s", task_type, table_result.table_name, result)
            table_result.warnings.append(f"AI {task_type} failed: {result}")
            continue

        if task_type == "translate_steps":
            table_result.ai_step_translations = result
            ai_translated = [r for r in result if r.get("sql")]
            if ai_translated:
                table_result.ai_enhanced = True
                formerly_manual = table_result.manual_steps
                ai_solved = sum(1 for r in ai_translated if r.get("confidence") in ("high", "medium"))
                if ai_solved > 0:
                    table_result.manual_steps = max(0, formerly_manual - ai_solved)
                    table_result.auto_steps += ai_solved
                    if table_result.manual_steps == 0 and table_result.partial_steps == 0:
                        table_result.tier = ConversionTier.AUTO
                    elif table_result.manual_steps == 0:
                        table_result.tier = ConversionTier.PARTIAL

        elif task_type == "enhance":
            if not isinstance(result, dict):
                continue
            enhanced_sql = result.get("enhanced_sql", "")
            changes = result.get("changes", [])
            confidence = result.get("confidence", "none")

            if enhanced_sql and changes:
                table_result.ai_enhanced = True
                table_result.ai_sql = enhanced_sql
                table_result.ai_changes = changes
                table_result.ai_confidence = confidence

    _recompute_scores(analysis)
    return analysis


def _recompute_scores(analysis: MQueryAnalysis) -> None:
    """Recompute counts and score after AI enhancement."""
    analysis.auto_count = 0
    analysis.partial_count = 0
    analysis.manual_count = 0

    for t in analysis.tables:
        if not t.has_m_query and not t.is_calculated_table:
            continue
        if t.is_calculated_table:
            analysis.manual_count += 1
        elif t.tier == ConversionTier.AUTO:
            analysis.auto_count += 1
        elif t.tier == ConversionTier.PARTIAL:
            analysis.partial_count += 1
        else:
            analysis.manual_count += 1

    scored = analysis.auto_count + analysis.partial_count + analysis.manual_count
    if scored > 0:
        auto_weight = analysis.auto_count * 1.0
        partial_weight = analysis.partial_count * 0.5
        analysis.migration_score = round(
            (auto_weight + partial_weight) / scored * 100, 1
        )
