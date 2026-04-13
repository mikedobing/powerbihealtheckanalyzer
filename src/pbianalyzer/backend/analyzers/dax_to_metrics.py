"""Feasibility analyzer for converting PBI DAX measures to UC Metric Views."""

from __future__ import annotations

from ..parsers.models import Measure, PBIModel, Relationship, Table
from .dax_patterns import (
    ALL_PATTERNS,
    DIRECT_PATTERNS,
    MANUAL_PATTERNS,
    TRANSLATABLE_PATTERNS,
    DaxPattern,
    Tier,
    extract_column_ref,
    translate_direct_aggregate,
    translate_expression,
)


class MeasureClassification:
    __slots__ = ("measure_name", "table_name", "dax_expression", "tier",
                 "sql_expression", "notes", "pattern_matched")

    def __init__(
        self,
        measure_name: str,
        table_name: str,
        dax_expression: str,
        tier: Tier,
        sql_expression: str | None,
        notes: str,
        pattern_matched: str,
    ):
        self.measure_name = measure_name
        self.table_name = table_name
        self.dax_expression = dax_expression
        self.tier = tier
        self.sql_expression = sql_expression
        self.notes = notes
        self.pattern_matched = pattern_matched

    def to_dict(self) -> dict:
        return {
            "measure_name": self.measure_name,
            "table_name": self.table_name,
            "dax_expression": self.dax_expression,
            "tier": self.tier,
            "sql_expression": self.sql_expression,
            "notes": self.notes,
            "pattern_matched": self.pattern_matched,
        }


class ProposedDimension:
    __slots__ = ("name", "expr", "source_table", "comment")

    def __init__(self, name: str, expr: str, source_table: str, comment: str = ""):
        self.name = name
        self.expr = expr
        self.source_table = source_table
        self.comment = comment

    def to_dict(self) -> dict:
        return {"name": self.name, "expr": self.expr, "source_table": self.source_table, "comment": self.comment}


class ProposedJoin:
    __slots__ = ("name", "source", "on_clause")

    def __init__(self, name: str, source: str, on_clause: str):
        self.name = name
        self.source = source
        self.on_clause = on_clause

    def to_dict(self) -> dict:
        return {"name": self.name, "source": self.source, "on": self.on_clause}


class MetricsAnalysis:
    def __init__(self) -> None:
        self.source_table: str = ""
        self.classifications: list[MeasureClassification] = []
        self.proposed_dimensions: list[ProposedDimension] = []
        self.proposed_joins: list[ProposedJoin] = []
        self.direct_count: int = 0
        self.translatable_count: int = 0
        self.manual_count: int = 0
        self.feasibility_score: float = 0.0
        self.warnings: list[str] = []

    def to_dict(self) -> dict:
        return {
            "source_table": self.source_table,
            "classifications": [c.to_dict() for c in self.classifications],
            "proposed_dimensions": [d.to_dict() for d in self.proposed_dimensions],
            "proposed_joins": [j.to_dict() for j in self.proposed_joins],
            "direct_count": self.direct_count,
            "translatable_count": self.translatable_count,
            "manual_count": self.manual_count,
            "feasibility_score": self.feasibility_score,
            "warnings": self.warnings,
        }


def classify_measure(measure: Measure, table: Table, model: PBIModel) -> MeasureClassification:
    """Classify a single DAX measure by UC Metric View convertibility.

    Priority: manual patterns first (to catch complex expressions that happen to
    contain simple aggregates), then check for pure direct aggregates, then
    translatable patterns, then fallback.
    """
    expr = measure.expression.strip()
    if not expr:
        return MeasureClassification(
            measure_name=measure.name,
            table_name=table.name,
            dax_expression=expr,
            tier="manual",
            sql_expression=None,
            notes="Empty measure expression",
            pattern_matched="empty",
        )

    for pat in MANUAL_PATTERNS:
        if pat.match(expr):
            return MeasureClassification(
                measure_name=measure.name,
                table_name=table.name,
                dax_expression=expr,
                tier="manual",
                sql_expression=None,
                notes=pat.notes,
                pattern_matched=pat.name,
            )

    for pat in TRANSLATABLE_PATTERNS:
        if pat.match(expr):
            sql = translate_expression(expr)
            notes = pat.notes
            if sql:
                notes = f"Auto-translated ({pat.name})"
            return MeasureClassification(
                measure_name=measure.name,
                table_name=table.name,
                dax_expression=expr,
                tier="translatable",
                sql_expression=sql,
                notes=notes,
                pattern_matched=pat.name,
            )

    direct = translate_direct_aggregate(expr)
    if direct:
        sql, pat_name = direct
        return MeasureClassification(
            measure_name=measure.name,
            table_name=table.name,
            dax_expression=expr,
            tier="direct",
            sql_expression=sql,
            notes=f"Direct 1:1 mapping via {pat_name}",
            pattern_matched=pat_name,
        )

    for pat in DIRECT_PATTERNS:
        if pat.match(expr):
            sql = None
            if pat.sql_template:
                col = extract_column_ref(expr) or "?"
                sql = pat.sql_template.replace("{col}", col)
            return MeasureClassification(
                measure_name=measure.name,
                table_name=table.name,
                dax_expression=expr,
                tier="direct",
                sql_expression=sql,
                notes=pat.notes,
                pattern_matched=pat.name,
            )

    if _looks_like_simple_ratio(expr):
        sql = translate_expression(expr)
        return MeasureClassification(
            measure_name=measure.name,
            table_name=table.name,
            dax_expression=expr,
            tier="translatable",
            sql_expression=sql,
            notes="Auto-translated (RATIO_HEURISTIC)" if sql else "Ratio of aggregates -- review generated SQL",
            pattern_matched="RATIO_HEURISTIC",
        )

    return MeasureClassification(
        measure_name=measure.name,
        table_name=table.name,
        dax_expression=expr,
        tier="manual",
        sql_expression=None,
        notes="No recognized pattern -- requires manual conversion",
        pattern_matched="unknown",
    )


def _looks_like_simple_ratio(expr: str) -> bool:
    """Heuristic: expression contains a division of two aggregate-like calls."""
    import re
    agg_re = r"\b(SUM|COUNT|DISTINCTCOUNT|AVERAGE|MIN|MAX|COUNTA)\s*\("
    matches = re.findall(agg_re, expr, re.IGNORECASE)
    return len(matches) >= 2 and "/" in expr


def _identify_source_table(model: PBIModel) -> str:
    """Pick the best fact table (source) for the metric view."""
    facts = model.fact_tables
    if facts:
        best = max(facts, key=lambda t: len(t.measures))
        return best.name

    tables_with_measures = [t for t in model.tables if t.measures and not t.is_hidden]
    if tables_with_measures:
        return max(tables_with_measures, key=lambda t: len(t.measures)).name

    if model.tables:
        return model.tables[0].name
    return "unknown"


def _build_joins(model: PBIModel, source_table: str) -> list[ProposedJoin]:
    """Map PBI relationships into UC Metric View join definitions."""
    joins: list[ProposedJoin] = []
    seen: set[str] = set()

    for rel in model.relationships:
        if not rel.is_active:
            continue

        if rel.from_table == source_table:
            dim_table = rel.to_table
            on_clause = f"source.{rel.from_column} = {_alias(dim_table)}.{rel.to_column}"
        elif rel.to_table == source_table:
            dim_table = rel.from_table
            on_clause = f"source.{rel.to_column} = {_alias(dim_table)}.{rel.from_column}"
        else:
            continue

        alias = _alias(dim_table)
        if alias in seen:
            continue
        seen.add(alias)

        joins.append(ProposedJoin(
            name=alias,
            source=f"{{catalog}}.{{schema}}.{_snake(dim_table)}",
            on_clause=on_clause,
        ))

    return joins


def _build_dimensions(model: PBIModel, source_table: str, joins: list[ProposedJoin]) -> list[ProposedDimension]:
    """Propose dimensions from dimension table columns and source date columns."""
    dims: list[ProposedDimension] = []
    join_aliases = {j.name for j in joins}

    for table in model.tables:
        alias = _alias(table.name)
        is_source = table.name == source_table
        is_joined = alias in join_aliases

        if not is_source and not is_joined:
            continue

        for col in table.columns:
            if col.is_hidden:
                continue

            dtype = col.data_type.lower()
            if is_source and ("date" in dtype or "date" in col.name.lower()):
                dims.append(ProposedDimension(
                    name=col.name,
                    expr=f"DATE_TRUNC('MONTH', source.{col.name})" if "date" in dtype else f"source.{col.name}",
                    source_table=table.name,
                    comment=f"Date dimension from {table.name}",
                ))
            elif is_joined and not is_source:
                if dtype in ("string", "text", "varchar", "nvarchar", ""):
                    if not _looks_like_key(col.name):
                        dims.append(ProposedDimension(
                            name=col.name,
                            expr=f"{alias}.{col.name}",
                            source_table=table.name,
                            comment=f"Dimension from {table.name}",
                        ))

    return dims


def _looks_like_key(name: str) -> bool:
    lower = name.lower()
    return lower.endswith("id") or lower.endswith("key") or lower.endswith("_pk") or lower.endswith("_fk")


def _alias(table_name: str) -> str:
    return table_name.lower().replace(" ", "_")


def _snake(name: str) -> str:
    import re
    s = re.sub(r"[^\w]+", "_", name).strip("_")
    return s.lower()


def analyze_model_for_metrics(model: PBIModel) -> MetricsAnalysis:
    """Analyze an entire PBI model for UC Metric View migration feasibility."""
    analysis = MetricsAnalysis()

    source_table = _identify_source_table(model)
    analysis.source_table = source_table

    for table in model.tables:
        for measure in table.measures:
            if measure.is_hidden:
                continue
            cls = classify_measure(measure, table, model)
            analysis.classifications.append(cls)

    analysis.direct_count = sum(1 for c in analysis.classifications if c.tier == "direct")
    analysis.translatable_count = sum(1 for c in analysis.classifications if c.tier == "translatable")
    analysis.manual_count = sum(1 for c in analysis.classifications if c.tier == "manual")

    total = len(analysis.classifications)
    if total > 0:
        convertible = analysis.direct_count + analysis.translatable_count
        analysis.feasibility_score = round(convertible / total * 100, 1)
    else:
        analysis.feasibility_score = 0.0

    analysis.proposed_joins = _build_joins(model, source_table)
    analysis.proposed_dimensions = _build_dimensions(model, source_table, analysis.proposed_joins)

    if not model.relationships:
        analysis.warnings.append("No relationships found -- metric view will be single-table without joins")
    if analysis.manual_count > 0:
        analysis.warnings.append(
            f"{analysis.manual_count} measure(s) require manual conversion and are excluded from generated YAML"
        )
    if not analysis.proposed_dimensions:
        analysis.warnings.append("No dimensions auto-detected -- add dimensions manually to the generated YAML")

    calc_tables = [t for t in model.tables if t.is_calculated]
    if calc_tables:
        analysis.warnings.append(
            f"{len(calc_tables)} calculated table(s) found ({', '.join(t.name for t in calc_tables)}) "
            "-- these cannot be represented in a metric view and may need to be materialized as Delta tables"
        )

    return analysis
