from typing import Literal

from pydantic import BaseModel, Field
from .. import __version__
from .parsers.models import (
    CategoryScore,
    Finding,
    HealthReport,
    Impact,
    Severity,
)


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class AnalysisRequest(BaseModel):
    mode: str = "file-only"
    warehouse_id: str | None = None
    days: int = 7


class QueryProfileSummary(BaseModel):
    query_id: str = ""
    status: str = ""
    total_time_ms: int = 0
    rows_read: int = 0
    rows_produced: int = 0
    read_bytes: int = 0
    tables_scanned: list[str] = Field(default_factory=list)
    join_types: list[str] = Field(default_factory=list)
    is_pbi_generated: bool = False
    has_llm_analysis: bool = False


class AnalysisResponse(BaseModel):
    report: HealthReport
    model_name: str = ""
    tables_count: int = 0
    relationships_count: int = 0
    measures_count: int = 0
    has_report_layout: bool = False
    parse_warnings: list[str] = Field(default_factory=list)
    query_profile_summary: QueryProfileSummary | None = None


class ExportQueryResponse(BaseModel):
    sql: str
    description: str


class MeasureClassificationOut(BaseModel):
    measure_name: str
    table_name: str
    dax_expression: str
    tier: Literal["direct", "translatable", "manual"]
    sql_expression: str | None = None
    notes: str = ""
    pattern_matched: str = ""


class ProposedDimensionOut(BaseModel):
    name: str
    expr: str
    source_table: str
    comment: str = ""


class ProposedJoinOut(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    source: str
    on_clause: str = Field(alias="on")


class MetricsAnalysisResponse(BaseModel):
    source_table: str = ""
    classifications: list[MeasureClassificationOut] = Field(default_factory=list)
    proposed_dimensions: list[ProposedDimensionOut] = Field(default_factory=list)
    proposed_joins: list[ProposedJoinOut] = Field(default_factory=list)
    direct_count: int = 0
    translatable_count: int = 0
    manual_count: int = 0
    feasibility_score: float = 0.0
    warnings: list[str] = Field(default_factory=list)


class MetricViewGenerateRequest(BaseModel):
    catalog: str = "my_catalog"
    schema_name: str = "my_schema"
    view_name: str = ""


class MetricViewGenerateResponse(BaseModel):
    yaml_content: str = ""
    sql_statement: str = ""
    warnings: list[str] = Field(default_factory=list)


# --- M Query / Pipeline Migration models ---


class AIStepTranslation(BaseModel):
    step_name: str = ""
    sql: str | None = None
    sql_type: str = ""
    notes: str = ""
    confidence: str = ""


class TableMigrationOut(BaseModel):
    table_name: str
    has_m_query: bool = False
    source_type: str = ""
    source_fqn: str = ""
    tier: Literal["auto", "partial", "manual"]
    generated_sql: str = ""
    step_count: int = 0
    auto_steps: int = 0
    partial_steps: int = 0
    manual_steps: int = 0
    manual_step_names: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    is_calculated_table: bool = False
    is_databricks_source: bool = False
    suggested_layer: str = "bronze"
    original_m: str = ""
    ai_enhanced: bool = False
    ai_sql: str = ""
    ai_changes: list[str] = Field(default_factory=list)
    ai_confidence: str = ""
    ai_step_translations: list[AIStepTranslation] = Field(default_factory=list)


class SourceInfo(BaseModel):
    fqn: str
    type: str
    is_databricks: bool = False


class MQueryAnalysisResponse(BaseModel):
    tables: list[TableMigrationOut] = Field(default_factory=list)
    total_tables: int = 0
    tables_with_m: int = 0
    auto_count: int = 0
    partial_count: int = 0
    manual_count: int = 0
    databricks_source_count: int = 0
    non_databricks_source_count: int = 0
    migration_score: float = 0.0
    unique_sources: list[SourceInfo] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    ai_enhanced: bool = False


class PipelineBundleSummary(BaseModel):
    pipeline_name: str = ""
    table_count: int = 0
    bronze_count: int = 0
    silver_count: int = 0
    gold_count: int = 0
    manual_count: int = 0
    file_count: int = 0
    warnings: list[str] = Field(default_factory=list)


__all__ = [
    "VersionOut",
    "AnalysisRequest",
    "AnalysisResponse",
    "ExportQueryResponse",
    "QueryProfileSummary",
    "MetricsAnalysisResponse",
    "MeasureClassificationOut",
    "ProposedDimensionOut",
    "ProposedJoinOut",
    "MetricViewGenerateRequest",
    "MetricViewGenerateResponse",
    "AIStepTranslation",
    "TableMigrationOut",
    "SourceInfo",
    "MQueryAnalysisResponse",
    "PipelineBundleSummary",
    "HealthReport",
    "CategoryScore",
    "Finding",
    "Severity",
    "Impact",
]
