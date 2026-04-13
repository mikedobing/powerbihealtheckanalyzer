"""Normalized Pydantic models for Power BI model metadata."""

from __future__ import annotations

from enum import Enum
from pydantic import BaseModel, Field


class StorageMode(str, Enum):
    IMPORT = "import"
    DIRECT_QUERY = "directQuery"
    DUAL = "dual"
    DEFAULT = "default"


class CrossFilterDirection(str, Enum):
    ONE = "oneDirection"
    BOTH = "bothDirections"
    AUTOMATIC = "automatic"


class RelationshipCardinality(str, Enum):
    ONE_TO_ONE = "oneToOne"
    ONE_TO_MANY = "oneToMany"
    MANY_TO_ONE = "manyToOne"
    MANY_TO_MANY = "manyToMany"


class Column(BaseModel):
    name: str
    data_type: str = ""
    is_hidden: bool = False
    sort_by_column: str | None = None
    source_column: str | None = None


class Measure(BaseModel):
    name: str
    expression: str = ""
    description: str = ""
    format_string: str = ""
    is_hidden: bool = False


class Partition(BaseModel):
    name: str
    source_type: str = ""
    query: str = ""
    mode: StorageMode = StorageMode.DEFAULT


class Table(BaseModel):
    name: str
    columns: list[Column] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    partitions: list[Partition] = Field(default_factory=list)
    storage_mode: StorageMode = StorageMode.DEFAULT
    is_hidden: bool = False
    is_calculated: bool = False
    calculated_table_expression: str | None = None


class Relationship(BaseModel):
    name: str = ""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cross_filter_direction: CrossFilterDirection = CrossFilterDirection.ONE
    from_cardinality: str = "many"
    to_cardinality: str = "one"
    is_active: bool = True


class DataSource(BaseModel):
    name: str = ""
    connection_string: str = ""
    provider: str = ""
    auth_kind: str = ""


class ModelAnnotation(BaseModel):
    name: str
    value: str = ""


class ReportVisual(BaseModel):
    visual_type: str = ""
    title: str = ""


class ReportPage(BaseModel):
    name: str
    display_name: str = ""
    visuals: list[ReportVisual] = Field(default_factory=list)


class ReportLayout(BaseModel):
    pages: list[ReportPage] = Field(default_factory=list)


class PBIModel(BaseModel):
    """Normalized Power BI semantic model extracted from .bim or .pbix."""

    name: str = ""
    compatibility_level: int = 0
    tables: list[Table] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    data_sources: list[DataSource] = Field(default_factory=list)
    annotations: list[ModelAnnotation] = Field(default_factory=list)
    report_layout: ReportLayout | None = None
    parse_warnings: list[str] = Field(default_factory=list)

    @property
    def fact_tables(self) -> list[Table]:
        """Tables that appear on the 'many' side of relationships."""
        many_side_tables = set()
        for rel in self.relationships:
            if rel.from_cardinality == "many":
                many_side_tables.add(rel.from_table)
            if rel.to_cardinality == "many":
                many_side_tables.add(rel.to_table)
        return [t for t in self.tables if t.name in many_side_tables]

    @property
    def dimension_tables(self) -> list[Table]:
        """Tables that appear on the 'one' side of relationships."""
        one_side_tables = set()
        for rel in self.relationships:
            if rel.from_cardinality == "one":
                one_side_tables.add(rel.from_table)
            if rel.to_cardinality == "one":
                one_side_tables.add(rel.to_table)
        return [t for t in self.tables if t.name in one_side_tables]


class QueryRecord(BaseModel):
    """A single query record from DBSQL query history."""

    query_id: str = ""
    query_text: str = ""
    status: str = ""
    duration_seconds: float = 0.0
    rows_produced: int = 0
    rows_read: int = 0
    bytes_read: int = 0
    warehouse_id: str = ""
    statement_type: str = ""
    start_time: str = ""
    end_time: str = ""
    error_message: str = ""


class QueryHistoryData(BaseModel):
    """Collection of query records for analysis."""

    queries: list[QueryRecord] = Field(default_factory=list)


# ── Query Profile models (execution plan from DBSQL Query Profile UI) ──


class QueryProfileNodeMetric(BaseModel):
    name: str = ""
    value: str = ""


class QueryProfileNode(BaseModel):
    id: str
    name: str = ""
    tag: str = ""
    hidden: bool = False
    metadata: dict[str, str] = Field(default_factory=dict)
    key_metrics: list[QueryProfileNodeMetric] = Field(default_factory=list)
    rows: int = 0
    duration_ms: int = 0
    peak_memory_bytes: int = 0
    is_photon: bool = False


class QueryProfileMetrics(BaseModel):
    total_time_ms: int = 0
    compilation_time_ms: int = 0
    execution_time_ms: int = 0
    query_execution_time_ms: int = 0
    read_bytes: int = 0
    rows_read: int = 0
    rows_produced: int = 0
    spill_to_disk_bytes: int = 0
    write_remote_bytes: int = 0
    read_cache_bytes: int = 0
    cache_hit_pct: float = 0.0
    result_from_cache: bool = False
    photon_time_ms: int = 0
    task_total_time_ms: int = 0
    read_files_count: int = 0
    read_partitions_count: int = 0
    metadata_time_ms: int = 0
    network_sent_bytes: int = 0


class QueryProfileEdge(BaseModel):
    source: str = ""
    target: str = ""


class QueryProfile(BaseModel):
    """Parsed DBSQL Query Profile export (single-query execution plan)."""

    query_id: str = ""
    query_text: str = ""
    status: str = ""
    endpoint_id: str = ""
    statement_type: str = ""
    dbsql_version: str = ""
    metrics: QueryProfileMetrics = Field(default_factory=QueryProfileMetrics)
    nodes: list[QueryProfileNode] = Field(default_factory=list)
    edges: list[QueryProfileEdge] = Field(default_factory=list)
    tables_scanned: list[str] = Field(default_factory=list)
    join_types: list[str] = Field(default_factory=list)
    has_nested_loop_join: bool = False
    has_subquery: bool = False
    shuffle_count: int = 0
    aggregate_count: int = 0
    sort_merge_join_count: int = 0
    photon_node_count: int = 0
    non_photon_node_count: int = 0
    filter_nodes_above_scan: int = 0
    is_pbi_generated: bool = False


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Impact(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Finding(BaseModel):
    """A single health check finding."""

    rule_id: str
    category: str
    name: str
    severity: Severity
    description: str
    recommendation: str
    impact: Impact = Impact.MEDIUM
    reference_url: str = ""
    details: dict = Field(default_factory=dict)


class CategoryScore(BaseModel):
    category: str
    display_name: str
    score: float
    max_score: float = 100.0
    findings: list[Finding] = Field(default_factory=list)
    assessed: bool = True


class HealthReport(BaseModel):
    """Complete health check report."""

    overall_score: float = 100.0
    categories: list[CategoryScore] = Field(default_factory=list)
    total_findings: int = 0
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    mode: str = "file-only"
