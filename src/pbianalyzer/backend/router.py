from __future__ import annotations

import json
import logging

from fastapi import File, Form, UploadFile
from fastapi.responses import Response

from .core import Dependencies, create_router
from .models import (
    AIStepTranslation,
    AnalysisResponse,
    ExportQueryResponse,
    MeasureClassificationOut,
    MetricsAnalysisResponse,
    MetricViewGenerateResponse,
    MQueryAnalysisResponse,
    PipelineBundleSummary,
    ProposedDimensionOut,
    ProposedJoinOut,
    QueryProfileSummary,
    SourceInfo,
    TableMigrationOut,
    VersionOut,
)
from .parsers import parse_bim, parse_pbip_zip, parse_pbix, parse_query_json
from .parsers.models import HealthReport, PBIModel, QueryHistoryData, QueryProfile
from .parsers.query_profile import is_query_profile, parse_query_profile_dict
from .rules.engine import RuleEngine
from .scoring import compute_health_report
from .analyzers.dbsql import build_export_query, fetch_query_history_live
from .analyzers.llm_query_analyzer import analyze_with_llm
from .analyzers.dax_to_metrics import analyze_model_for_metrics
from .analyzers.metrics_converter import generate_metric_view_yaml
from .analyzers.m_query_analyzer import analyze_m_queries, analyze_m_queries_with_llm
from .analyzers.pipeline_generator import generate_pipeline_bundle, bundle_to_zip

logger = logging.getLogger(__name__)

router = create_router()
engine = RuleEngine()


@router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


def _parse_json_upload(content: bytes) -> tuple[QueryHistoryData | None, QueryProfile | None]:
    """Auto-detect whether uploaded JSON is query history or query profile."""
    text = content.decode("utf-8-sig") if isinstance(content, bytes) else content
    data = json.loads(text)

    if is_query_profile(data):
        profile = parse_query_profile_dict(data)
        return None, profile

    query_data = parse_query_json(content)
    return query_data, None


def _build_profile_summary(
    profile: QueryProfile, has_llm: bool = False,
) -> QueryProfileSummary:
    return QueryProfileSummary(
        query_id=profile.query_id,
        status=profile.status,
        total_time_ms=profile.metrics.total_time_ms,
        rows_read=profile.metrics.rows_read,
        rows_produced=profile.metrics.rows_produced,
        read_bytes=profile.metrics.read_bytes,
        tables_scanned=profile.tables_scanned,
        join_types=profile.join_types,
        is_pbi_generated=profile.is_pbi_generated,
        has_llm_analysis=has_llm,
    )


@router.post("/analyze", response_model=AnalysisResponse, operation_id="analyzeFiles")
async def analyze_files(
    model_file: UploadFile = File(..., description="BIM or PBIX file"),
    query_file: UploadFile | None = File(None, description="Optional DBSQL query JSON"),
    llm_endpoint: str = Form("", description="Optional serving endpoint for AI analysis"),
):
    """Analyze uploaded Power BI model file with optional query data."""
    content = await model_file.read()
    filename = model_file.filename or ""

    lower_name = filename.lower()
    if lower_name.endswith(".pbix") or lower_name.endswith(".pbit"):
        model = parse_pbix(content)
        mode = "pbix"
    elif lower_name.endswith(".zip"):
        model = parse_pbip_zip(content)
        mode = "pbip"
    else:
        model = parse_bim(content)
        mode = "bim"

    query_data: QueryHistoryData | None = None
    query_profile: QueryProfile | None = None
    profile_summary: QueryProfileSummary | None = None

    if query_file:
        query_content = await query_file.read()
        if query_content:
            query_data, query_profile = _parse_json_upload(query_content)
            if query_data:
                mode = f"{mode}+queries"
            elif query_profile:
                mode = f"{mode}+profile"

    findings = engine.analyze(
        model=model,
        query_data=query_data,
        query_profile=query_profile,
    )

    has_llm = False
    if query_profile and llm_endpoint:
        try:
            llm_findings = await analyze_with_llm(
                query_profile, endpoint_name=llm_endpoint
            )
            real_llm = [f for f in llm_findings if f.rule_id != "llm_analysis_error"]
            findings.extend(llm_findings)
            has_llm = len(real_llm) > 0
        except Exception as exc:
            logger.warning("LLM analysis failed: %s", exc)

    if query_profile:
        profile_summary = _build_profile_summary(query_profile, has_llm=has_llm)

    report = compute_health_report(findings, mode=mode)
    total_measures = sum(len(t.measures) for t in model.tables)

    return AnalysisResponse(
        report=report,
        model_name=model.name or filename,
        tables_count=len(model.tables),
        relationships_count=len(model.relationships),
        measures_count=total_measures,
        has_report_layout=model.report_layout is not None,
        parse_warnings=model.parse_warnings,
        query_profile_summary=profile_summary,
    )


@router.post("/analyze-profile", response_model=AnalysisResponse, operation_id="analyzeProfile")
async def analyze_profile_only(
    query_file: UploadFile = File(..., description="DBSQL Query Profile JSON"),
    llm_endpoint: str = Form("", description="Optional serving endpoint for AI analysis"),
):
    """Analyze a DBSQL Query Profile JSON without a model file (standalone mode)."""
    content = await query_file.read()
    query_data, query_profile = _parse_json_upload(content)

    if query_profile is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is not a DBSQL Query Profile. Expected format with 'graphs' and 'query' keys.",
        )

    model = PBIModel()
    findings = engine.analyze(query_profile=query_profile)

    has_llm = False
    if llm_endpoint:
        try:
            llm_findings = await analyze_with_llm(
                query_profile, endpoint_name=llm_endpoint,
            )
            real_llm = [f for f in llm_findings if f.rule_id != "llm_analysis_error"]
            findings.extend(llm_findings)
            has_llm = len(real_llm) > 0
        except Exception as exc:
            logger.warning("LLM analysis failed: %s", exc)

    profile_summary = _build_profile_summary(query_profile, has_llm=has_llm)
    report = compute_health_report(findings, mode="profile")

    filename = query_file.filename or "Query Profile"
    return AnalysisResponse(
        report=report,
        model_name=filename,
        tables_count=0,
        relationships_count=0,
        measures_count=0,
        has_report_layout=False,
        parse_warnings=[],
        query_profile_summary=profile_summary,
    )


@router.post("/analyze-live", response_model=AnalysisResponse, operation_id="analyzeLive")
async def analyze_live(
    model_file: UploadFile = File(..., description="BIM or PBIX file"),
    warehouse_id: str = Form(..., description="Databricks SQL warehouse ID"),
    days: int = Form(7, description="Days of query history to analyze"),
    ws: Dependencies.Client = None,  # type: ignore[assignment]
):
    """Analyze uploaded model file plus live Databricks SQL telemetry."""
    content = await model_file.read()
    filename = model_file.filename or ""

    lower_name = filename.lower()
    if lower_name.endswith(".pbix") or lower_name.endswith(".pbit"):
        model = parse_pbix(content)
    elif lower_name.endswith(".zip"):
        model = parse_pbip_zip(content)
    else:
        model = parse_bim(content)

    query_data = await fetch_query_history_live(ws, warehouse_id, days=days)

    findings = engine.analyze(model=model, query_data=query_data)
    report = compute_health_report(findings, mode="live")

    total_measures = sum(len(t.measures) for t in model.tables)

    return AnalysisResponse(
        report=report,
        model_name=model.name or filename,
        tables_count=len(model.tables),
        relationships_count=len(model.relationships),
        measures_count=total_measures,
        has_report_layout=model.report_layout is not None,
        parse_warnings=model.parse_warnings,
    )


@router.get("/export-query", response_model=ExportQueryResponse, operation_id="getExportQuery")
async def get_export_query(warehouse_id: str = "", days: int = 7):
    """Get the SQL query template for customers to export their query history."""
    sql = build_export_query(
        warehouse_id=warehouse_id or "<your_warehouse_id>",
        days=days,
    )
    return ExportQueryResponse(
        sql=sql,
        description=(
            "Run this query against your Databricks SQL warehouse to export "
            "query history. Save the results as JSON and upload to the analyzer."
        ),
    )


def _parse_model_file(content: bytes, filename: str) -> PBIModel:
    """Parse a PBI model file by extension."""
    lower = filename.lower()
    if lower.endswith(".pbix") or lower.endswith(".pbit"):
        return parse_pbix(content)
    if lower.endswith(".zip"):
        return parse_pbip_zip(content)
    return parse_bim(content)


@router.post("/analyze-metrics", response_model=MetricsAnalysisResponse, operation_id="analyzeMetrics")
async def analyze_metrics(
    model_file: UploadFile = File(..., description="BIM, PBIX, or PBIP file"),
):
    """Analyze a PBI model for UC Metric View migration feasibility."""
    content = await model_file.read()
    model = _parse_model_file(content, model_file.filename or "")
    analysis = analyze_model_for_metrics(model)

    return MetricsAnalysisResponse(
        source_table=analysis.source_table,
        classifications=[
            MeasureClassificationOut(**c.to_dict()) for c in analysis.classifications
        ],
        proposed_dimensions=[
            ProposedDimensionOut(**d.to_dict()) for d in analysis.proposed_dimensions
        ],
        proposed_joins=[
            ProposedJoinOut(**j.to_dict()) for j in analysis.proposed_joins
        ],
        direct_count=analysis.direct_count,
        translatable_count=analysis.translatable_count,
        manual_count=analysis.manual_count,
        feasibility_score=analysis.feasibility_score,
        warnings=analysis.warnings,
    )


@router.post("/generate-metric-view", response_model=MetricViewGenerateResponse, operation_id="generateMetricView")
async def generate_metric_view(
    model_file: UploadFile = File(..., description="BIM, PBIX, or PBIP file"),
    catalog: str = Form("my_catalog", description="Target UC catalog name"),
    schema_name: str = Form("my_schema", description="Target UC schema name"),
    view_name: str = Form("", description="Optional metric view name"),
):
    """Generate a UC Metric View YAML definition from a PBI model."""
    content = await model_file.read()
    model = _parse_model_file(content, model_file.filename or "")
    analysis = analyze_model_for_metrics(model)

    yaml_body, sql, warnings = generate_metric_view_yaml(
        analysis, model,
        catalog=catalog,
        schema=schema_name,
        view_name=view_name or None,
    )

    return MetricViewGenerateResponse(
        yaml_content=yaml_body,
        sql_statement=sql,
        warnings=warnings,
    )


def _build_m_query_response(analysis) -> MQueryAnalysisResponse:
    """Build the MQueryAnalysisResponse from an MQueryAnalysis."""
    return MQueryAnalysisResponse(
        tables=[
            TableMigrationOut(
                table_name=t.table_name,
                has_m_query=t.has_m_query,
                source_type=t.source_type,
                source_fqn=t.source_fqn,
                tier=t.tier,
                generated_sql=t.generated_sql,
                step_count=t.step_count,
                auto_steps=t.auto_steps,
                partial_steps=t.partial_steps,
                manual_steps=t.manual_steps,
                manual_step_names=t.manual_step_names,
                warnings=t.warnings,
                is_calculated_table=t.is_calculated_table,
                is_databricks_source=t.is_databricks_source,
                suggested_layer=t.suggested_layer,
                original_m=t.original_m,
                ai_enhanced=t.ai_enhanced,
                ai_sql=t.ai_sql,
                ai_changes=t.ai_changes,
                ai_confidence=t.ai_confidence,
                ai_step_translations=[
                    AIStepTranslation(**s) for s in t.ai_step_translations
                ],
            )
            for t in analysis.tables
        ],
        total_tables=analysis.total_tables,
        tables_with_m=analysis.tables_with_m,
        auto_count=analysis.auto_count,
        partial_count=analysis.partial_count,
        manual_count=analysis.manual_count,
        databricks_source_count=analysis.databricks_source_count,
        non_databricks_source_count=analysis.non_databricks_source_count,
        migration_score=analysis.migration_score,
        unique_sources=[
            SourceInfo(fqn=s["fqn"], type=s["type"], is_databricks=s["is_databricks"])
            for s in analysis.unique_sources
        ],
        warnings=analysis.warnings,
        ai_enhanced=analysis.ai_enhanced,
    )


@router.post("/analyze-m-queries", response_model=MQueryAnalysisResponse, operation_id="analyzeMQueries")
async def analyze_m_queries_endpoint(
    model_file: UploadFile = File(..., description="BIM, PBIX, or PBIP file"),
    llm_endpoint: str = Form("", description="Override serving endpoint name (uses default if empty)"),
    skip_ai: bool = Form(False, description="Skip the AI enhancement pass"),
):
    """Analyze M queries in a PBI model for pipeline migration feasibility.

    Pass 1 (heuristic): instant regex-based conversion — always runs.
    Pass 2 (AI): LLM reviews and enhances translations — runs automatically unless skip_ai=true.
    """
    content = await model_file.read()
    model = _parse_model_file(content, model_file.filename or "")

    if skip_ai:
        analysis = analyze_m_queries(model)
    else:
        try:
            endpoint = llm_endpoint or None
            analysis = await analyze_m_queries_with_llm(model, endpoint_name=endpoint)
        except Exception as exc:
            logger.warning("AI-enhanced M analysis failed, falling back to heuristic: %s", exc)
            analysis = analyze_m_queries(model)
            analysis.warnings.append(f"AI enhancement failed ({exc}). Showing heuristic results only.")

    return _build_m_query_response(analysis)


@router.post("/generate-pipeline-bundle", operation_id="generatePipelineBundle")
async def generate_pipeline_bundle_endpoint(
    model_file: UploadFile = File(..., description="BIM, PBIX, or PBIP file"),
    catalog: str = Form("my_catalog", description="Target UC catalog name"),
    schema_name: str = Form("my_schema", description="Target UC schema name"),
    bundle_name: str = Form("", description="Optional bundle name"),
    include_metrics: bool = Form(True, description="Include UC Metric View in gold layer"),
):
    """Generate a DABs bundle ZIP from a PBI model (with automatic AI enhancement)."""
    content = await model_file.read()
    model = _parse_model_file(content, model_file.filename or "")

    try:
        m_analysis = await analyze_m_queries_with_llm(model)
    except Exception as exc:
        logger.warning("AI pass failed for bundle generation, using heuristic: %s", exc)
        m_analysis = analyze_m_queries(model)

    bundle = generate_pipeline_bundle(
        model=model,
        m_analysis=m_analysis,
        catalog=catalog,
        schema=schema_name,
        bundle_name=bundle_name or "",
        include_metric_view=include_metrics,
    )

    zip_bytes = bundle_to_zip(bundle)
    filename = f"{bundle.pipeline_name}_bundle.zip"

    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Bundle-Summary": json.dumps({
                "pipeline_name": bundle.pipeline_name,
                "table_count": bundle.table_count,
                "bronze_count": bundle.bronze_count,
                "silver_count": bundle.silver_count,
                "gold_count": bundle.gold_count,
                "manual_count": bundle.manual_count,
                "file_count": len(bundle.files),
                "warnings": bundle.warnings,
            }),
        },
    )


@router.post("/export-pdf", operation_id="exportPdf")
async def export_pdf(data: AnalysisResponse):
    """Generate a downloadable PDF report from analysis results."""
    from .pdf_report import generate_pdf

    pdf_bytes = generate_pdf(data)

    filename = (data.model_name or "health-check").replace(" ", "_")
    filename = f"pbi_health_check_{filename}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
