"""Health score computation and report generation."""

from __future__ import annotations

from .parsers.models import (
    CategoryScore,
    Finding,
    HealthReport,
    Severity,
)

class _CatConfig:
    __slots__ = ("display_name", "weight")
    def __init__(self, display_name: str, weight: float) -> None:
        self.display_name = display_name
        self.weight = weight

CATEGORY_CONFIG: dict[str, _CatConfig] = {
    "data_model": _CatConfig("Data Model", 0.20),
    "dax_quality": _CatConfig("DAX Quality & Pushdown", 0.15),
    "storage_modes": _CatConfig("Storage Modes", 0.15),
    "parallelization": _CatConfig("Parallelization & Concurrency", 0.10),
    "report_design": _CatConfig("Report Design", 0.10),
    "connectivity": _CatConfig("Connectivity", 0.10),
    "dbsql_performance": _CatConfig("Databricks SQL Performance", 0.20),
    "uc_metrics_feasibility": _CatConfig("UC Metrics Migration", 0.0),
}

SEVERITY_DEDUCTIONS = {
    Severity.ERROR: 15,
    Severity.WARNING: 8,
    Severity.INFO: 3,
}


def compute_health_report(
    findings: list[Finding],
    mode: str = "file-only",
) -> HealthReport:
    """Compute a scored health report from findings."""

    findings_by_category: dict[str, list[Finding]] = {}
    for f in findings:
        findings_by_category.setdefault(f.category, []).append(f)

    categories: list[CategoryScore] = []

    for cat_id, config in CATEGORY_CONFIG.items():
        cat_findings = findings_by_category.get(cat_id, [])

        assessed = True
        if cat_id == "dbsql_performance" and "queries" not in mode and "profile" not in mode and mode != "live":
            assessed = False
        if cat_id == "report_design" and mode not in ("pbix", "pbix+queries", "pbix+profile"):
            assessed = len(cat_findings) > 0
        if mode == "profile" and cat_id != "dbsql_performance":
            assessed = False
        if cat_id == "uc_metrics_feasibility":
            assessed = mode != "profile" and len(cat_findings) > 0

        score = 100.0
        for f in cat_findings:
            deduction = SEVERITY_DEDUCTIONS.get(f.severity, 0)
            score -= deduction
        score = max(0.0, score)

        categories.append(CategoryScore(
            category=cat_id,
            display_name=config.display_name,
            score=round(score, 1),
            findings=cat_findings,
            assessed=assessed,
        ))

    assessed_cats = [c for c in categories if c.assessed]
    if assessed_cats:
        total_weight = sum(
            CATEGORY_CONFIG[c.category].weight for c in assessed_cats
        )
        if total_weight > 0:
            overall = sum(
                c.score * CATEGORY_CONFIG[c.category].weight / total_weight
                for c in assessed_cats
            )
        else:
            overall = 100.0
    else:
        overall = 100.0

    error_count = sum(1 for f in findings if f.severity == Severity.ERROR)
    warning_count = sum(1 for f in findings if f.severity == Severity.WARNING)
    info_count = sum(1 for f in findings if f.severity == Severity.INFO)

    return HealthReport(
        overall_score=round(overall, 1),
        categories=categories,
        total_findings=len(findings),
        error_count=error_count,
        warning_count=warning_count,
        info_count=info_count,
        mode=mode,
    )
