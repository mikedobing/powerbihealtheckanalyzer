"""Report design checks (PBIX only)."""

from __future__ import annotations

from ...parsers.models import Finding, Impact, PBIModel, Severity


def run_report_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    if not model.report_layout:
        return findings

    pages = model.report_layout.pages
    max_visuals = thresholds.get("report_too_many_visuals", {}).get("max_visuals", 20)
    max_pages = thresholds.get("report_many_pages", {}).get("max_pages", 15)

    for page in pages:
        visual_count = len(page.visuals)
        if visual_count > max_visuals:
            page_label = page.display_name or page.name
            findings.append(Finding(
                rule_id="report_too_many_visuals",
                category="report_design",
                name="Too Many Visuals Per Page",
                severity=Severity.WARNING,
                description=(
                    f"Page '{page_label}' has {visual_count} visuals "
                    f"(threshold: {max_visuals})"
                ),
                recommendation=(
                    "Split into multiple focused pages or use bookmarks/drill-through "
                    "to reduce concurrent query load."
                ),
                impact=Impact.HIGH,
                details={"page": page_label, "visual_count": visual_count},
            ))

    if len(pages) > max_pages:
        findings.append(Finding(
            rule_id="report_many_pages",
            category="report_design",
            name="High Page Count",
            severity=Severity.INFO,
            description=f"Report has {len(pages)} pages (threshold: {max_pages})",
            recommendation="Consider consolidating related pages or splitting into separate reports.",
            impact=Impact.LOW,
            details={"page_count": len(pages)},
        ))

    return findings
