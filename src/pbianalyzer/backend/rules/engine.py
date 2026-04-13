"""Rule engine that orchestrates all checks."""

from __future__ import annotations

from pathlib import Path

import yaml

from ..parsers.models import Finding, PBIModel, QueryHistoryData, QueryProfile
from .checks import (
    run_connectivity_checks,
    run_dax_checks,
    run_dbsql_checks,
    run_model_checks,
    run_parallelization_checks,
    run_query_profile_checks,
    run_report_checks,
    run_storage_checks,
)

DEFINITIONS_PATH = Path(__file__).parent / "definitions.yaml"


class RuleEngine:
    """Loads rule definitions and runs all checks against provided data."""

    def __init__(self) -> None:
        self.thresholds = self._load_thresholds()

    def _load_thresholds(self) -> dict:
        """Extract threshold values from rule definitions, keyed by rule ID."""
        with open(DEFINITIONS_PATH) as f:
            data = yaml.safe_load(f)
        thresholds: dict = {}
        for rule in data.get("rules", []):
            if "threshold" in rule:
                thresholds[rule["id"]] = rule["threshold"]
        return thresholds

    def analyze_model(self, model: PBIModel) -> list[Finding]:
        """Run all model-based checks (BIM/PBIX data)."""
        findings: list[Finding] = []
        findings.extend(run_model_checks(model, self.thresholds))
        findings.extend(run_dax_checks(model, self.thresholds))
        findings.extend(run_storage_checks(model, self.thresholds))
        findings.extend(run_parallelization_checks(model, self.thresholds))
        findings.extend(run_report_checks(model, self.thresholds))
        findings.extend(run_connectivity_checks(model, self.thresholds))
        return findings

    def analyze_queries(self, query_data: QueryHistoryData) -> list[Finding]:
        """Run DBSQL performance checks on query history data."""
        return run_dbsql_checks(query_data, self.thresholds)

    def analyze_query_profile(self, profile: QueryProfile) -> list[Finding]:
        """Run heuristic checks on a query profile execution plan."""
        return run_query_profile_checks(profile, self.thresholds)

    def analyze(
        self,
        model: PBIModel | None = None,
        query_data: QueryHistoryData | None = None,
        query_profile: QueryProfile | None = None,
    ) -> list[Finding]:
        """Run all applicable checks."""
        findings: list[Finding] = []
        if model:
            findings.extend(self.analyze_model(model))
        if query_data:
            findings.extend(self.analyze_queries(query_data))
        if query_profile:
            findings.extend(self.analyze_query_profile(query_profile))
        return findings
