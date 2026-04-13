"""Query parallelization and concurrency checks."""

from __future__ import annotations

from ...parsers.models import Finding, Impact, PBIModel, Severity


def run_parallelization_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    # Check model annotations for parallelism settings
    annotation_map = {a.name: a.value for a in model.annotations}

    max_parallelism = annotation_map.get("MaxParallelismPerQuery")
    max_connections = annotation_map.get("DataSourceDefaultMaxConnections")

    total_measures = sum(len(t.measures) for t in model.tables)

    if not max_parallelism and total_measures > 5:
        findings.append(Finding(
            rule_id="parallel_max_parallelism_default",
            category="parallelization",
            name="MaxParallelismPerQuery Not Tuned",
            severity=Severity.INFO,
            description=(
                f"Model has {total_measures} measures but MaxParallelismPerQuery "
                f"is not explicitly set — default parallelism may throttle performance"
            ),
            recommendation=(
                "Set MaxParallelismPerQuery via Tabular Editor to match the number "
                "of concurrent SQL queries your reports generate."
            ),
            impact=Impact.MEDIUM,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/04.%20Query%20Parallelization",
            details={"total_measures": total_measures},
        ))

    if not max_connections and total_measures > 10:
        findings.append(Finding(
            rule_id="parallel_max_connections_default",
            category="parallelization",
            name="DataSourceDefaultMaxConnections at Default",
            severity=Severity.INFO,
            description=(
                f"Model has {total_measures} measures but DataSourceDefaultMaxConnections "
                f"is at the default limit of 10"
            ),
            recommendation=(
                "Increase DataSourceDefaultMaxConnections using Tabular Editor "
                "(requires CompatibilityLevel 1569+). Also ensure your Databricks "
                "SQL Warehouse has min 2 clusters."
            ),
            impact=Impact.MEDIUM,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/14.%20Data%20Source%20Default%20Max%20Connections",
            details={"total_measures": total_measures},
        ))

    return findings
