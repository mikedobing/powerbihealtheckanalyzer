"""Connectivity and integration checks."""

from __future__ import annotations

import re
from ...parsers.models import Finding, Impact, PBIModel, Severity


def _check_partitions_for_connections(model: PBIModel) -> dict:
    """Extract connection info from partition M expressions."""
    info = {
        "has_databricks_connector": False,
        "has_odbc_jdbc": False,
        "has_hardcoded_connection": False,
        "has_parameterized_connection": False,
        "has_pat_auth": False,
    }

    for table in model.tables:
        for partition in table.partitions:
            query = partition.query.lower()
            if not query:
                continue

            if "databricks.catalogs" in query or "databricks.query" in query:
                info["has_databricks_connector"] = True

            if "odbc" in query or "jdbc" in query:
                info["has_odbc_jdbc"] = True

            # Hardcoded vs parameterized connection strings
            hostname_pattern = re.compile(
                r'["\'][\w.-]+\.(azuredatabricks\.net|cloud\.databricks\.com|databricks\.com)',
                re.IGNORECASE,
            )
            if hostname_pattern.search(partition.query):
                info["has_hardcoded_connection"] = True

            # Parameters referenced in M expressions
            if re.search(r'\b(Hostname|HttpPath|ServerHostname)\b', partition.query):
                info["has_parameterized_connection"] = True

    # Check data sources for auth patterns
    for ds in model.data_sources:
        conn = ds.connection_string.lower()
        if "personal access token" in conn or "pat" in ds.auth_kind.lower():
            info["has_pat_auth"] = True
        if "odbc" in conn or "jdbc" in conn:
            info["has_odbc_jdbc"] = True

    return info


def run_connectivity_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    conn_info = _check_partitions_for_connections(model)

    if conn_info["has_hardcoded_connection"] and not conn_info["has_parameterized_connection"]:
        findings.append(Finding(
            rule_id="conn_hardcoded_connection",
            category="connectivity",
            name="Hardcoded Connection String",
            severity=Severity.WARNING,
            description="Connection strings contain hardcoded server/path values instead of Power BI parameters",
            recommendation=(
                "Parameterize Hostname and HttpPath as Power BI parameters "
                "to simplify environment switching."
            ),
            impact=Impact.MEDIUM,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/01.%20Connection%20Parameters",
        ))

    if conn_info["has_odbc_jdbc"] and not conn_info["has_databricks_connector"]:
        findings.append(Finding(
            rule_id="conn_non_native_connector",
            category="connectivity",
            name="Non-Native Databricks Connector",
            severity=Severity.WARNING,
            description="Connection uses generic ODBC/JDBC instead of the native Databricks connector",
            recommendation="Switch to the native Databricks connector for better performance and CloudFetch support.",
            impact=Impact.MEDIUM,
        ))

    if conn_info["has_pat_auth"]:
        findings.append(Finding(
            rule_id="conn_pat_auth",
            category="connectivity",
            name="PAT-Based Authentication",
            severity=Severity.INFO,
            description="Personal Access Token authentication detected",
            recommendation="Migrate to M2M OAuth or Entra ID SSO for better security and auditability.",
            impact=Impact.MEDIUM,
            reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/13.%20M2M%20OAuth%20Credentials%20Management",
        ))

    return findings
