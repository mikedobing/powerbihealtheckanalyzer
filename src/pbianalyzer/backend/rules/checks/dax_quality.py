"""DAX quality and pushdown opportunity checks."""

from __future__ import annotations

import re
from ...parsers.models import Finding, Impact, PBIModel, Severity, StorageMode


def _count_calculate_depth(expression: str) -> int:
    """Estimate nesting depth of CALCULATE calls."""
    depth = 0
    max_depth = 0
    for token in re.finditer(r"\b(CALCULATE|FILTER)\s*\(", expression, re.IGNORECASE):
        depth += 1
        max_depth = max(max_depth, depth)
    return max_depth


def _has_related_table_filter(expression: str) -> bool:
    """Detect CALCULATE with filters referencing related tables."""
    pattern = re.compile(
        r"\bCALCULATE\s*\([^)]*['\"]?\w+['\"]?\s*\[\w+\]\s*[=<>!]",
        re.IGNORECASE,
    )
    return bool(pattern.search(expression))


def _has_row_by_row_pattern(expression: str) -> bool:
    """Detect SUMX/ADDCOLUMNS with complex inner expressions."""
    pattern = re.compile(
        r"\b(SUMX|AVERAGEX|MINX|MAXX|COUNTX|ADDCOLUMNS|GENERATE)\s*\(",
        re.IGNORECASE,
    )
    if not pattern.search(expression):
        return False
    return len(expression) > 200


def _has_time_intelligence_pattern(expression: str) -> bool:
    """Detect time intelligence functions that may generate suboptimal SQL."""
    pattern = re.compile(
        r"\b(DATEADD|DATESYTD|DATESQTD|DATESMTD|SAMEPERIODLASTYEAR|"
        r"PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR|TOTALYTD|TOTALQTD|TOTALMTD)\s*\(",
        re.IGNORECASE,
    )
    return bool(pattern.search(expression))


def run_dax_checks(model: PBIModel, thresholds: dict) -> list[Finding]:
    findings: list[Finding] = []

    max_length = thresholds.get("dax_complex_measure", {}).get("max_length", 500)
    max_calc_depth = thresholds.get("dax_complex_measure", {}).get("max_calculate_depth", 3)

    dq_tables = {
        t.name for t in model.tables
        if t.storage_mode in (StorageMode.DIRECT_QUERY, StorageMode.DEFAULT)
    }

    for table in model.tables:
        for measure in table.measures:
            expr = measure.expression.strip()
            if not expr:
                continue

            # Complex measure detection
            calc_depth = _count_calculate_depth(expr)
            if len(expr) > max_length or calc_depth > max_calc_depth:
                findings.append(Finding(
                    rule_id="dax_complex_measure",
                    category="dax_quality",
                    name="Complex DAX Measure",
                    severity=Severity.WARNING,
                    description=(
                        f"Measure '{measure.name}' in '{table.name}': "
                        f"{len(expr)} chars, CALCULATE depth {calc_depth}"
                    ),
                    recommendation="Simplify or push computation into a Databricks SQL view.",
                    impact=Impact.MEDIUM,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/10.%20Pushdown%20Calculations",
                    details={
                        "table": table.name,
                        "measure": measure.name,
                        "length": len(expr),
                        "calculate_depth": calc_depth,
                    },
                ))

            # CALCULATE with related table filter (pushdown opportunity)
            if _has_related_table_filter(expr) and table.name in dq_tables:
                findings.append(Finding(
                    rule_id="dax_calculate_related_filter",
                    category="dax_quality",
                    name="CALCULATE with Related Table Filter",
                    severity=Severity.WARNING,
                    description=(
                        f"Measure '{measure.name}' in '{table.name}' uses CALCULATE "
                        f"with a related table filter — generates separate SQL queries per measure"
                    ),
                    recommendation="Push the calculation into a Databricks SQL view using GROUP BY.",
                    impact=Impact.HIGH,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/10.%20Pushdown%20Calculations",
                    details={"table": table.name, "measure": measure.name},
                ))

            # Row-by-row patterns
            if _has_row_by_row_pattern(expr):
                findings.append(Finding(
                    rule_id="dax_row_by_row",
                    category="dax_quality",
                    name="Row-by-Row DAX Pattern",
                    severity=Severity.WARNING,
                    description=(
                        f"Measure '{measure.name}' in '{table.name}' uses "
                        f"iterator functions (SUMX/ADDCOLUMNS) with complex expressions"
                    ),
                    recommendation="Replace with set-based patterns or push to a SQL view.",
                    impact=Impact.MEDIUM,
                    details={"table": table.name, "measure": measure.name},
                ))

            # Time intelligence on DQ
            if _has_time_intelligence_pattern(expr) and table.name in dq_tables:
                findings.append(Finding(
                    rule_id="dax_time_intelligence_suboptimal",
                    category="dax_quality",
                    name="Suboptimal Time Intelligence",
                    severity=Severity.INFO,
                    description=(
                        f"Measure '{measure.name}' uses time intelligence functions "
                        f"on a DirectQuery table"
                    ),
                    recommendation="Use a persisted calendar table in Dual mode for efficient SQL.",
                    impact=Impact.MEDIUM,
                    reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/15.%20Calendar-based%20Time%20Intelligence",
                    details={"table": table.name, "measure": measure.name},
                ))

    return findings
