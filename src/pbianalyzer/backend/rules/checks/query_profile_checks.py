"""Heuristic checks for DBSQL Query Profile exports.

Uses the "4 S's" framework from the Databricks performance-tuning methodology:
  Skew — Spill — Shuffle — Small files
Plus additional checks for join strategy, Photon, query anti-patterns, and I/O.
"""

from __future__ import annotations

import re
from collections import Counter

from ...parsers.models import Finding, Impact, QueryProfile, Severity


def run_query_profile_checks(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    """Run all heuristic-based checks against a parsed query profile."""
    findings: list[Finding] = []

    findings.extend(_check_overall_latency(profile, thresholds))
    findings.extend(_check_compilation_time(profile, thresholds))
    findings.extend(_check_spill_to_disk(profile))
    findings.extend(_check_scan_to_output_ratio(profile, thresholds))
    findings.extend(_check_duplicate_table_scans(profile))
    findings.extend(_check_nested_loop_joins(profile))
    findings.extend(_check_cartesian_joins(profile))
    findings.extend(_check_sort_merge_small_table(profile))
    findings.extend(_check_excessive_shuffles(profile, thresholds))
    findings.extend(_check_subqueries(profile))
    findings.extend(_check_high_bytes_low_rows(profile, thresholds))
    findings.extend(_check_scan_hotspot(profile))
    findings.extend(_check_small_files(profile, thresholds))
    findings.extend(_check_photon_utilization(profile))
    findings.extend(_check_select_star(profile))
    findings.extend(_check_order_by_without_limit(profile))
    findings.extend(_check_high_metadata_time(profile, thresholds))
    findings.extend(_check_missing_analyze_table(profile))
    findings.extend(_check_pbi_query_patterns(profile))

    return findings


def _check_overall_latency(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    total_ms = profile.metrics.total_time_ms
    threshold_ms = thresholds.get("qp_slow_query", {}).get("total_time_ms", 10000)
    if total_ms <= threshold_ms:
        return []
    return [Finding(
        rule_id="qp_slow_query",
        category="dbsql_performance",
        name="Slow Query",
        severity=Severity.ERROR if total_ms > threshold_ms * 3 else Severity.WARNING,
        description=(
            f"Total query time is {total_ms / 1000:.1f}s "
            f"(threshold: {threshold_ms / 1000:.0f}s). "
            f"Compilation: {profile.metrics.compilation_time_ms}ms, "
            f"Execution: {profile.metrics.query_execution_time_ms}ms."
        ),
        recommendation=(
            "Investigate the query profile to identify bottlenecks. "
            "Check for data skew, missing statistics (ANALYZE TABLE), "
            "or opportunities to add liquid clustering, materialized views, "
            "or predicate pushdown."
        ),
        impact=Impact.HIGH,
        details={
            "total_time_ms": total_ms,
            "compilation_time_ms": profile.metrics.compilation_time_ms,
            "execution_time_ms": profile.metrics.query_execution_time_ms,
            "photon_time_ms": profile.metrics.photon_time_ms,
        },
    )]


def _check_compilation_time(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    comp_ms = profile.metrics.compilation_time_ms
    threshold_ms = thresholds.get("qp_slow_compilation", {}).get("compilation_time_ms", 3000)
    if comp_ms <= threshold_ms:
        return []

    total = profile.metrics.total_time_ms or 1
    comp_pct = comp_ms / total * 100

    return [Finding(
        rule_id="qp_slow_compilation",
        category="dbsql_performance",
        name="High Compilation Time",
        severity=Severity.WARNING,
        description=(
            f"Query compilation took {comp_ms:,}ms ({comp_pct:.0f}% of total time). "
            f"Complex query plans or large schema metadata can slow compilation."
        ),
        recommendation=(
            "Simplify the query structure — reduce the number of CTEs, subqueries, "
            "and joins. For Power BI, push complex calculations to Databricks SQL "
            "views or materialized views to reduce the DAX-generated SQL complexity."
        ),
        impact=Impact.MEDIUM,
        details={
            "compilation_time_ms": comp_ms,
            "compilation_pct": round(comp_pct, 1),
        },
    )]


def _check_spill_to_disk(profile: QueryProfile) -> list[Finding]:
    spill = profile.metrics.spill_to_disk_bytes
    if spill <= 0:
        return []
    spill_mb = spill / (1024 * 1024)
    return [Finding(
        rule_id="qp_spill_to_disk",
        category="dbsql_performance",
        name="Spill to Disk Detected",
        severity=Severity.ERROR if spill_mb > 1024 else Severity.WARNING,
        description=(
            f"Query spilled {spill_mb:,.0f} MB to disk. This indicates insufficient "
            f"memory for the operation and causes severe I/O overhead."
        ),
        recommendation=(
            "Consider increasing warehouse size, reducing data skew, "
            "or pre-aggregating data in materialized views. "
            "Check for Cartesian products or skewed joins."
        ),
        impact=Impact.HIGH,
        details={"spill_to_disk_mb": round(spill_mb, 1)},
    )]


def _check_scan_to_output_ratio(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    m = profile.metrics
    if m.rows_produced <= 0 or m.read_bytes <= 0:
        return []

    bytes_per_output_row = m.read_bytes / m.rows_produced
    threshold = thresholds.get("qp_scan_output_ratio", {}).get(
        "max_bytes_per_output_row", 10_000_000
    )

    if bytes_per_output_row <= threshold:
        return []

    read_mb = m.read_bytes / (1024 * 1024)
    return [Finding(
        rule_id="qp_scan_output_ratio",
        category="dbsql_performance",
        name="Extreme Scan-to-Output Ratio",
        severity=Severity.ERROR,
        description=(
            f"Read {read_mb:,.0f} MB to produce {m.rows_produced:,} row(s). "
            f"That's {bytes_per_output_row / (1024 * 1024):,.0f} MB read per output row — "
            f"the query is doing far more work than the result requires."
        ),
        recommendation=(
            "Create a materialized view or aggregate table that pre-computes "
            "this result. Add predicate pushdown filters, partition pruning via "
            "liquid clustering, or PK/FK constraints with RELY for join elimination. "
            "In Power BI, consider user-defined aggregations."
        ),
        impact=Impact.HIGH,
        reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/05.%20User-defined%20Aggregations",
        details={
            "read_bytes": m.read_bytes,
            "read_mb": round(read_mb, 1),
            "rows_produced": m.rows_produced,
            "bytes_per_output_row": round(bytes_per_output_row),
        },
    )]


def _check_duplicate_table_scans(profile: QueryProfile) -> list[Finding]:
    table_counts = Counter(profile.tables_scanned)
    duplicates = {tbl: cnt for tbl, cnt in table_counts.items() if cnt > 1}
    if not duplicates:
        return []

    descriptions = [f"{tbl} ({cnt}x)" for tbl, cnt in duplicates.items()]
    return [Finding(
        rule_id="qp_duplicate_table_scans",
        category="dbsql_performance",
        name="Duplicate Table Scans",
        severity=Severity.WARNING,
        description=(
            f"The same table is scanned multiple times in this query: "
            f"{', '.join(descriptions)}. Each scan adds I/O overhead."
        ),
        recommendation=(
            "Restructure the query to scan each table once using CTEs or subqueries, "
            "or create a materialized view that pre-joins the data. "
            "For Power BI, this often indicates the DAX engine is generating "
            "redundant SQL — push calculations into Databricks views."
        ),
        impact=Impact.MEDIUM,
        details={"duplicate_scans": duplicates},
    )]


def _check_nested_loop_joins(profile: QueryProfile) -> list[Finding]:
    if not profile.has_nested_loop_join:
        return []

    nl_joins = [jt for jt in profile.join_types if "Nested Loop" in jt]
    return [Finding(
        rule_id="qp_nested_loop_join",
        category="dbsql_performance",
        name="Nested Loop Join Detected",
        severity=Severity.WARNING,
        description=(
            f"Query uses a Nested Loop Join ({', '.join(nl_joins)}). "
            f"These have O(n*m) complexity and are much slower than hash joins "
            f"for large datasets."
        ),
        recommendation=(
            "Add join predicates that allow hash or sort-merge join strategies. "
            "Ensure join columns have matching types. Add table statistics via "
            "ANALYZE TABLE. If this is a semi-join for an EXISTS clause, consider "
            "rewriting with IN or JOIN."
        ),
        impact=Impact.HIGH,
        details={"nested_loop_joins": nl_joins, "all_joins": profile.join_types},
    )]


def _check_excessive_shuffles(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    threshold = thresholds.get("qp_excessive_shuffles", {}).get("max_shuffles", 6)
    if profile.shuffle_count <= threshold:
        return []

    return [Finding(
        rule_id="qp_excessive_shuffles",
        category="dbsql_performance",
        name="Excessive Shuffle Exchanges",
        severity=Severity.WARNING,
        description=(
            f"Query has {profile.shuffle_count} shuffle exchanges (threshold: {threshold}). "
            f"Excessive shuffling moves data between nodes and adds latency."
        ),
        recommendation=(
            "Reduce shuffles by pre-aggregating data, using broadcast joins "
            "for small tables, or clustering tables on frequently-joined columns. "
            "Consider liquid clustering on join/filter columns."
        ),
        impact=Impact.MEDIUM,
        details={"shuffle_count": profile.shuffle_count},
    )]


def _check_subqueries(profile: QueryProfile) -> list[Finding]:
    if not profile.has_subquery:
        return []
    return [Finding(
        rule_id="qp_subquery_detected",
        category="dbsql_performance",
        name="Subquery in Execution Plan",
        severity=Severity.INFO,
        description=(
            "The execution plan contains a subquery node. Subqueries can be "
            "executed once per outer row in some cases, creating performance issues."
        ),
        recommendation=(
            "Consider rewriting as a JOIN or using a CTE. "
            "If this is Power BI-generated SQL, push the logic into a "
            "Databricks SQL view to give the optimizer more control."
        ),
        impact=Impact.LOW,
        details={},
    )]


def _check_high_bytes_low_rows(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    m = profile.metrics
    if m.rows_read <= 0:
        return []

    bytes_per_row = m.read_bytes / m.rows_read
    threshold = thresholds.get("qp_wide_row_scan", {}).get("max_bytes_per_row", 500_000)

    if bytes_per_row <= threshold:
        return []

    return [Finding(
        rule_id="qp_wide_row_scan",
        category="dbsql_performance",
        name="Wide Row Scan (High Bytes Per Row)",
        severity=Severity.WARNING,
        description=(
            f"Average {bytes_per_row / 1024:,.0f} KB read per row "
            f"({m.read_bytes / (1024 * 1024):,.0f} MB total for {m.rows_read:,} rows). "
            f"This suggests scanning many unused columns."
        ),
        recommendation=(
            "Select only the columns needed. In Power BI, remove unused columns "
            "from the model. In Databricks, use column pruning and consider "
            "liquid clustering on filter columns."
        ),
        impact=Impact.MEDIUM,
        details={
            "bytes_per_row": round(bytes_per_row),
            "read_bytes": m.read_bytes,
            "rows_read": m.rows_read,
        },
    )]


def _check_cartesian_joins(profile: QueryProfile) -> list[Finding]:
    cartesians = [
        jt for jt in profile.join_types
        if "cross" in jt.lower() or "cartesian" in jt.lower()
    ]
    if not cartesians:
        return []
    return [Finding(
        rule_id="qp_cartesian_join",
        category="dbsql_performance",
        name="Cartesian / Cross Join Detected",
        severity=Severity.ERROR,
        description=(
            f"Query contains a Cartesian or Cross join ({', '.join(cartesians)}). "
            f"This produces O(n*m) rows and explodes data volume."
        ),
        recommendation=(
            "Add a proper join condition to convert to an equi-join. "
            "If a cross join is truly needed, consider pre-filtering both sides "
            "to minimize row counts. Check for missing ON clauses."
        ),
        impact=Impact.HIGH,
        details={"cartesian_joins": cartesians},
    )]


def _check_sort_merge_small_table(profile: QueryProfile) -> list[Finding]:
    if profile.sort_merge_join_count == 0:
        return []

    scan_rows = {}
    for n in profile.nodes:
        if not n.hidden and "SCAN" in n.tag.upper():
            tbl = n.metadata.get("SCAN_IDENTIFIER", n.name.replace("Scan ", "").strip())
            scan_rows[tbl] = n.rows

    small_tables = [t for t, r in scan_rows.items() if 0 < r < 10_000]

    desc = (
        f"Query uses {profile.sort_merge_join_count} SortMergeJoin(s), "
        f"which require shuffling both sides. "
    )
    if small_tables:
        desc += (
            f"Tables {', '.join(small_tables)} have fewer than 10k rows and "
            f"could likely use a BroadcastHashJoin instead."
        )
    else:
        desc += (
            "SortMergeJoin is expensive; the optimizer may lack table statistics "
            "to choose BroadcastHashJoin where appropriate."
        )

    return [Finding(
        rule_id="qp_sort_merge_small_table",
        category="dbsql_performance",
        name="SortMergeJoin on Potentially Small Table",
        severity=Severity.WARNING,
        description=desc,
        recommendation=(
            "Run `ANALYZE TABLE <table> COMPUTE STATISTICS FOR ALL COLUMNS` "
            "on all tables in this query so the optimizer can choose broadcast joins. "
            "Alternatively, use a hint: `SELECT /*+ BROADCAST(small_table) */ ...` "
            "or increase `spark.sql.autoBroadcastJoinThreshold` (default 10MB)."
        ),
        impact=Impact.HIGH,
        details={
            "sort_merge_join_count": profile.sort_merge_join_count,
            "small_tables": small_tables,
            "scan_row_counts": scan_rows,
        },
    )]


def _check_scan_hotspot(profile: QueryProfile) -> list[Finding]:
    exec_ms = profile.metrics.query_execution_time_ms
    if exec_ms <= 0:
        return []

    scans = []
    for n in profile.nodes:
        if not n.hidden and "SCAN" in n.tag.upper() and n.duration_ms > 0:
            tbl = n.metadata.get("SCAN_IDENTIFIER", n.name.replace("Scan ", "").strip())
            scans.append((tbl, n.duration_ms, n.rows))

    if not scans:
        return []

    hottest_tbl, hottest_dur, hottest_rows = max(scans, key=lambda x: x[1])
    hottest_pct = hottest_dur / exec_ms * 100

    if hottest_pct < 50:
        return []

    return [Finding(
        rule_id="qp_scan_hotspot",
        category="dbsql_performance",
        name="Scan Hotspot — Single Table Dominates Query Time",
        severity=Severity.WARNING if hottest_pct < 75 else Severity.ERROR,
        description=(
            f"Scanning `{hottest_tbl}` took {hottest_dur:,}ms "
            f"({hottest_pct:.0f}% of execution time, {hottest_rows:,} rows). "
            f"This single scan is the primary bottleneck."
        ),
        recommendation=(
            f"Apply Liquid Clustering on `{hottest_tbl}` using frequently filtered/joined columns: "
            f"`ALTER TABLE {hottest_tbl} CLUSTER BY (col1, col2)` then `OPTIMIZE {hottest_tbl}`. "
            f"Also run `ANALYZE TABLE {hottest_tbl} COMPUTE STATISTICS FOR ALL COLUMNS` "
            f"and ensure filters are pushed down into the scan."
        ),
        impact=Impact.HIGH,
        details={
            "table": hottest_tbl,
            "scan_duration_ms": hottest_dur,
            "scan_pct_of_execution": round(hottest_pct, 1),
            "rows_scanned": hottest_rows,
        },
    )]


def _check_small_files(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    m = profile.metrics
    if m.read_files_count <= 0 or m.read_bytes <= 0:
        return []

    avg_file_bytes = m.read_bytes / m.read_files_count
    avg_file_mb = avg_file_bytes / (1024 * 1024)
    threshold_mb = thresholds.get("qp_small_files", {}).get("min_avg_file_mb", 8)

    if avg_file_mb >= threshold_mb:
        return []

    return [Finding(
        rule_id="qp_small_files",
        category="dbsql_performance",
        name="Small File Problem Detected",
        severity=Severity.WARNING if avg_file_mb >= 1 else Severity.ERROR,
        description=(
            f"Read {m.read_files_count:,} files averaging {avg_file_mb:.1f} MB each "
            f"(ideal: 64–256 MB). Small files add I/O overhead and reduce scan throughput."
        ),
        recommendation=(
            "Run `OPTIMIZE <table>` to compact small files into larger ones. "
            "Enable auto-compaction: `ALTER TABLE <table> SET TBLPROPERTIES "
            "('delta.autoOptimize.autoCompact' = 'true')`. "
            "For UC managed tables, enable Predictive Optimization: "
            "`ALTER CATALOG <catalog> ENABLE PREDICTIVE OPTIMIZATION`."
        ),
        impact=Impact.MEDIUM,
        details={
            "files_read": m.read_files_count,
            "avg_file_mb": round(avg_file_mb, 2),
            "total_read_mb": round(m.read_bytes / (1024 * 1024), 1),
        },
    )]


def _check_photon_utilization(profile: QueryProfile) -> list[Finding]:
    m = profile.metrics
    total_nodes = profile.photon_node_count + profile.non_photon_node_count
    if total_nodes == 0:
        return []

    if profile.photon_node_count == 0 and m.photon_time_ms == 0:
        return [Finding(
            rule_id="qp_no_photon",
            category="dbsql_performance",
            name="Photon Engine Not Used",
            severity=Severity.WARNING,
            description=(
                "No nodes in this query used the Photon engine. "
                "Photon is a vectorized C++ engine that can be 2-8x faster for scans, "
                "joins, and aggregations."
            ),
            recommendation=(
                "Ensure the SQL warehouse has Photon enabled. "
                "If using UDFs (Python/Scala), replace them with built-in SQL functions — "
                "UDFs force fallback to the JVM engine and defeat Photon."
            ),
            impact=Impact.MEDIUM,
            details={"photon_nodes": 0, "total_nodes": total_nodes},
        )]

    if profile.non_photon_node_count > 0 and m.photon_time_ms > 0:
        photon_pct = profile.photon_node_count / total_nodes * 100
        if photon_pct < 60:
            return [Finding(
                rule_id="qp_low_photon",
                category="dbsql_performance",
                name="Low Photon Utilization",
                severity=Severity.INFO,
                description=(
                    f"Only {profile.photon_node_count}/{total_nodes} plan nodes "
                    f"({photon_pct:.0f}%) ran on Photon. Some operations fell back to "
                    f"the JVM engine, which is slower."
                ),
                recommendation=(
                    "Check if the query uses UDFs, complex nested types, or operations "
                    "not yet supported by Photon. Replace UDFs with built-in functions "
                    "where possible."
                ),
                impact=Impact.LOW,
                details={
                    "photon_nodes": profile.photon_node_count,
                    "non_photon_nodes": profile.non_photon_node_count,
                    "photon_pct": round(photon_pct, 1),
                },
            )]

    return []


def _check_select_star(profile: QueryProfile) -> list[Finding]:
    sql = profile.query_text.strip()
    if not sql:
        return []

    normalized = re.sub(r'\s+', ' ', sql.upper())
    select_star = re.findall(r'SELECT\s+\*', normalized)
    if not select_star:
        return []

    select_count = normalized.count("SELECT ")
    star_count = len(select_star)

    if star_count == 0:
        return []

    return [Finding(
        rule_id="qp_select_star",
        category="dbsql_performance",
        name="SELECT * Detected",
        severity=Severity.WARNING if star_count > 1 else Severity.INFO,
        description=(
            f"Query uses SELECT * ({star_count} occurrence{'s' if star_count > 1 else ''} "
            f"across {select_count} SELECT statements). "
            f"This reads all columns, defeating column pruning and increasing I/O."
        ),
        recommendation=(
            "Select only the columns you need. In Power BI, remove unused columns "
            "from the data model. In SQL, list specific columns instead of using *. "
            "Column pruning can dramatically reduce bytes read from Delta tables."
        ),
        impact=Impact.MEDIUM,
        details={"select_star_count": star_count, "select_count": select_count},
    )]


def _check_order_by_without_limit(profile: QueryProfile) -> list[Finding]:
    sql = profile.query_text.strip()
    if not sql:
        return []

    normalized = re.sub(r'\s+', ' ', sql.upper())
    if "ORDER BY" not in normalized:
        return []

    has_limit = "LIMIT " in normalized or "FETCH FIRST" in normalized or "TOP " in normalized
    if has_limit:
        return []

    if profile.metrics.rows_produced > 1000:
        return [Finding(
            rule_id="qp_order_by_no_limit",
            category="dbsql_performance",
            name="ORDER BY Without LIMIT",
            severity=Severity.WARNING,
            description=(
                f"Query sorts the entire result set ({profile.metrics.rows_produced:,} rows) "
                f"without a LIMIT clause. Full sorts are expensive and require shuffling "
                f"all data to a single partition."
            ),
            recommendation=(
                "Add a LIMIT clause if only the top/bottom N rows are needed. "
                "If the full sorted result is required, consider pre-sorting via "
                "a materialized view or Liquid Clustering."
            ),
            impact=Impact.MEDIUM,
            details={"rows_produced": profile.metrics.rows_produced},
        )]

    return []


def _check_high_metadata_time(profile: QueryProfile, thresholds: dict) -> list[Finding]:
    m = profile.metrics
    threshold_ms = thresholds.get("qp_high_metadata_time", {}).get("metadata_time_ms", 2000)

    if m.metadata_time_ms <= threshold_ms:
        return []

    total = m.total_time_ms or 1
    meta_pct = m.metadata_time_ms / total * 100

    return [Finding(
        rule_id="qp_high_metadata_time",
        category="dbsql_performance",
        name="High Metadata Resolution Time",
        severity=Severity.WARNING if meta_pct < 30 else Severity.ERROR,
        description=(
            f"Metadata resolution took {m.metadata_time_ms:,}ms ({meta_pct:.0f}% of total). "
            f"This is the time spent resolving table/column metadata from Unity Catalog."
        ),
        recommendation=(
            "This can indicate a large number of tables or complex views being resolved. "
            "Simplify the query structure, reduce the number of referenced tables, "
            "or flatten deeply nested view chains."
        ),
        impact=Impact.MEDIUM,
        details={
            "metadata_time_ms": m.metadata_time_ms,
            "metadata_pct": round(meta_pct, 1),
        },
    )]


def _check_missing_analyze_table(profile: QueryProfile) -> list[Finding]:
    if not profile.tables_scanned:
        return []

    signals = 0
    reasons = []

    if profile.sort_merge_join_count > 0:
        signals += 2
        reasons.append(f"{profile.sort_merge_join_count} SortMergeJoin(s) (often caused by missing stats)")

    m = profile.metrics
    if m.compilation_time_ms > 0 and m.total_time_ms > 0:
        comp_pct = m.compilation_time_ms / m.total_time_ms * 100
        if comp_pct > 25:
            signals += 1
            reasons.append(f"high compilation time ({comp_pct:.0f}% of total)")

    if profile.has_nested_loop_join:
        signals += 1
        reasons.append("nested loop join present")

    if signals < 2:
        return []

    tables_str = ", ".join(f"`{t}`" for t in set(profile.tables_scanned))
    analyze_cmds = "\n".join(
        f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS;"
        for t in sorted(set(profile.tables_scanned))
    )

    return [Finding(
        rule_id="qp_missing_analyze_table",
        category="dbsql_performance",
        name="Table Statistics Likely Missing or Stale",
        severity=Severity.WARNING,
        description=(
            f"Multiple signals suggest the optimizer lacks accurate table statistics "
            f"for {tables_str}: {'; '.join(reasons)}. "
            f"Without statistics, the optimizer cannot choose optimal join strategies "
            f"or accurate cardinality estimates."
        ),
        recommendation=(
            f"Run ANALYZE TABLE on all tables in this query:\n```sql\n{analyze_cmds}\n```\n"
            f"Re-run ANALYZE after significant data changes (loads, merges, deletes). "
            f"For UC managed tables, enable Predictive Optimization to keep stats fresh automatically."
        ),
        impact=Impact.HIGH,
        reference_url="https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html",
        details={"signals": signals, "reasons": reasons, "tables": list(set(profile.tables_scanned))},
    )]


def _check_pbi_query_patterns(profile: QueryProfile) -> list[Finding]:
    if not profile.is_pbi_generated:
        return []

    findings: list[Finding] = []

    findings.append(Finding(
        rule_id="qp_pbi_generated",
        category="dbsql_performance",
        name="Power BI-Generated Query Detected",
        severity=Severity.INFO,
        description=(
            "This query was generated by Power BI (detected from correlation IDs). "
            "Power BI auto-generated SQL may not be optimal for the Databricks "
            "query engine."
        ),
        recommendation=(
            "Review the DAX measures driving this query. Push complex calculations "
            "to Databricks SQL views or materialized views. Set dimension tables "
            "to Dual storage mode to reduce unnecessary SQL queries."
        ),
        impact=Impact.MEDIUM,
        reference_url="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts/tree/main/10.%20Pushdown%20Calculations",
        details={
            "query_length": len(profile.query_text),
            "tables_scanned": profile.tables_scanned,
            "join_count": len(profile.join_types),
        },
    ))

    return findings
