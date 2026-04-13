"""Parse DBSQL Query Profile JSON exports (from the Query Profile UI)."""

from __future__ import annotations

import json
from collections import Counter

from .models import (
    QueryProfile,
    QueryProfileEdge,
    QueryProfileMetrics,
    QueryProfileNode,
    QueryProfileNodeMetric,
)


def is_query_profile(data: dict | list) -> bool:
    """Detect whether a parsed JSON object is a Query Profile export."""
    if not isinstance(data, dict):
        return False
    return (
        "graphs" in data
        and "query" in data
        and isinstance(data.get("graphs"), list)
    )


def parse_query_profile(content: bytes | str) -> QueryProfile:
    """Parse a DBSQL Query Profile JSON export into structured data."""
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig")

    data = json.loads(content)
    return parse_query_profile_dict(data)


def parse_query_profile_dict(data: dict) -> QueryProfile:
    """Parse an already-loaded dict into a QueryProfile."""
    query_info = data.get("query", {})
    graphs = data.get("graphs", [])
    graph = graphs[0] if graphs else {}

    metrics = _parse_metrics(query_info.get("metrics", {}))

    raw_nodes = graph.get("nodes", [])
    raw_edges = graph.get("edges", [])

    nodes = [_parse_node(n) for n in raw_nodes]
    edges = [_parse_edge(e) for e in raw_edges]

    visible_nodes = [n for n in nodes if not n.hidden]

    tables_scanned: list[str] = []
    join_types: list[str] = []
    has_nested_loop = False
    has_subquery = False
    shuffle_count = 0
    aggregate_count = 0

    sort_merge_join_count = 0
    photon_node_count = 0
    non_photon_node_count = 0
    filter_nodes_above_scan = 0

    node_by_id: dict[str, QueryProfileNode] = {n.id: n for n in nodes}
    child_to_parent: dict[str, str] = {}
    for e in edges:
        child_to_parent[e.source] = e.target

    for n in visible_nodes:
        tag = n.tag.upper()
        if "SCAN" in tag:
            table_name = n.name.replace("Scan ", "").strip()
            if table_name:
                tables_scanned.append(table_name)
        if "JOIN" in tag:
            join_algo = n.metadata.get("JOIN_ALGORITHM", "")
            join_type = n.metadata.get("JOIN_TYPE", "")
            label = f"{join_type} ({join_algo})" if join_algo else join_type or n.name
            join_types.append(label)
            if "NESTED_LOOP" in tag:
                has_nested_loop = True
            if "SORT_MERGE" in tag or "sort merge" in join_algo.lower():
                sort_merge_join_count += 1
        if "SUBQUERY" in tag:
            has_subquery = True
        if "SHUFFLE" in tag and "SINK" in tag:
            shuffle_count += 1
        if "AGG" in tag:
            aggregate_count += 1
        if n.is_photon:
            photon_node_count += 1
        elif tag not in ("", "UNKNOWN_SPARK_PLAN.RESULTQUERYSTAGE"):
            non_photon_node_count += 1
        if "FILTER" in tag and n.rows == 0:
            parent_id = child_to_parent.get(n.id)
            if parent_id:
                parent = node_by_id.get(parent_id)
                if parent and "SCAN" in parent.tag.upper() and parent.rows > 100:
                    filter_nodes_above_scan += 1

    query_text = query_info.get("queryText", "")
    is_pbi = _detect_pbi_query(query_text)

    channel = query_info.get("channelUsed", {})
    dbsql_version = channel.get("dbsqlVersion", "") if isinstance(channel, dict) else ""

    return QueryProfile(
        query_id=query_info.get("id", ""),
        query_text=query_text,
        status=query_info.get("status", ""),
        endpoint_id=query_info.get("endpointId", ""),
        statement_type=query_info.get("statementType", ""),
        dbsql_version=dbsql_version,
        metrics=metrics,
        nodes=nodes,
        edges=edges,
        tables_scanned=tables_scanned,
        join_types=join_types,
        has_nested_loop_join=has_nested_loop,
        has_subquery=has_subquery,
        shuffle_count=shuffle_count,
        aggregate_count=aggregate_count,
        sort_merge_join_count=sort_merge_join_count,
        photon_node_count=photon_node_count,
        non_photon_node_count=non_photon_node_count,
        filter_nodes_above_scan=filter_nodes_above_scan,
        is_pbi_generated=is_pbi,
    )


def _parse_metrics(raw: dict) -> QueryProfileMetrics:
    cache_pct = raw.get("bytesReadFromCachePercentage")
    if cache_pct is None:
        read_bytes = raw.get("readBytes", 0) or 0
        cache_bytes = raw.get("readCacheBytes", 0) or 0
        cache_pct = (cache_bytes / read_bytes * 100) if read_bytes > 0 else 0

    return QueryProfileMetrics(
        total_time_ms=int(raw.get("totalTimeMs", 0) or 0),
        compilation_time_ms=int(raw.get("compilationTimeMs", 0) or 0),
        execution_time_ms=int(raw.get("executionTimeMs", 0) or 0),
        query_execution_time_ms=int(raw.get("queryExecutionTimeMs", 0) or 0),
        read_bytes=int(raw.get("readBytes", 0) or 0),
        rows_read=int(raw.get("rowsReadCount", 0) or 0),
        rows_produced=int(raw.get("rowsProducedCount", 0) or 0),
        spill_to_disk_bytes=int(raw.get("spillToDiskBytes", 0) or 0),
        write_remote_bytes=int(raw.get("writeRemoteBytes", 0) or 0),
        read_cache_bytes=int(raw.get("readCacheBytes", 0) or 0),
        cache_hit_pct=float(cache_pct),
        result_from_cache=bool(raw.get("resultFromCache", False)),
        photon_time_ms=int(raw.get("photonTotalTimeMs", 0) or 0),
        task_total_time_ms=int(raw.get("taskTotalTimeMs", 0) or 0),
        read_files_count=int(raw.get("readFilesCount", 0) or 0),
        read_partitions_count=int(raw.get("readPartitionsCount", 0) or 0),
        metadata_time_ms=int(raw.get("metadataTimeMs", 0) or 0),
        network_sent_bytes=int(raw.get("networkSentBytes", 0) or 0),
    )


def _parse_node(raw: dict) -> QueryProfileNode:
    meta_list = raw.get("metadata", [])
    metadata: dict[str, str] = {}
    if isinstance(meta_list, list):
        for entry in meta_list:
            if isinstance(entry, dict) and "key" in entry:
                metadata[entry["key"]] = str(entry.get("value", entry.get("string_value", "")))

    key_metrics: list[QueryProfileNodeMetric] = []
    km_raw = raw.get("keyMetrics", {})
    rows = 0
    duration_ms = 0
    peak_memory_bytes = 0

    if isinstance(km_raw, dict):
        for k, v in km_raw.items():
            key_metrics.append(QueryProfileNodeMetric(name=k, value=str(v)))
        rows = int(km_raw.get("rowsNum", 0) or 0)
        duration_ms = int(km_raw.get("durationMs", 0) or 0)
        peak_memory_bytes = int(km_raw.get("peakMemoryBytes", 0) or 0)
    elif isinstance(km_raw, list):
        for km in km_raw:
            if isinstance(km, dict):
                key_metrics.append(QueryProfileNodeMetric(
                    name=km.get("name", ""),
                    value=str(km.get("value", "")),
                ))

    is_photon = metadata.get("IS_PHOTON", "").lower() == "true"

    return QueryProfileNode(
        id=str(raw.get("id", "")),
        name=raw.get("name", ""),
        tag=raw.get("tag", ""),
        hidden=raw.get("hidden", False),
        metadata=metadata,
        key_metrics=key_metrics,
        rows=rows,
        duration_ms=duration_ms,
        peak_memory_bytes=peak_memory_bytes,
        is_photon=is_photon,
    )


def _parse_edge(raw: dict) -> QueryProfileEdge:
    return QueryProfileEdge(
        source=str(raw.get("from", raw.get("source", ""))),
        target=str(raw.get("to", raw.get("target", ""))),
    )


def _detect_pbi_query(sql: str) -> bool:
    indicators = [
        "correlationid",
        "activityid",
        "`OTBL`",
        "`ITBL`",
        "as `OTBL`",
        "as `ITBL`",
        "OUTER APPLY",
    ]
    lower = sql.lower()
    return any(ind.lower() in lower for ind in indicators)


def summarize_profile_for_llm(profile: QueryProfile) -> str:
    """Build a concise text summary of a query profile for LLM consumption."""
    m = profile.metrics
    lines = [
        "=== DBSQL Query Profile Summary ===",
        f"Query ID: {profile.query_id}",
        f"Status: {profile.status}",
        f"DBSQL Version: {profile.dbsql_version}",
        f"Power BI Generated: {'Yes' if profile.is_pbi_generated else 'No'}",
        "",
        "--- Timing ---",
        f"Total time: {m.total_time_ms:,}ms",
        f"Compilation time: {m.compilation_time_ms:,}ms",
        f"Query execution time: {m.query_execution_time_ms:,}ms",
        f"Photon time: {m.photon_time_ms:,}ms",
        f"Task total time: {m.task_total_time_ms:,}ms",
        f"Metadata resolution time: {m.metadata_time_ms:,}ms",
        "",
        "--- I/O ---",
        f"Bytes read: {m.read_bytes:,}",
        f"Rows read: {m.rows_read:,}",
        f"Rows produced: {m.rows_produced:,}",
        f"Files read: {m.read_files_count}",
        f"Partitions read: {m.read_partitions_count}",
        f"Spill to disk: {m.spill_to_disk_bytes:,} bytes",
        f"Cache hit: {m.cache_hit_pct:.0f}%",
        f"Result from cache: {m.result_from_cache}",
        f"Network sent: {m.network_sent_bytes:,} bytes",
        "",
        "--- Plan Structure ---",
        f"Total visible nodes: {len([n for n in profile.nodes if not n.hidden])}",
        f"Shuffle exchanges: {profile.shuffle_count}",
        f"Aggregates: {profile.aggregate_count}",
        f"Sort-merge joins: {profile.sort_merge_join_count}",
        f"Has nested loop join: {profile.has_nested_loop_join}",
        f"Has subquery: {profile.has_subquery}",
        f"Photon nodes: {profile.photon_node_count}/{profile.photon_node_count + profile.non_photon_node_count}",
    ]

    visible_scans = [
        n for n in profile.nodes if not n.hidden and "SCAN" in n.tag.upper()
    ]
    if visible_scans:
        lines.append("")
        lines.append("--- Table Scans (with per-node metrics) ---")
        for n in visible_scans:
            tbl = n.metadata.get("SCAN_IDENTIFIER", n.name.replace("Scan ", "").strip())
            peak_mb = n.peak_memory_bytes / (1024 * 1024)
            lines.append(
                f"  {tbl}: {n.rows:,} rows, {n.duration_ms:,}ms, "
                f"peak mem {peak_mb:.0f}MB"
            )

    visible_joins = [
        n for n in profile.nodes if not n.hidden and "JOIN" in n.tag.upper()
    ]
    if visible_joins:
        lines.append("")
        lines.append("--- Joins (with per-node metrics) ---")
        for n in visible_joins:
            algo = n.metadata.get("JOIN_ALGORITHM", "?")
            jtype = n.metadata.get("JOIN_TYPE", "?")
            build = n.metadata.get("JOIN_BUILD_SIDE", "?")
            lines.append(
                f"  {jtype} ({algo}), build={build}, "
                f"{n.rows:,} rows out, {n.duration_ms:,}ms"
            )

    if profile.tables_scanned:
        table_counts = Counter(profile.tables_scanned)
        dupes = {t: c for t, c in table_counts.items() if c > 1}
        if dupes:
            lines.append("")
            lines.append("--- Duplicate Scans ---")
            for tbl, count in dupes.items():
                lines.append(f"  {tbl}: scanned {count}x")

    lines.append("")
    lines.append("--- SQL Query ---")
    lines.append(profile.query_text[:4000])

    return "\n".join(lines)
