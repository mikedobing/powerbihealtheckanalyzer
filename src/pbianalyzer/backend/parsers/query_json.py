"""Parse exported Databricks SQL query history JSON."""

from __future__ import annotations

import json
from .models import QueryHistoryData, QueryRecord


def parse_query_json(content: bytes | str) -> QueryHistoryData:
    """Parse an exported JSON array of DBSQL query history records."""
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig")

    data = json.loads(content)

    if isinstance(data, dict):
        data = data.get("queries", data.get("data", data.get("results", [data])))

    if not isinstance(data, list):
        data = [data]

    queries = []
    for row in data:
        queries.append(QueryRecord(
            query_id=str(row.get("query_id", row.get("queryId", ""))),
            query_text=row.get("query_text", row.get("queryText", row.get("sql", ""))),
            status=row.get("status", ""),
            duration_seconds=float(row.get("duration_seconds", row.get("duration", 0)) or 0),
            rows_produced=int(row.get("rows_produced", row.get("rowsProduced", 0)) or 0),
            rows_read=int(row.get("rows_read", row.get("rowsRead", 0)) or 0),
            bytes_read=int(row.get("bytes_read", row.get("bytesRead", 0)) or 0),
            warehouse_id=str(row.get("warehouse_id", row.get("warehouseId", ""))),
            statement_type=row.get("statement_type", row.get("statementType", "")),
            start_time=str(row.get("start_time", row.get("startTime", ""))),
            end_time=str(row.get("end_time", row.get("endTime", ""))),
            error_message=row.get("error_message", row.get("errorMessage", "")),
        ))

    return QueryHistoryData(queries=queries)
