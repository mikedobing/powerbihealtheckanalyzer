"""Databricks SQL analyzer — works with live SDK or uploaded query JSON."""

from __future__ import annotations

from ..parsers.models import QueryHistoryData, QueryRecord


QUERY_HISTORY_SQL = """\
SELECT
  query_id, query_text, status,
  duration / 1000 as duration_seconds,
  rows_produced, rows_read, bytes_read,
  warehouse_id, statement_type,
  start_time, end_time,
  error_message
FROM system.query.history
WHERE start_time > dateadd(DAY, -{days}, now())
  AND warehouse_id = '{warehouse_id}'
ORDER BY start_time DESC
LIMIT {limit}
"""


def build_export_query(warehouse_id: str, days: int = 7, limit: int = 1000) -> str:
    """Build the SQL query customers can run to export their query history."""
    return QUERY_HISTORY_SQL.format(
        warehouse_id=warehouse_id,
        days=days,
        limit=limit,
    )


async def fetch_query_history_live(
    ws_client: object,
    warehouse_id: str,
    days: int = 7,
    limit: int = 500,
) -> QueryHistoryData:
    """Fetch query history from a live Databricks workspace.

    Uses the Statement Execution API to query system.query.history.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    assert isinstance(ws_client, WorkspaceClient)

    sql = QUERY_HISTORY_SQL.format(
        warehouse_id=warehouse_id, days=days, limit=limit
    )

    response = ws_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="120s",
    )

    queries: list[QueryRecord] = []

    if response.status and response.status.state == StatementState.SUCCEEDED:
        if response.result and response.result.data_array:
            schema_cols = (response.manifest.schema.columns or []) if response.manifest and response.manifest.schema else []
            columns = [c.name for c in schema_cols]
            for row in response.result.data_array:
                row_dict = dict(zip(columns, row))
                queries.append(QueryRecord(
                    query_id=str(row_dict.get("query_id", "")),
                    query_text=str(row_dict.get("query_text", "")),
                    status=str(row_dict.get("status", "")),
                    duration_seconds=float(row_dict.get("duration_seconds", 0) or 0),
                    rows_produced=int(row_dict.get("rows_produced", 0) or 0),
                    rows_read=int(row_dict.get("rows_read", 0) or 0),
                    bytes_read=int(row_dict.get("bytes_read", 0) or 0),
                    warehouse_id=str(row_dict.get("warehouse_id", "")),
                    statement_type=str(row_dict.get("statement_type", "")),
                    start_time=str(row_dict.get("start_time", "")),
                    end_time=str(row_dict.get("end_time", "")),
                    error_message=str(row_dict.get("error_message", "")),
                ))

    return QueryHistoryData(queries=queries)
