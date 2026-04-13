"""LLM-powered query profile analysis via Databricks Foundation Model API."""

from __future__ import annotations

import asyncio
import json
import logging
import os

from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from ..parsers.models import Finding, Impact, QueryProfile, Severity
from ..parsers.query_profile import summarize_profile_for_llm

logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "databricks-claude-opus-4-6"
ENV_ENDPOINT_KEY = "PBIANALYZER_LLM_ENDPOINT"

SYSTEM_PROMPT = """\
You are a Databricks SQL performance expert specializing in Power BI on Databricks optimization. \
You use the systematic "4 S's" framework for bottleneck identification: \
**Skew** (uneven data distribution), **Spill** (memory to disk eviction), \
**Shuffle** (excessive data movement), and **Small Files** (too many tiny files degrading I/O).

You will receive a DBSQL Query Profile summary and the SQL query text. Analyze it and identify \
performance issues, anti-patterns, and optimization opportunities.

## Analysis Focus Areas

1. **The 4 S's Framework**:
   - **Skew**: Look for joins where one side processes far more rows than the other, \
     SortMergeJoins that should be broadcasts, or aggregations with uneven group sizes
   - **Spill**: Any spill to disk indicates memory pressure — recommend warehouse sizing \
     or query restructuring
   - **Shuffle**: Count shuffle exchanges; each one moves data between nodes. Recommend \
     broadcast joins, pre-aggregation, or liquid clustering to reduce shuffles
   - **Small Files**: High file count with low average size degrades scan performance. \
     Recommend OPTIMIZE, auto-compaction, or Predictive Optimization

2. **Join Strategy Optimization**:
   - SortMergeJoin on small tables → should be BroadcastHashJoin (missing stats?)
   - Nested Loop joins → add equi-join predicates
   - Join order: filter large tables before joining, join smallest first
   - PK/FK constraints with RELY for join elimination

3. **Databricks-Specific Optimizations**:
   - `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` for all scanned tables
   - Liquid Clustering: `ALTER TABLE ... CLUSTER BY (filter_col, join_col)`
   - Materialized views for repeated aggregation patterns
   - Predicate pushdown: are filters applied at scan level or post-scan?
   - AQE (Adaptive Query Execution): is it likely helping or being defeated?

4. **Power BI-Specific** (if PBI-generated):
   - DAX patterns causing inefficient SQL (CALCULATE with related filters, row-by-row)
   - Missing Dual mode on dimension tables
   - Opportunities to push calculations into Databricks SQL views

5. **Anti-Patterns Checklist**:
   - SELECT * defeating column pruning
   - ORDER BY without LIMIT (full sort)
   - Repeated subqueries (same data scanned multiple times)
   - UDFs defeating Photon engine
   - Cartesian/cross joins

6. **I/O & Storage**:
   - Scan-to-output ratio: massive reads for tiny results
   - Column pruning opportunities
   - Cache utilization (low cache hit %)
   - File and partition read counts

## Output Format

Return your analysis as a JSON array of findings. Each finding must have:
- "name": short title (string)
- "severity": "error" | "warning" | "info"
- "description": 1-2 sentence description of what you found (string)
- "recommendation": specific actionable fix with Databricks SQL commands in code blocks \
  where applicable (string). Include actual table names from the profile.
- "impact": "high" | "medium" | "low"

Return ONLY the JSON array, no other text. Example:
[
  {
    "name": "Missing Table Statistics",
    "severity": "warning",
    "description": "Table commercial_prod.master.gold_sfdc_opportunity is scanned with 2144 rows \
taking 2334ms, suggesting the optimizer lacks statistics for efficient planning.",
    "recommendation": "Run:\\n```sql\\nANALYZE TABLE commercial_prod.master.gold_sfdc_opportunity \
COMPUTE STATISTICS FOR ALL COLUMNS;\\n```\\nThis helps the optimizer choose broadcast joins and \
accurate cardinality estimates.",
    "impact": "high"
  }
]

Be specific — reference actual table names, row counts, timings, and metrics from the profile. \
Do not repeat findings that are obvious from basic metrics alone (like slow query time or spill \
amount — the heuristic engine already catches those). Focus on deeper insights that require \
understanding the SQL structure, join strategy, and execution plan together."""


async def analyze_with_llm(
    profile: QueryProfile,
    ws_client: object | None = None,
    endpoint_name: str | None = None,
) -> list[Finding]:
    """Call a Databricks serving endpoint to get LLM-powered query analysis.

    Args:
        profile: Parsed query profile
        ws_client: Optional WorkspaceClient (used in Databricks App context)
        endpoint_name: Serving endpoint name (defaults to FMAPI)

    Returns:
        List of Finding objects from LLM analysis
    """
    endpoint = endpoint_name or os.environ.get(ENV_ENDPOINT_KEY, DEFAULT_ENDPOINT)
    summary = summarize_profile_for_llm(profile)

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
        ChatMessage(role=ChatMessageRole.USER, content=summary),
    ]

    try:
        response_text = await _call_endpoint(ws_client, endpoint, messages)
        findings = _parse_llm_response(response_text)
        return findings
    except Exception as exc:
        logger.warning("LLM analysis failed: %s", exc)
        return [Finding(
            rule_id="llm_analysis_error",
            category="dbsql_performance",
            name="AI Analysis Unavailable",
            severity=Severity.INFO,
            description=f"LLM-powered analysis could not complete: {exc}",
            recommendation=(
                f"Check that the serving endpoint '{endpoint}' is accessible. "
                "The heuristic-based analysis above is still valid."
            ),
            impact=Impact.LOW,
        )]


async def _call_endpoint(
    ws_client: object | None,
    endpoint: str,
    messages: list[ChatMessage],
) -> str:
    """Call the serving endpoint and return the response text."""
    if ws_client is not None:
        return await _call_via_sdk(ws_client, endpoint, messages)
    return await _call_via_http(endpoint, messages)


async def _call_via_sdk(
    ws_client: object,
    endpoint: str,
    messages: list[ChatMessage],
) -> str:
    """Call via Databricks SDK (preferred in app context)."""
    from databricks.sdk import WorkspaceClient

    assert isinstance(ws_client, WorkspaceClient)

    response = await asyncio.to_thread(
        ws_client.serving_endpoints.query,
        name=endpoint,
        messages=messages,
        max_tokens=4096,
        temperature=0.1,
    )

    return _extract_response_text(response)


async def _call_via_http(endpoint: str, messages: list[ChatMessage]) -> str:
    """Fallback: call via HTTP using Databricks SDK config for auth."""
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()

    response = await asyncio.to_thread(
        ws.serving_endpoints.query,
        name=endpoint,
        messages=messages,
        max_tokens=4096,
        temperature=0.1,
    )

    return _extract_response_text(response)


def _extract_response_text(response: object) -> str:
    """Extract text content from a QueryEndpointResponse."""
    if hasattr(response, "choices") and response.choices:
        choice = response.choices[0]
        if hasattr(choice, "message") and hasattr(choice.message, "content"):
            return choice.message.content or ""

    if isinstance(response, dict):
        choices = response.get("choices", [])
        if choices and isinstance(choices[0], dict):
            return choices[0].get("message", {}).get("content", "")

    raise ValueError(f"Unexpected response format: {type(response)}")


def _parse_llm_response(text: str) -> list[Finding]:
    """Parse the LLM JSON response into Finding objects.

    Handles truncated responses by attempting to close the JSON array
    and salvaging complete objects.
    """
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    items = None

    try:
        items = json.loads(text)
    except json.JSONDecodeError:
        pass

    if items is None:
        start = text.find("[")
        end = text.rfind("]")
        if start >= 0 and end > start:
            try:
                items = json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                pass

    if items is None:
        start = text.find("[")
        if start >= 0:
            fragment = text[start:]
            for repair_suffix in ["]", "}]", "\"}]", "\"}  }]"]:
                try:
                    items = json.loads(fragment + repair_suffix)
                    items = [i for i in items if isinstance(i, dict) and "name" in i]
                    if items:
                        logger.info("Recovered %d findings from truncated LLM response", len(items))
                        break
                except json.JSONDecodeError:
                    continue

    if items is None:
        logger.warning("Could not parse LLM response as JSON: %s", text[:300])
        return []

    if not isinstance(items, list):
        items = [items]

    findings: list[Finding] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue

        sev_str = item.get("severity", "info").lower()
        severity = {"error": Severity.ERROR, "warning": Severity.WARNING}.get(
            sev_str, Severity.INFO
        )
        impact_str = item.get("impact", "medium").lower()
        impact = {"high": Impact.HIGH, "low": Impact.LOW}.get(
            impact_str, Impact.MEDIUM
        )

        findings.append(Finding(
            rule_id=f"llm_insight_{i}",
            category="dbsql_performance",
            name=item.get("name", f"AI Insight #{i + 1}"),
            severity=severity,
            description=item.get("description", ""),
            recommendation=item.get("recommendation", ""),
            impact=impact,
            details={"source": "ai_analysis"},
        ))

    return findings
