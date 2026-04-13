# CLAUDE.md - PBI Health Check Analyser

## What this project is

A **Power BI to Databricks SQL Health Check Optimiser** built as a Databricks App (FastAPI + React). Users upload Power BI model files and/or Databricks SQL query data, and the tool runs best-practice checks, scores the model across categories, optionally invokes an LLM for deeper SQL analysis, and produces prioritised recommendations. Results can be exported to PDF.

Live app: `https://pbianalyzer-1444828305810485.aws.databricksapps.com`

---

## Project structure

```
pbianalyzer/
├── src/pbianalyzer/
│   ├── backend/                    # FastAPI backend (Python)
│   │   ├── app.py                  # FastAPI app factory entry point
│   │   ├── router.py               # All API routes
│   │   ├── models.py               # Pydantic API response models
│   │   ├── scoring.py              # Health score computation + category weights
│   │   ├── pdf_report.py           # PDF export (fpdf2, pure Python)
│   │   ├── core/                   # App factory, static files, DI
│   │   ├── parsers/                # File format parsers
│   │   │   ├── models.py           # Domain models (PBIModel, Finding, HealthReport, etc.)
│   │   │   ├── bim.py              # .bim (TMSL JSON) parser
│   │   │   ├── pbix.py             # .pbix/.pbit parser (legacy JSON + pbixray for binary)
│   │   │   ├── pbip.py             # .pbip (ZIP) parser
│   │   │   ├── tmdl.py             # TMDL text format parser (used by pbip)
│   │   │   ├── query_json.py       # DBSQL query history JSON parser
│   │   │   └── query_profile.py    # DBSQL query profile JSON parser
│   │   ├── rules/                  # Rule engine
│   │   │   ├── definitions.yaml    # Rule catalog: IDs, thresholds, descriptions
│   │   │   ├── engine.py           # Orchestrates all check modules
│   │   │   └── checks/             # Python check functions by category
│   │   └── analyzers/              # External integrations
│   │       ├── dbsql.py            # Live DBSQL query history via SDK
│   │       ├── llm_query_analyzer.py  # LLM-powered analysis via serving endpoint
│   │       ├── dax_patterns.py     # DAX-to-SQL regex patterns and translation rules
│   │       ├── dax_to_metrics.py   # DAX measure classifier for UC Metric View migration
│   │       ├── metrics_converter.py # UC Metric View YAML/SQL generator
│   │       ├── m_query_parser.py   # Power Query M expression parser
│   │       ├── m_to_sql.py         # M step to SQL fragment translator
│   │       ├── m_query_analyzer.py # Per-table M query migration classifier
│   │       └── pipeline_generator.py # DABs bundle generator (SDP SQL + YAML → ZIP)
│   ├── ui/                         # React frontend (TypeScript)
│   │   ├── routes/index.tsx        # Main page: upload + dashboard
│   │   ├── components/health-check/
│   │   │   ├── file-upload.tsx     # Drag-drop upload form
│   │   │   ├── health-dashboard.tsx # Results UI (tabs, scores, findings, PDF export)
│   │   │   ├── metrics-migration.tsx # UC Metrics Migration tab (feasibility + YAML generator)
│   │   │   ├── pipeline-migration.tsx # Pipeline Migration tab (M query analysis + DABs download)
│   │   │   └── types.ts           # Frontend type definitions
│   │   └── lib/api.ts             # Auto-generated OpenAPI client + React Query hooks
│   └── __dist__/                   # Built frontend (Vite output, baked into wheel)
├── pyproject.toml                  # Python deps, apx config, build config
├── databricks.yml                  # Databricks Asset Bundle config
├── app.yml                         # Runtime command for deployed app
├── package.json                    # Frontend deps (React 19, TanStack, Vite)
└── .build/                         # Build output (wheel + requirements.txt + app.yml)
```

---

## Architecture and data flow

```
Upload (.bim/.pbix/.pbip + optional JSON) 
    → POST /api/analyze
    → Parser (by file extension) → PBIModel
    → JSON auto-detection → QueryHistoryData OR QueryProfile
    → RuleEngine.analyze(model, query_data, query_profile) → [Finding]
    → (optional) analyze_with_llm(profile, endpoint) → [Finding] (AI)
    → compute_health_report(all_findings, mode) → HealthReport
    → AnalysisResponse JSON → Frontend dashboard
    → (optional) POST /api/export-pdf → PDF bytes
```

**Analysis modes** determine which categories are scored:
- `file-only` / `pbix` — PBI model checks only
- `file+queries` / `pbix+queries` — PBI model + DBSQL history
- `file+profile` / `pbix+profile` — PBI model + DBSQL query profile
- `profile` — query profile only (no PBI model)
- `live` — model + live query history from warehouse

---

## Key conventions

### Rule engine pattern

Rules are defined in two places that must stay in sync:

1. **`rules/definitions.yaml`** — Rule metadata: `id`, `category`, `name`, `description`, `severity`, `impact`, `threshold` values, `recommendation`, `reference` URL
2. **`rules/checks/*.py`** — Python functions that use `thresholds.get("rule_id", {})` and emit `Finding` objects with matching `rule_id`

To add a new rule:
1. Add entry to `definitions.yaml` with a unique `id` and appropriate `category`
2. Add check logic in the corresponding `checks/{category}.py` file
3. The `Finding.rule_id` must match the YAML `id` exactly

### Categories and scoring

Eight categories with fixed weights (in `scoring.py`):

| Category | Weight | Check module |
|----------|--------|-------------|
| `data_model` | 0.20 | `model_structure.py` |
| `dax_quality` | 0.15 | `dax_quality.py` |
| `storage_modes` | 0.15 | `storage_modes.py` |
| `parallelization` | 0.10 | `parallelization.py` |
| `report_design` | 0.10 | `report_design.py` |
| `connectivity` | 0.10 | `connectivity.py` |
| `dbsql_performance` | 0.20 | `dbsql_performance.py` + `query_profile_checks.py` |
| `uc_metrics_feasibility` | 0.00 | Informational only — not scored |

Scoring: each category starts at 100, deducts per finding (ERROR: -15, WARNING: -8, INFO: -3). Overall score is weighted average of assessed categories. `uc_metrics_feasibility` has weight 0 and is purely informational.

### LLM integration

- File: `analyzers/llm_query_analyzer.py`
- Uses **Databricks serving endpoint** (default: `databricks-claude-opus-4-6`)
- Configurable via `PBIANALYZER_LLM_ENDPOINT` env var or `llm_endpoint` form field
- System prompt uses the "4 S's" framework (Skew, Spill, Shuffle, Small Files)
- LLM findings get `details={"source": "ai_analysis"}` so the UI can badge them
- Response parsing includes JSON truncation recovery for partial responses

### Finding model

All findings (heuristic and AI) use the same `Finding` Pydantic model from `parsers/models.py`:
```python
Finding(
    rule_id="...",          # matches definitions.yaml id (or llm_insight_N)
    category="...",         # one of the 7 category keys
    name="...",
    description="...",
    severity=Severity.ERROR | WARNING | INFO,
    impact=Impact.HIGH | MEDIUM | LOW,
    recommendation="...",   # may contain ```code blocks``` for SQL
    reference_url="...",
    details={...},          # optional metadata, e.g. {"source": "ai_analysis"}
)
```

### UC Metrics Migration

Two new analyzers convert PBI model measures to Databricks UC Metric View definitions:

1. **Feasibility Analyzer** (`analyzers/dax_to_metrics.py`) — classifies each DAX measure into three tiers:
   - **direct**: Simple aggregates (SUM, COUNT, AVERAGE, etc.) that map 1:1 to SQL
   - **translatable**: Patterns with known SQL equivalents (CALCULATE+FILTER, DIVIDE, TOTALYTD, etc.)
   - **manual**: Complex DAX requiring human rewrite (VAR/RETURN, nested CALCULATE, iterators, TOPN, etc.)

2. **Metric View Converter** (`analyzers/metrics_converter.py`) — generates a UC Metric View YAML definition:
   - Maps PBI relationships to `joins` section
   - Maps dimension table columns to `dimensions`
   - Converts direct/translatable measures to SQL `expr` in `measures`
   - Includes manual measures as YAML comments with original DAX for reference

Classification priority: manual patterns checked first (to avoid e.g. TOPN being misclassified as a direct SUM), then translatable, then direct.

DAX pattern rules are in `analyzers/dax_patterns.py`. To add a new pattern, add a `DaxPattern` to `DIRECT_PATTERNS`, `TRANSLATABLE_PATTERNS`, or `MANUAL_PATTERNS`.

**API endpoints:**
- `POST /api/analyze-metrics` — accepts model file, returns `MetricsAnalysisResponse` with per-measure classifications
- `POST /api/generate-metric-view` — accepts model file + catalog/schema, returns YAML and SQL

**Frontend:** A "UC Metrics Migration" tab appears in the dashboard when a PBI model with measures is uploaded. It shows a feasibility score ring, measure classification table, proposed joins/dimensions, and a "Generate Metric View" form.

### Pipeline Migration (M Query → DABs)

Four modules handle converting PBI M queries into Databricks SDP pipelines packaged as Asset Bundles:

1. **M Query Parser** (`analyzers/m_query_parser.py`) — parses Power Query M `let...in` expressions into structured steps:
   - Extracts source info (Databricks.Catalogs, SQL Server, CSV, etc.)
   - Classifies each step: filter, rename, type_cast, add_column, group, join, sort, etc.
   - Handles `#"quoted step names"`, nested parentheses, and M string escaping

2. **M-to-SQL Translator** (`analyzers/m_to_sql.py`) — converts parsed M steps to SQL fragments:
   - `Table.SelectRows` → `WHERE` clause
   - `Table.RenameColumns` → column aliases
   - `Table.TransformColumnTypes` → `CAST` operations
   - `Table.AddColumn` → computed columns with M→SQL expression translation
   - `Table.Group` → `GROUP BY` with aggregate functions
   - `Table.NestedJoin` / `Table.Join` → `JOIN` clauses
   - `Table.RemoveColumns` / `Table.SelectColumns` → column projection
   - `Table.Distinct` → `DISTINCT`
   - `Table.Sort` → `ORDER BY`
   - `Table.ReplaceValue` → `COALESCE` or `CASE WHEN`

3. **M Query Analyzer** (`analyzers/m_query_analyzer.py`) — orchestrates per-table analysis:
   - Classifies each table as `auto` (all steps converted), `partial` (some manual), or `manual` (no conversion)
   - Suggests medallion layer (bronze/silver/gold) based on transformation complexity
   - Identifies Databricks vs non-Databricks sources
   - Computes overall migration readiness score

4. **Pipeline Generator** (`analyzers/pipeline_generator.py`) — produces a complete DABs bundle:
   - Bronze SQL: raw `CREATE OR REFRESH MATERIALIZED VIEW` for each source table
   - Silver SQL: transformations referencing `LIVE.bronze_*` tables
   - Gold SQL: aggregations and (optionally) UC Metric Views from DAX
   - `databricks.yml`: bundle config with catalog/schema variables and dev/prod targets
   - `resources/pipeline.yml`: SDP pipeline resource definition
   - `docs/manual_migration_notes.md`: manual migration guide for unconvertible tables
   - `README.md`: deployment instructions and migration summary
   - Output is a downloadable ZIP file

5. **LLM M Translator** (`analyzers/llm_m_translator.py`) — AI-powered second pass:
   - `translate_m_steps_with_llm`: translates individual M steps that the heuristic couldn't handle (Pivot, Unpivot, FillDown, complex `each` expressions)
   - `enhance_full_query_with_llm`: reviews heuristic SQL against the original M query and adds missing transformations (renames, casts, filter fixes)
   - Uses the same Databricks serving endpoint pattern as the query profile LLM analyzer
   - All AI calls run concurrently via `asyncio.gather` for minimal latency

**Two-pass architecture:**
- **Pass 1 (instant, heuristic):** Regex-based parsing and translation — always runs
- **Pass 2 (AI, optional):** LLM reviews and enhances results — only runs when `llm_endpoint` is provided
- For `auto` tables with transformations: AI reviews the heuristic SQL for completeness
- For `partial`/`manual` tables: AI attempts to translate unconverted steps AND reviews overall SQL
- AI results include confidence ratings (high/medium/low) and change descriptions
- If AI fails, heuristic results are returned with a warning

**API endpoints:**
- `POST /api/analyze-m-queries` — accepts model file + optional `llm_endpoint`, returns `MQueryAnalysisResponse` with per-table migration classification and AI enhancements
- `POST /api/generate-pipeline-bundle` — accepts model file + catalog/schema/options, returns ZIP of complete DABs bundle

**Frontend:** A "Pipeline Migration" tab appears when M queries are found. Shows migration readiness score, per-table analysis with expandable heuristic SQL and AI-enhanced SQL, AI change descriptions, step-level AI translations, data source inventory, and a "Generate DABs Bundle" button to download the ZIP. When an LLM endpoint is configured (same localStorage key `pbi_llm_endpoint`), the AI pass runs automatically and results are badged with "AI Enhanced".

### Frontend conventions

- API prefix is always `/api` (set in `_metadata.py`)
- Main page (`routes/index.tsx`) uses raw `fetch` for uploads
- `lib/api.ts` is auto-generated by apx from the OpenAPI spec — do not edit manually
- LLM endpoint preference persisted to `localStorage` key `pbi_llm_endpoint`
- PBI, DBSQL, UC Metrics, and Pipeline Migration findings are split into tabs when applicable
- Code blocks in recommendations are rendered with syntax highlighting and copy buttons

---

## Supported file formats

| Format | Extension | Parser | Notes |
|--------|-----------|--------|-------|
| BIM (TMSL JSON) | `.bim` | `parse_bim` | Standard tabular model JSON |
| PBIX / PBIT | `.pbix`, `.pbit` | `parse_pbix` | Legacy JSON + `pbixray` for modern binary format |
| PBIP (ZIP) | `.zip` | `parse_pbip_zip` | Prefers `model.bim`, falls back to TMDL `definition/` folder |
| Query History | `.json` | `parse_query_json` | Exported from `system.query.history` |
| Query Profile | `.json` | `parse_query_profile_dict` | DBSQL Query Profile export (has `graphs` key) |

JSON uploads are auto-detected: if the JSON has a `graphs` key it's treated as a query profile, otherwise as query history.

---

## Development

```bash
apx dev start          # Start local dev server at http://localhost:9000
apx dev stop           # Stop dev server
apx build              # Build wheel + frontend into .build/
```

The dev server runs FastAPI (with hot reload) + Vite frontend dev server behind a proxy on port 9000.

## Deployment to Databricks

```bash
databricks bundle deploy --target dev                    # Upload bundle
databricks apps deploy pbianalyzer \
  --profile e2-field-eng \
  --source-code-path /Workspace/Users/mike.dobing@databricks.com/.bundle/pbianalyzer/dev/files/.build
```

**Important:** The `--source-code-path` must point to the `.build` subdirectory (contains the wheel + `requirements.txt`), not the raw source root.

Bundle config is in `databricks.yml`, target workspace profile is `e2-field-eng`.

---

## Dependencies to be aware of

- **fpdf2** — PDF generation (pure Python, no system deps needed)
- **pbixray** — Parses binary DataModel from modern .pbix files
- **databricks-sdk** — Workspace client for live queries and LLM serving endpoints
- **networkx** — Used for model relationship graph analysis
- All Python deps in `pyproject.toml`, frontend deps in `package.json`
- `lib/api.ts` is auto-generated — do not hand-edit

---

## Common tasks

**Add a new heuristic check:** Edit `definitions.yaml` + corresponding `checks/*.py` file. Match `rule_id` strings exactly.

**Modify the LLM prompt:** Edit `SYSTEM_PROMPT` in `analyzers/llm_query_analyzer.py`. The prompt defines analysis focus areas and the JSON output schema.

**Add a new file format:** Create parser in `parsers/`, return `PBIModel`. Wire it into `router.py`'s file extension detection.

**Change scoring weights:** Edit `CATEGORY_CONFIG` in `scoring.py`.

**Add a new API endpoint:** Add route to `router.py`. The OpenAPI spec regenerates on dev server restart, which updates `lib/api.ts`.

**Add a new DAX pattern:** Add a `DaxPattern` to the appropriate list in `analyzers/dax_patterns.py` (DIRECT, TRANSLATABLE, or MANUAL). If it's a direct pattern with a SQL equivalent, provide `sql_template`. The classifier in `dax_to_metrics.py` checks manual first, then translatable, then direct.

**Modify metric view generation:** Edit `analyzers/metrics_converter.py`. The `generate_metric_view_yaml` function builds the YAML structure and wraps it in a `CREATE VIEW ... WITH METRICS` SQL statement.

**Add a new M step translator:** Add a case to `_STEP_CLASSIFIERS` in `m_query_parser.py` for detection, then add a `_translate_*` function in `m_to_sql.py` and register it in the `translators` dict inside `_translate_step`.

**Modify pipeline bundle output:** Edit `pipeline_generator.py`. The `_build_*_sql` functions generate SDP SQL for each layer. The `_build_databricks_yml` and `_build_pipeline_yml` functions generate the bundle configuration.

**Add a new M expression function mapping:** Add to `_m_each_expr_to_sql` in `m_to_sql.py` for expression-level M→SQL mappings (e.g. `Text.Upper` → `UPPER`), or to `_M_LIST_FUNC_TO_SQL` for aggregate function mappings.
