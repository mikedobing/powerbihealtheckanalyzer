import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";

interface AIStepTranslation {
  step_name: string;
  sql: string | null;
  sql_type: string;
  notes: string;
  confidence: string;
}

interface TableMigration {
  table_name: string;
  has_m_query: boolean;
  source_type: string;
  source_fqn: string;
  tier: "auto" | "partial" | "manual";
  generated_sql: string;
  step_count: number;
  auto_steps: number;
  partial_steps: number;
  manual_steps: number;
  manual_step_names: string[];
  warnings: string[];
  is_calculated_table: boolean;
  is_databricks_source: boolean;
  suggested_layer: string;
  original_m: string;
  ai_enhanced: boolean;
  ai_sql: string;
  ai_changes: string[];
  ai_confidence: string;
  ai_step_translations: AIStepTranslation[];
}

interface SourceInfo {
  fqn: string;
  type: string;
  is_databricks: boolean;
}

export interface MQueryAnalysis {
  tables: TableMigration[];
  total_tables: number;
  tables_with_m: number;
  auto_count: number;
  partial_count: number;
  manual_count: number;
  databricks_source_count: number;
  non_databricks_source_count: number;
  migration_score: number;
  unique_sources: SourceInfo[];
  warnings: string[];
  ai_enhanced: boolean;
}

function TierBadge({ tier }: { tier: string }) {
  const styles: Record<string, string> = {
    auto: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 border-green-200 dark:border-green-800",
    partial:
      "bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-400 border-amber-200 dark:border-amber-800",
    manual:
      "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400 border-red-200 dark:border-red-800",
  };
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${styles[tier] || ""}`}
    >
      {tier}
    </span>
  );
}

function LayerBadge({ layer }: { layer: string }) {
  const styles: Record<string, string> = {
    bronze:
      "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-400 border-orange-200 dark:border-orange-800",
    silver:
      "bg-slate-100 text-slate-700 dark:bg-slate-800/50 dark:text-slate-300 border-slate-200 dark:border-slate-700",
    gold: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400 border-yellow-200 dark:border-yellow-800",
  };
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${styles[layer] || ""}`}
    >
      {layer}
    </span>
  );
}

function ScoreRing({
  score,
  size = 100,
}: {
  score: number;
  size?: number;
}) {
  const radius = (size - 12) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (score / 100) * circumference;
  const color =
    score >= 75
      ? "text-green-500"
      : score >= 50
        ? "text-amber-500"
        : "text-red-500";

  return (
    <div className="relative inline-flex items-center justify-center">
      <svg width={size} height={size} className="-rotate-90">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth={6}
          className="text-muted/30"
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth={6}
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          className={color}
        />
      </svg>
      <span className="absolute text-xl font-bold">{score}%</span>
    </div>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [text]);
  return (
    <button
      type="button"
      onClick={handleCopy}
      className="absolute top-2 right-2 px-2 py-1 rounded text-[10px] font-medium bg-muted-foreground/10 hover:bg-muted-foreground/20 text-muted-foreground transition-colors"
    >
      {copied ? "Copied!" : "Copy"}
    </button>
  );
}

function AIBadge({ confidence }: { confidence: string }) {
  const styles: Record<string, string> = {
    high: "bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400 border-violet-200 dark:border-violet-800",
    medium:
      "bg-violet-100/60 text-violet-600 dark:bg-violet-900/20 dark:text-violet-400 border-violet-200/60 dark:border-violet-800/60",
    low: "bg-gray-100 text-gray-600 dark:bg-gray-800/30 dark:text-gray-400 border-gray-200 dark:border-gray-700",
  };
  return (
    <span
      className={`inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium border ${styles[confidence] || styles.low}`}
    >
      AI {confidence}
    </span>
  );
}

function ExpandableSQL({
  sql,
  label,
}: {
  sql: string;
  label: string;
}) {
  const [open, setOpen] = useState(false);
  if (!sql) return null;
  return (
    <div className="mt-2">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="text-xs text-blue-500 hover:text-blue-400 underline"
      >
        {open ? `Hide ${label}` : `Show ${label}`}
      </button>
      {open && (
        <div className="relative mt-1">
          <pre className="p-3 pr-16 rounded-lg bg-zinc-900 dark:bg-zinc-950 text-emerald-400 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-all border border-zinc-700/50">
            {sql.trim()}
          </pre>
          <CopyButton text={sql.trim()} />
        </div>
      )}
    </div>
  );
}

export function PipelineMigration({
  analysis,
  modelFile,
}: {
  analysis: MQueryAnalysis;
  modelFile: File | null;
}) {
  const [generating, setGenerating] = useState(false);
  const [showGenerator, setShowGenerator] = useState(false);
  const [catalog, setCatalog] = useState("my_catalog");
  const [schema, setSchema] = useState("my_schema");
  const [bundleName, setBundleName] = useState("");
  const [includeMetrics, setIncludeMetrics] = useState(true);
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  const handleGenerate = useCallback(async () => {
    if (!modelFile) return;
    setGenerating(true);
    try {
      const formData = new FormData();
      formData.append("model_file", modelFile);
      formData.append("catalog", catalog);
      formData.append("schema_name", schema);
      if (bundleName) formData.append("bundle_name", bundleName);
      formData.append("include_metrics", String(includeMetrics));

      const res = await fetch("/api/generate-pipeline-bundle", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const errText = await res.text();
        alert(`Generation failed: ${errText}`);
        return;
      }

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      const disp = res.headers.get("Content-Disposition") || "";
      const fnMatch = disp.match(/filename="([^"]+)"/);
      a.download = fnMatch ? fnMatch[1] : "pipeline_bundle.zip";
      a.href = url;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      alert(`Error: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setGenerating(false);
    }
  }, [modelFile, catalog, schema, bundleName, includeMetrics]);

  const tablesWithM = analysis.tables.filter((t) => t.has_m_query);
  const calcTables = analysis.tables.filter((t) => t.is_calculated_table);

  return (
    <div className="space-y-6">
      {/* Header row with score + generate button */}
      <div className="flex items-start justify-between gap-6">
        <div className="flex items-center gap-6">
          <ScoreRing score={analysis.migration_score} />
          <div className="space-y-1">
            <h3 className="text-lg font-semibold">
              Pipeline Migration Readiness
              {analysis.ai_enhanced && (
                <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-medium bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400 border border-violet-200 dark:border-violet-800">
                  AI Enhanced
                </span>
              )}
            </h3>
            <div className="flex gap-4 text-sm text-muted-foreground">
              <span>
                <strong className="text-green-500">{analysis.auto_count}</strong>{" "}
                auto
              </span>
              <span>
                <strong className="text-amber-500">
                  {analysis.partial_count}
                </strong>{" "}
                partial
              </span>
              <span>
                <strong className="text-red-500">{analysis.manual_count}</strong>{" "}
                manual
              </span>
            </div>
            <div className="flex gap-4 text-xs text-muted-foreground">
              <span>{analysis.tables_with_m} tables with M queries</span>
              <span>{analysis.databricks_source_count} Databricks sources</span>
              {analysis.non_databricks_source_count > 0 && (
                <span className="text-amber-500">
                  {analysis.non_databricks_source_count} non-Databricks
                </span>
              )}
            </div>
          </div>
        </div>

        {modelFile && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowGenerator(!showGenerator)}
            className="shrink-0"
          >
            {showGenerator ? "Hide Generator" : "Generate DABs Bundle"}
          </Button>
        )}
      </div>

      {/* Inline generator panel */}
      {showGenerator && (
        <div className="rounded-lg border border-blue-500/30 bg-blue-500/5 p-4 space-y-4">
          <h4 className="text-sm font-semibold">Generate Databricks Asset Bundle</h4>
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className="block text-xs font-medium mb-1 text-muted-foreground">
                Catalog
              </label>
              <input
                type="text"
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
                className="w-full px-3 py-1.5 rounded-md border bg-background text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-medium mb-1 text-muted-foreground">
                Schema
              </label>
              <input
                type="text"
                value={schema}
                onChange={(e) => setSchema(e.target.value)}
                className="w-full px-3 py-1.5 rounded-md border bg-background text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-medium mb-1 text-muted-foreground">
                Bundle Name
              </label>
              <input
                type="text"
                value={bundleName}
                onChange={(e) => setBundleName(e.target.value)}
                placeholder="auto from model name"
                className="w-full px-3 py-1.5 rounded-md border bg-background text-sm"
              />
            </div>
          </div>
          <div className="flex items-center gap-4">
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={includeMetrics}
                onChange={(e) => setIncludeMetrics(e.target.checked)}
                className="rounded"
              />
              Include UC Metric View in gold layer
            </label>
            <Button
              size="sm"
              onClick={handleGenerate}
              disabled={generating}
            >
              {generating ? "Generating..." : "Download Bundle ZIP"}
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">
            Generates a complete Databricks Asset Bundle with SDP pipeline SQL
            files, databricks.yml, and a README. Bronze tables for raw ingestion,
            silver for transformations, gold for aggregations and metric views.
          </p>
        </div>
      )}

      {/* Warnings */}
      {analysis.warnings.length > 0 && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3">
          <h4 className="text-sm font-semibold text-amber-500 mb-1">Warnings</h4>
          <ul className="text-xs text-muted-foreground space-y-1">
            {analysis.warnings.map((w, i) => (
              <li key={i}>• {w}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Data Sources */}
      {analysis.unique_sources.length > 0 && (
        <div>
          <h4 className="text-sm font-semibold mb-2">Data Sources</h4>
          <div className="flex flex-wrap gap-2">
            {analysis.unique_sources.map((s, i) => (
              <div
                key={i}
                className={`px-3 py-1.5 rounded-md border text-xs ${
                  s.is_databricks
                    ? "border-green-500/30 bg-green-500/5 text-green-600 dark:text-green-400"
                    : "border-amber-500/30 bg-amber-500/5 text-amber-600 dark:text-amber-400"
                }`}
              >
                <span className="font-medium">{s.type}</span>
                {s.fqn && <span className="ml-1 opacity-70">{s.fqn}</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Tables with M Queries */}
      {tablesWithM.length > 0 && (
        <div>
          <h4 className="text-sm font-semibold mb-2">
            Tables with M Queries ({tablesWithM.length})
          </h4>
          <div className="rounded-lg border overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-muted/50 border-b">
                  <th className="text-left px-3 py-2 font-medium">Table</th>
                  <th className="text-left px-3 py-2 font-medium">Source</th>
                  <th className="text-center px-3 py-2 font-medium">Tier</th>
                  <th className="text-center px-3 py-2 font-medium">Layer</th>
                  <th className="text-center px-3 py-2 font-medium">Steps</th>
                  <th className="text-center px-3 py-2 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {tablesWithM.map((t) => {
                  const isExpanded = expandedRow === t.table_name;
                  return (
                    <>
                      <tr
                        key={t.table_name}
                        className="border-b cursor-pointer hover:bg-muted/30 transition-colors"
                        onClick={() =>
                          setExpandedRow(
                            isExpanded ? null : t.table_name,
                          )
                        }
                      >
                        <td className="px-3 py-2 font-medium">
                          <span className="mr-1 text-muted-foreground">
                            {isExpanded ? "▾" : "▸"}
                          </span>
                          {t.table_name}
                        </td>
                        <td className="px-3 py-2 text-xs text-muted-foreground truncate max-w-[200px]">
                          {t.source_fqn || t.source_type || "—"}
                        </td>
                        <td className="px-3 py-2 text-center">
                          <TierBadge tier={t.tier} />
                        </td>
                        <td className="px-3 py-2 text-center">
                          <LayerBadge layer={t.suggested_layer} />
                        </td>
                        <td className="px-3 py-2 text-center text-xs">
                          {t.step_count > 0 ? (
                            <span>
                              <span className="text-green-500">
                                {t.auto_steps}
                              </span>
                              /
                              <span className="text-amber-500">
                                {t.partial_steps}
                              </span>
                              /
                              <span className="text-red-500">
                                {t.manual_steps}
                              </span>
                            </span>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td className="px-3 py-2 text-center">
                          <div className="flex items-center justify-center gap-1">
                            {t.is_databricks_source ? (
                              <span className="text-green-500 text-xs">✓ Databricks</span>
                            ) : (
                              <span className="text-amber-500 text-xs">
                                ⚠ External
                              </span>
                            )}
                            {t.ai_enhanced && (
                              <span className="inline-flex items-center px-1 py-0.5 rounded-full text-[9px] font-medium bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400">
                                AI
                              </span>
                            )}
                          </div>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr key={`${t.table_name}-detail`}>
                          <td colSpan={6} className="p-4 bg-muted/20">
                            <div className="space-y-3">
                              {t.warnings.length > 0 && (
                                <div className="text-xs text-amber-500">
                                  {t.warnings.map((w, i) => (
                                    <div key={i}>⚠ {w}</div>
                                  ))}
                                </div>
                              )}
                              {t.manual_step_names.length > 0 && (
                                <div className="text-xs">
                                  <span className="font-medium">
                                    Manual steps:{" "}
                                  </span>
                                  {t.manual_step_names.join(", ")}
                                </div>
                              )}
                              <ExpandableSQL
                                sql={t.generated_sql}
                                label="Heuristic SQL"
                              />
                              {t.ai_enhanced && t.ai_sql && (
                                <div className="space-y-1">
                                  <div className="flex items-center gap-2">
                                    <AIBadge confidence={t.ai_confidence} />
                                    {t.ai_changes.length > 0 && (
                                      <span className="text-xs text-violet-500">
                                        {t.ai_changes.length} improvement{t.ai_changes.length !== 1 ? "s" : ""}
                                      </span>
                                    )}
                                  </div>
                                  {t.ai_changes.length > 0 && (
                                    <ul className="text-xs text-muted-foreground pl-4 list-disc">
                                      {t.ai_changes.map((c, i) => (
                                        <li key={i}>{c}</li>
                                      ))}
                                    </ul>
                                  )}
                                  <ExpandableSQL
                                    sql={t.ai_sql}
                                    label="AI-Enhanced SQL"
                                  />
                                </div>
                              )}
                              {t.ai_step_translations.length > 0 && (
                                <div className="space-y-2">
                                  <h5 className="text-xs font-medium flex items-center gap-1">
                                    AI Step Translations
                                    <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400">
                                      AI
                                    </span>
                                  </h5>
                                  {t.ai_step_translations.map((s, i) => (
                                    <div key={i} className="text-xs rounded border p-2 space-y-1">
                                      <div className="flex items-center gap-2">
                                        <span className="font-medium">{s.step_name}</span>
                                        {s.confidence && <AIBadge confidence={s.confidence} />}
                                      </div>
                                      {s.notes && (
                                        <p className="text-muted-foreground">{s.notes}</p>
                                      )}
                                      {s.sql && (
                                        <ExpandableSQL sql={s.sql} label="SQL" />
                                      )}
                                      {!s.sql && (
                                        <p className="text-red-400 text-xs italic">
                                          Could not translate this step
                                        </p>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              )}
                              <ExpandableSQL
                                sql={t.original_m}
                                label="Original M Query"
                              />
                            </div>
                          </td>
                        </tr>
                      )}
                    </>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Calculated Tables */}
      {calcTables.length > 0 && (
        <div>
          <h4 className="text-sm font-semibold mb-2">
            Calculated Tables ({calcTables.length})
          </h4>
          <div className="rounded-lg border p-3 space-y-2">
            {calcTables.map((t) => (
              <div key={t.table_name} className="text-sm">
                <span className="font-medium">{t.table_name}</span>
                <span className="ml-2 text-xs text-muted-foreground">
                  DAX calculated table — convert via UC Metrics tab or
                  materialize as Delta table
                </span>
                <ExpandableSQL
                  sql={t.original_m}
                  label="DAX Expression"
                />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
