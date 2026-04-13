import { useState, useCallback, useEffect } from "react";
import { Button } from "@/components/ui/button";
import type {
  AnalysisResult,
  CategoryScore,
  Finding,
  QueryProfileSummary,
} from "./types";
import { MetricsMigration } from "./metrics-migration";
import type { MetricsAnalysis } from "./metrics-migration";
import { PipelineMigration } from "./pipeline-migration";
import type { MQueryAnalysis } from "./pipeline-migration";

const SQL_KEYWORDS =
  /^(ANALYZE TABLE|ALTER TABLE|CREATE |DROP |SELECT |INSERT |UPDATE |DELETE |OPTIMIZE |SET |GRANT |REVOKE |DESCRIBE |SHOW |USE |WITH )/i;

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      navigator.clipboard.writeText(text).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      });
    },
    [text],
  );
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

function CodeBlock({ code }: { code: string }) {
  return (
    <div
      className="relative group mt-2 mb-1"
      onClick={(e) => e.stopPropagation()}
    >
      <pre className="p-3 pr-16 rounded-lg bg-zinc-900 dark:bg-zinc-950 text-emerald-400 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-all border border-zinc-700/50">
        {code.trim()}
      </pre>
      <CopyButton text={code.trim()} />
    </div>
  );
}

function FormattedText({ text }: { text: string }) {
  const parts: React.ReactNode[] = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    const fenceMatch = remaining.match(/```(?:\w*\n?)?([\s\S]*?)```/);
    if (fenceMatch && fenceMatch.index !== undefined) {
      if (fenceMatch.index > 0) {
        parts.push(
          ...renderInlineSegments(
            remaining.slice(0, fenceMatch.index),
            key,
          ),
        );
        key += 100;
      }
      parts.push(<CodeBlock key={key++} code={fenceMatch[1]} />);
      remaining = remaining.slice(fenceMatch.index + fenceMatch[0].length);
      continue;
    }

    parts.push(...renderInlineSegments(remaining, key));
    break;
  }

  return <div className="text-sm mt-1 space-y-1">{parts}</div>;
}

function renderInlineSegments(
  text: string,
  startKey: number,
): React.ReactNode[] {
  const nodes: React.ReactNode[] = [];
  let key = startKey;

  const segments = text.split(/(`[^`]+`)/g);
  for (const seg of segments) {
    if (seg.startsWith("`") && seg.endsWith("`") && seg.length > 2) {
      const code = seg.slice(1, -1);
      if (SQL_KEYWORDS.test(code.trim())) {
        nodes.push(<CodeBlock key={key++} code={code} />);
      } else {
        nodes.push(
          <code
            key={key++}
            className="px-1.5 py-0.5 rounded bg-muted text-xs font-mono"
          >
            {code}
          </code>,
        );
      }
    } else if (seg.trim()) {
      const lines = seg.split(/;\s*(?=[A-Z])/);
      if (lines.length > 1 && lines.some((l) => SQL_KEYWORDS.test(l.trim()))) {
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          if (SQL_KEYWORDS.test(trimmed)) {
            const stmt = trimmed.endsWith(";") ? trimmed : trimmed + ";";
            nodes.push(<CodeBlock key={key++} code={stmt} />);
          } else {
            nodes.push(
              <span key={key++} className="block">
                {trimmed}
              </span>,
            );
          }
        }
      } else {
        nodes.push(<span key={key++}>{seg}</span>);
      }
    }
  }

  return nodes;
}

function ScoreRing({ score, size = 120 }: { score: number; size?: number }) {
  const radius = (size - 12) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (score / 100) * circumference;
  const color =
    score >= 80
      ? "text-green-500"
      : score >= 60
        ? "text-yellow-500"
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
          strokeWidth="6"
          className="text-muted/30"
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth="6"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          className={`${color} transition-all duration-1000`}
        />
      </svg>
      <div className="absolute flex flex-col items-center">
        <span className={`text-3xl font-bold ${color}`}>
          {Math.round(score)}
        </span>
        <span className="text-xs text-muted-foreground">/ 100</span>
      </div>
    </div>
  );
}

function CategoryBar({ cat }: { cat: CategoryScore }) {
  if (!cat.assessed) {
    return (
      <div className="flex items-center justify-between p-3 rounded-lg border bg-card/50">
        <span className="text-sm font-medium">{cat.display_name}</span>
        <span className="text-xs text-muted-foreground italic">
          Not assessed
        </span>
      </div>
    );
  }

  const color =
    cat.score >= 80
      ? "bg-green-500"
      : cat.score >= 60
        ? "bg-yellow-500"
        : "bg-red-500";

  return (
    <div className="p-3 rounded-lg border bg-card/50 space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium">{cat.display_name}</span>
        <div className="flex items-center gap-2">
          {cat.findings.length > 0 && (
            <span className="text-xs text-muted-foreground">
              {cat.findings.length} finding
              {cat.findings.length !== 1 ? "s" : ""}
            </span>
          )}
          <span className="text-sm font-bold">{Math.round(cat.score)}</span>
        </div>
      </div>
      <div className="h-2 bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full ${color} rounded-full transition-all duration-700`}
          style={{ width: `${cat.score}%` }}
        />
      </div>
    </div>
  );
}

function SeverityBadge({ severity }: { severity: string }) {
  const styles: Record<string, string> = {
    error:
      "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400 border-red-200 dark:border-red-800",
    warning:
      "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400 border-yellow-200 dark:border-yellow-800",
    info: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400 border-blue-200 dark:border-blue-800",
  };
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${styles[severity] || ""}`}
    >
      {severity}
    </span>
  );
}

function ImpactBadge({ impact }: { impact: string }) {
  const styles: Record<string, string> = {
    high: "text-red-600 dark:text-red-400",
    medium: "text-yellow-600 dark:text-yellow-400",
    low: "text-muted-foreground",
  };
  return (
    <span className={`text-xs font-medium ${styles[impact] || ""}`}>
      {impact} impact
    </span>
  );
}

function FindingCard({ finding }: { finding: Finding }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div
      className="border rounded-lg p-4 space-y-2 bg-card hover:bg-accent/30 transition-colors cursor-pointer"
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1 flex-1">
          <div className="flex items-center gap-2 flex-wrap">
            <SeverityBadge severity={finding.severity} />
            <span className="font-medium text-sm">{finding.name}</span>
            <ImpactBadge impact={finding.impact} />
            {finding.details?.source === "ai_analysis" && (
              <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-400 border border-violet-200 dark:border-violet-800">
                AI
              </span>
            )}
          </div>
          <p className="text-sm text-muted-foreground">
            {finding.description}
          </p>
        </div>
      </div>

      {expanded && (
        <div className="pt-2 border-t space-y-3">
          <div>
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
              Recommendation
            </p>
            <FormattedText text={finding.recommendation} />
          </div>
          {finding.reference_url && (
            <a
              href={finding.reference_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
              onClick={(e) => e.stopPropagation()}
            >
              View reference implementation &rarr;
            </a>
          )}
          {Object.keys(finding.details).length > 0 && (
            <details className="text-xs" onClick={(e) => e.stopPropagation()}>
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                Technical details
              </summary>
              <pre className="mt-1 p-2 rounded bg-muted text-xs overflow-auto">
                {JSON.stringify(finding.details, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

function QueryProfileBanner({ summary }: { summary: QueryProfileSummary }) {
  const readMb = (summary.read_bytes / (1024 * 1024)).toFixed(1);
  const timeSec = (summary.total_time_ms / 1000).toFixed(1);
  return (
    <div className="rounded-lg border border-blue-300/50 bg-blue-50 dark:bg-blue-950/20 p-4 space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-sm font-medium text-blue-800 dark:text-blue-300">
          Query Profile Analysis
          {summary.is_pbi_generated && (
            <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400">
              Power BI Generated
            </span>
          )}
          {summary.has_llm_analysis && (
            <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-violet-100 text-violet-800 dark:bg-violet-900/30 dark:text-violet-400">
              AI Analysis
            </span>
          )}
        </p>
        <span className="text-xs text-blue-600 dark:text-blue-400 font-mono">
          {summary.query_id}
        </span>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
        <div>
          <span className="text-xs text-muted-foreground">Total Time</span>
          <p className="font-medium">{timeSec}s</p>
        </div>
        <div>
          <span className="text-xs text-muted-foreground">Data Read</span>
          <p className="font-medium">{readMb} MB</p>
        </div>
        <div>
          <span className="text-xs text-muted-foreground">Rows Read</span>
          <p className="font-medium">{summary.rows_read.toLocaleString()}</p>
        </div>
        <div>
          <span className="text-xs text-muted-foreground">Rows Produced</span>
          <p className="font-medium">
            {summary.rows_produced.toLocaleString()}
          </p>
        </div>
      </div>
      {summary.tables_scanned.length > 0 && (
        <div className="text-xs text-blue-700 dark:text-blue-400">
          <span className="font-medium">Tables scanned:</span>{" "}
          {summary.tables_scanned.join(", ")}
        </div>
      )}
      {summary.join_types.length > 0 && (
        <div className="text-xs text-blue-700 dark:text-blue-400">
          <span className="font-medium">Joins:</span>{" "}
          {summary.join_types.join(", ")}
        </div>
      )}
    </div>
  );
}

const PBI_CATEGORIES = new Set([
  "data_model",
  "dax_quality",
  "storage_modes",
  "parallelization",
  "report_design",
  "connectivity",
]);
const DBSQL_CATEGORIES = new Set(["dbsql_performance"]);

type Tab = "pbi" | "dbsql" | "metrics" | "pipeline";

function sortFindings(findings: Finding[]) {
  const severityOrder: Record<string, number> = {
    error: 0,
    warning: 1,
    info: 2,
  };
  const impactOrder: Record<string, number> = { high: 0, medium: 1, low: 2 };
  return [...findings].sort((a, b) => {
    const sevDiff =
      (severityOrder[a.severity] ?? 3) - (severityOrder[b.severity] ?? 3);
    if (sevDiff !== 0) return sevDiff;
    return (impactOrder[a.impact] ?? 3) - (impactOrder[b.impact] ?? 3);
  });
}

function TabPanel({
  categories,
  allFindings,
}: {
  categories: CategoryScore[];
  allFindings: Finding[];
}) {
  const [activeCategory, setActiveCategory] = useState<string | null>(null);

  const displayFindings = activeCategory
    ? allFindings.filter((f) => f.category === activeCategory)
    : allFindings;

  const sortedFindings = sortFindings(displayFindings);
  const aiCount = allFindings.filter(
    (f) => f.details?.source === "ai_analysis",
  ).length;

  const assessedCats = categories.filter((c) => c.assessed);
  const totalWeight = assessedCats.length || 1;
  const tabScore =
    assessedCats.length > 0
      ? assessedCats.reduce((sum, c) => sum + c.score, 0) / totalWeight
      : 100;

  const findingCount = allFindings.length;
  const errorCount = allFindings.filter((f) => f.severity === "error").length;
  const warnCount = allFindings.filter((f) => f.severity === "warning").length;
  const infoCount = allFindings.filter((f) => f.severity === "info").length;

  return (
    <div className="space-y-6">
      <div className="grid md:grid-cols-[auto_1fr] gap-8">
        <div className="flex flex-col items-center gap-3">
          <ScoreRing score={tabScore} />
          <div className="text-center">
            <p className="text-xs text-muted-foreground mt-1">
              {findingCount} finding{findingCount !== 1 ? "s" : ""}:{" "}
              {errorCount} errors, {warnCount} warnings, {infoCount} info
              {aiCount > 0 && (
                <span className="text-violet-600 dark:text-violet-400">
                  {" "}
                  ({aiCount} from AI)
                </span>
              )}
            </p>
          </div>
        </div>

        <div className="space-y-2">
          {categories.map((cat) => (
            <div
              key={cat.category}
              className="cursor-pointer"
              onClick={() =>
                setActiveCategory(
                  activeCategory === cat.category ? null : cat.category,
                )
              }
            >
              <CategoryBar cat={cat} />
            </div>
          ))}
        </div>
      </div>

      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold">
            {activeCategory
              ? `${categories.find((c) => c.category === activeCategory)?.display_name} Findings`
              : "All Findings"}
          </h2>
          {activeCategory && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setActiveCategory(null)}
            >
              Show all
            </Button>
          )}
        </div>

        {sortedFindings.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            No findings in this category. Looking good!
          </div>
        ) : (
          <div className="space-y-3">
            {sortedFindings.map((f, i) => (
              <FindingCard key={`${f.rule_id}-${i}`} finding={f} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

interface HealthDashboardProps {
  result: AnalysisResult;
  onReset: () => void;
  modelFile?: File | null;
}

export function HealthDashboard({ result, onReset, modelFile }: HealthDashboardProps) {
  const { report } = result;
  const [exporting, setExporting] = useState(false);
  const [metricsAnalysis, setMetricsAnalysis] = useState<MetricsAnalysis | null>(null);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [pipelineAnalysis, setPipelineAnalysis] = useState<MQueryAnalysis | null>(null);
  const [pipelineLoading, setPipelineLoading] = useState(false);

  const pbiCategories = report.categories.filter((c) =>
    PBI_CATEGORIES.has(c.category),
  );
  const dbsqlCategories = report.categories.filter((c) =>
    DBSQL_CATEGORIES.has(c.category),
  );

  const pbiFindings = pbiCategories.flatMap((c) => c.findings);
  const dbsqlFindings = dbsqlCategories.flatMap((c) => c.findings);

  const hasPbi = pbiCategories.some((c) => c.assessed);
  const hasDbsql = dbsqlCategories.some((c) => c.assessed);

  const defaultTab: Tab = hasPbi ? "pbi" : "dbsql";
  const [activeTab, setActiveTab] = useState<Tab>(defaultTab);

  const pbiIssueCount = pbiFindings.filter(
    (f) => f.severity === "error" || f.severity === "warning",
  ).length;
  const dbsqlIssueCount = dbsqlFindings.filter(
    (f) => f.severity === "error" || f.severity === "warning",
  ).length;

  const handleExportPdf = useCallback(async () => {
    setExporting(true);
    try {
      const res = await fetch("/api/export-pdf", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(result),
      });
      if (!res.ok) throw new Error(`Export failed: ${res.status}`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download =
        res.headers
          .get("Content-Disposition")
          ?.match(/filename="?([^"]+)"?/)?.[1] ?? "health-check-report.pdf";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error("PDF export failed:", err);
    } finally {
      setExporting(false);
    }
  }, [result]);

  useEffect(() => {
    if (!modelFile || result.tables_count === 0) return;
    setMetricsLoading(true);
    const fd = new FormData();
    fd.append("model_file", modelFile);
    fetch("/api/analyze-metrics", { method: "POST", body: fd })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => { if (data) setMetricsAnalysis(data); })
      .catch(() => {})
      .finally(() => setMetricsLoading(false));
  }, [modelFile, result.tables_count]);

  useEffect(() => {
    if (!modelFile || result.tables_count === 0) return;
    setPipelineLoading(true);
    const fd = new FormData();
    fd.append("model_file", modelFile);
    fetch("/api/analyze-m-queries", { method: "POST", body: fd })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => { if (data) setPipelineAnalysis(data); })
      .catch(() => {})
      .finally(() => setPipelineLoading(false));
  }, [modelFile, result.tables_count]);

  const hasMetrics = metricsAnalysis !== null && metricsAnalysis.classifications.length > 0;
  const hasPipeline = pipelineAnalysis !== null && pipelineAnalysis.tables_with_m > 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Health Check Results</h1>
          <p className="text-muted-foreground mt-1">
            {result.model_name}
            {result.tables_count > 0 && (
              <>
                {" "}
                &middot; {result.tables_count} tables &middot;{" "}
                {result.relationships_count} relationships &middot;{" "}
                {result.measures_count} measures
              </>
            )}
            {result.has_report_layout && " \u00B7 report layout included"}
            {result.query_profile_summary &&
              " \u00B7 query profile analyzed"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            onClick={handleExportPdf}
            disabled={exporting}
          >
            {exporting ? "Generating PDF…" : "Export PDF"}
          </Button>
          <Button variant="outline" onClick={onReset}>
            Analyze Another
          </Button>
        </div>
      </div>

      {/* Parse warnings */}
      {result.parse_warnings && result.parse_warnings.length > 0 && (
        <div className="rounded-lg border border-amber-300/50 bg-amber-50 dark:bg-amber-950/20 p-4 space-y-1">
          <p className="text-sm font-medium text-amber-800 dark:text-amber-300">
            Parser Notes
          </p>
          {result.parse_warnings.map((w, i) => (
            <p key={i} className="text-sm text-amber-700 dark:text-amber-400">
              {w}
            </p>
          ))}
        </div>
      )}

      {/* Tabs */}
      {(hasPbi || hasDbsql || hasMetrics || hasPipeline) && (
        <div className="border-b">
          <nav className="flex gap-1" aria-label="Results tabs">
            {hasPbi && (
              <button
                type="button"
                className={`relative px-5 py-3 text-sm font-medium transition-colors ${
                  activeTab === "pbi"
                    ? "text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => setActiveTab("pbi")}
              >
                <span className="flex items-center gap-2">
                  Power BI Model
                  {pbiIssueCount > 0 && (
                    <span className="inline-flex items-center justify-center h-5 min-w-5 px-1.5 rounded-full text-[10px] font-bold bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300">
                      {pbiIssueCount}
                    </span>
                  )}
                </span>
                {activeTab === "pbi" && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary rounded-t" />
                )}
              </button>
            )}
            {hasDbsql && (
              <button
                type="button"
                className={`relative px-5 py-3 text-sm font-medium transition-colors ${
                  activeTab === "dbsql"
                    ? "text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => setActiveTab("dbsql")}
              >
                <span className="flex items-center gap-2">
                  Databricks SQL
                  {dbsqlIssueCount > 0 && (
                    <span className="inline-flex items-center justify-center h-5 min-w-5 px-1.5 rounded-full text-[10px] font-bold bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300">
                      {dbsqlIssueCount}
                    </span>
                  )}
                </span>
                {activeTab === "dbsql" && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary rounded-t" />
                )}
              </button>
            )}
            {(hasMetrics || metricsLoading) && (
              <button
                type="button"
                className={`relative px-5 py-3 text-sm font-medium transition-colors ${
                  activeTab === "metrics"
                    ? "text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => setActiveTab("metrics")}
              >
                <span className="flex items-center gap-2">
                  UC Metrics Migration
                  {hasMetrics && (
                    <span className="inline-flex items-center justify-center h-5 min-w-5 px-1.5 rounded-full text-[10px] font-bold bg-purple-100 text-purple-800 dark:bg-purple-900/40 dark:text-purple-300">
                      {Math.round(metricsAnalysis!.feasibility_score)}%
                    </span>
                  )}
                  {metricsLoading && (
                    <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium text-muted-foreground">
                      ...
                    </span>
                  )}
                </span>
                {activeTab === "metrics" && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary rounded-t" />
                )}
              </button>
            )}
            {(hasPipeline || pipelineLoading) && (
              <button
                type="button"
                className={`relative px-5 py-3 text-sm font-medium transition-colors ${
                  activeTab === "pipeline"
                    ? "text-foreground"
                    : "text-muted-foreground hover:text-foreground"
                }`}
                onClick={() => setActiveTab("pipeline")}
              >
                <span className="flex items-center gap-2">
                  Pipeline Migration
                  {hasPipeline && (
                    <span className="inline-flex items-center justify-center h-5 min-w-5 px-1.5 rounded-full text-[10px] font-bold bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300">
                      {Math.round(pipelineAnalysis!.migration_score)}%
                    </span>
                  )}
                  {pipelineLoading && (
                    <span className="inline-flex items-center px-1.5 py-0.5 rounded-full text-[10px] font-medium text-muted-foreground">
                      ...
                    </span>
                  )}
                </span>
                {activeTab === "pipeline" && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary rounded-t" />
                )}
              </button>
            )}
          </nav>
        </div>
      )}

      {/* Tab content */}
      {activeTab === "pbi" && hasPbi && (
        <TabPanel categories={pbiCategories} allFindings={pbiFindings} />
      )}
      {activeTab === "dbsql" && hasDbsql && (
        <>
          {result.query_profile_summary && (
            <QueryProfileBanner summary={result.query_profile_summary} />
          )}
          <TabPanel categories={dbsqlCategories} allFindings={dbsqlFindings} />
        </>
      )}
      {activeTab === "metrics" && hasMetrics && metricsAnalysis && (
        <MetricsMigration analysis={metricsAnalysis} modelFile={modelFile ?? null} />
      )}
      {activeTab === "metrics" && metricsLoading && (
        <div className="text-center py-12 text-muted-foreground">
          Analyzing measures for UC Metric View migration...
        </div>
      )}
      {activeTab === "pipeline" && hasPipeline && pipelineAnalysis && (
        <PipelineMigration analysis={pipelineAnalysis} modelFile={modelFile ?? null} />
      )}
      {activeTab === "pipeline" && pipelineLoading && (
        <div className="text-center py-12 text-muted-foreground">
          Analyzing M queries for pipeline migration...
        </div>
      )}
    </div>
  );
}
