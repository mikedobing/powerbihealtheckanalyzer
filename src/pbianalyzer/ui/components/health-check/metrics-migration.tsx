import { useState, useCallback } from "react";
import { Button } from "@/components/ui/button";

interface MeasureClassification {
  measure_name: string;
  table_name: string;
  dax_expression: string;
  tier: "direct" | "translatable" | "manual";
  sql_expression: string | null;
  notes: string;
  pattern_matched: string;
}

interface ProposedDimension {
  name: string;
  expr: string;
  source_table: string;
  comment: string;
}

interface ProposedJoin {
  name: string;
  source: string;
  on: string;
}

export interface MetricsAnalysis {
  source_table: string;
  classifications: MeasureClassification[];
  proposed_dimensions: ProposedDimension[];
  proposed_joins: ProposedJoin[];
  direct_count: number;
  translatable_count: number;
  manual_count: number;
  feasibility_score: number;
  warnings: string[];
}

interface MetricViewOutput {
  yaml_content: string;
  sql_statement: string;
  warnings: string[];
}

function TierBadge({ tier }: { tier: string }) {
  const styles: Record<string, string> = {
    direct:
      "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 border-green-200 dark:border-green-800",
    translatable:
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

function FeasibilityRing({
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
    score >= 70
      ? "text-green-500"
      : score >= 40
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
          strokeWidth="5"
          className="text-muted/30"
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth="5"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          className={`${color} transition-all duration-1000`}
        />
      </svg>
      <div className="absolute flex flex-col items-center">
        <span className={`text-2xl font-bold ${color}`}>
          {Math.round(score)}%
        </span>
        <span className="text-[10px] text-muted-foreground">convertible</span>
      </div>
    </div>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        });
      }}
      className="absolute top-2 right-2 px-2 py-1 rounded text-[10px] font-medium bg-muted-foreground/10 hover:bg-muted-foreground/20 text-muted-foreground transition-colors"
    >
      {copied ? "Copied!" : "Copy"}
    </button>
  );
}

interface MetricsMigrationProps {
  analysis: MetricsAnalysis;
  modelFile: File | null;
}

export function MetricsMigration({
  analysis,
  modelFile,
}: MetricsMigrationProps) {
  const [catalog, setCatalog] = useState("my_catalog");
  const [schemaName, setSchemaName] = useState("my_schema");
  const [viewName, setViewName] = useState("");
  const [generating, setGenerating] = useState(false);
  const [generated, setGenerated] = useState<MetricViewOutput | null>(null);
  const [showConverter, setShowConverter] = useState(false);
  const [expandedRow, setExpandedRow] = useState<number | null>(null);

  const handleGenerate = useCallback(async () => {
    if (!modelFile) return;
    setGenerating(true);
    try {
      const fd = new FormData();
      fd.append("model_file", modelFile);
      fd.append("catalog", catalog);
      fd.append("schema_name", schemaName);
      if (viewName) fd.append("view_name", viewName);

      const res = await fetch("/api/generate-metric-view", {
        method: "POST",
        body: fd,
      });
      if (!res.ok) throw new Error(`Generation failed: ${res.status}`);
      const data: MetricViewOutput = await res.json();
      setGenerated(data);
    } catch (err) {
      console.error("Metric view generation failed:", err);
    } finally {
      setGenerating(false);
    }
  }, [modelFile, catalog, schemaName, viewName]);

  const { classifications } = analysis;

  return (
    <div className="space-y-6">
      {/* Header row: score ring + summary + Convert button */}
      <div className="grid md:grid-cols-[auto_1fr_auto] gap-6 items-start">
        <div className="flex flex-col items-center gap-3">
          <FeasibilityRing score={analysis.feasibility_score} />
          <div className="text-center text-xs text-muted-foreground">
            <span className="text-green-600 dark:text-green-400 font-medium">
              {analysis.direct_count} direct
            </span>{" "}
            +{" "}
            <span className="text-amber-600 dark:text-amber-400 font-medium">
              {analysis.translatable_count} translatable
            </span>{" "}
            +{" "}
            <span className="text-red-600 dark:text-red-400 font-medium">
              {analysis.manual_count} manual
            </span>
          </div>
        </div>

        <div className="space-y-3">
          <div className="p-3 rounded-lg border bg-card/50 space-y-1">
            <span className="text-xs text-muted-foreground">
              Identified Source Table (Fact)
            </span>
            <p className="font-medium text-sm">{analysis.source_table}</p>
          </div>
          {analysis.proposed_joins.length > 0 && (
            <div className="p-3 rounded-lg border bg-card/50 space-y-1">
              <span className="text-xs text-muted-foreground">
                Proposed Joins ({analysis.proposed_joins.length})
              </span>
              <div className="flex flex-wrap gap-1.5">
                {analysis.proposed_joins.map((j) => (
                  <span
                    key={j.name}
                    className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400"
                  >
                    {j.name}
                  </span>
                ))}
              </div>
            </div>
          )}
          {analysis.proposed_dimensions.length > 0 && (
            <div className="p-3 rounded-lg border bg-card/50 space-y-1">
              <span className="text-xs text-muted-foreground">
                Proposed Dimensions ({analysis.proposed_dimensions.length})
              </span>
              <div className="flex flex-wrap gap-1.5">
                {analysis.proposed_dimensions.map((d) => (
                  <span
                    key={d.name}
                    className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400"
                  >
                    {d.name}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Convert button -- always visible at top right */}
        <div className="flex flex-col items-end gap-2">
          <Button
            onClick={() => setShowConverter(!showConverter)}
            variant={showConverter ? "default" : "outline"}
            className={showConverter ? "" : "border-purple-400 text-purple-600 hover:bg-purple-50 dark:border-purple-600 dark:text-purple-400 dark:hover:bg-purple-950/30"}
          >
            {showConverter ? "Hide Converter" : "Convert to Metric View"}
          </Button>
          {!showConverter && (
            <p className="text-[11px] text-muted-foreground text-right max-w-[180px]">
              Generate a UC Metric View SQL definition from this model
            </p>
          )}
        </div>
      </div>

      {/* Converter panel -- slides in below the header when open */}
      {showConverter && (
        <div className="border rounded-lg p-4 space-y-4 bg-gradient-to-b from-purple-50/50 to-card/50 dark:from-purple-950/20 dark:to-card/50 border-purple-200/50 dark:border-purple-800/30">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold">
              Generate UC Metric View
            </h3>
            <span className="text-xs text-muted-foreground">
              {analysis.direct_count + analysis.translatable_count} of{" "}
              {classifications.length} measures will be included
            </span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <label className="text-xs text-muted-foreground block mb-1">
                Catalog
              </label>
              <input
                type="text"
                value={catalog}
                onChange={(e) => setCatalog(e.target.value)}
                className="w-full px-3 py-2 rounded-md border bg-background text-sm"
                placeholder="my_catalog"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">
                Schema
              </label>
              <input
                type="text"
                value={schemaName}
                onChange={(e) => setSchemaName(e.target.value)}
                className="w-full px-3 py-2 rounded-md border bg-background text-sm"
                placeholder="my_schema"
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">
                View Name (optional)
              </label>
              <input
                type="text"
                value={viewName}
                onChange={(e) => setViewName(e.target.value)}
                className="w-full px-3 py-2 rounded-md border bg-background text-sm"
                placeholder="auto-generated"
              />
            </div>
          </div>
          <Button
            onClick={handleGenerate}
            disabled={generating || !modelFile}
            size="sm"
          >
            {generating ? "Generating..." : "Generate"}
          </Button>

          {generated && (
            <div className="space-y-4 mt-2">
              {generated.warnings.length > 0 && (
                <div className="rounded-lg border border-amber-300/50 bg-amber-50 dark:bg-amber-950/20 p-3 space-y-1">
                  {generated.warnings.map((w, i) => (
                    <p
                      key={i}
                      className="text-sm text-amber-700 dark:text-amber-400"
                    >
                      {w}
                    </p>
                  ))}
                </div>
              )}
              <div>
                <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                  SQL Statement
                </p>
                <div className="relative">
                  <pre className="p-3 pr-16 rounded-lg bg-zinc-900 dark:bg-zinc-950 text-emerald-400 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-all border border-zinc-700/50">
                    {generated.sql_statement}
                  </pre>
                  <CopyButton text={generated.sql_statement} />
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Warnings */}
      {analysis.warnings.length > 0 && (
        <div className="rounded-lg border border-amber-300/50 bg-amber-50 dark:bg-amber-950/20 p-3 space-y-1">
          {analysis.warnings.map((w, i) => (
            <p
              key={i}
              className="text-sm text-amber-700 dark:text-amber-400"
            >
              {w}
            </p>
          ))}
        </div>
      )}

      {/* Measure classification table */}
      <div>
        <h3 className="text-sm font-semibold mb-3">
          Measure Classifications ({classifications.length})
        </h3>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="text-left px-3 py-2 font-medium">Measure</th>
                <th className="text-left px-3 py-2 font-medium">Table</th>
                <th className="text-center px-3 py-2 font-medium">Tier</th>
                <th className="text-left px-3 py-2 font-medium hidden md:table-cell">
                  SQL
                </th>
                <th className="text-left px-3 py-2 font-medium hidden lg:table-cell">
                  Notes
                </th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {classifications.map((c, i) => (
                <tr
                  key={`${c.table_name}-${c.measure_name}`}
                  className="hover:bg-accent/30 cursor-pointer transition-colors"
                  onClick={() =>
                    setExpandedRow(expandedRow === i ? null : i)
                  }
                >
                  <td className="px-3 py-2 font-medium">
                    {c.measure_name}
                  </td>
                  <td className="px-3 py-2 text-muted-foreground">
                    {c.table_name}
                  </td>
                  <td className="px-3 py-2 text-center">
                    <TierBadge tier={c.tier} />
                  </td>
                  <td className="px-3 py-2 hidden md:table-cell">
                    {c.sql_expression ? (
                      <code className="text-xs px-1.5 py-0.5 rounded bg-muted font-mono">
                        {c.sql_expression}
                      </code>
                    ) : (
                      <span className="text-xs text-muted-foreground italic">
                        --
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground hidden lg:table-cell">
                    {c.notes}
                  </td>
                </tr>
              ))}
              {classifications.length === 0 && (
                <tr>
                  <td
                    colSpan={5}
                    className="px-3 py-8 text-center text-muted-foreground"
                  >
                    No measures found in the model.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {expandedRow !== null && classifications[expandedRow] && (
          <div className="mt-2 p-3 rounded-lg border bg-muted/30 text-xs space-y-1">
            <p>
              <span className="font-medium">DAX:</span>{" "}
              <code className="bg-muted px-1 py-0.5 rounded font-mono whitespace-pre-wrap">
                {classifications[expandedRow].dax_expression}
              </code>
            </p>
            <p>
              <span className="font-medium">Pattern:</span>{" "}
              {classifications[expandedRow].pattern_matched}
            </p>
            <p>
              <span className="font-medium">Notes:</span>{" "}
              {classifications[expandedRow].notes}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
