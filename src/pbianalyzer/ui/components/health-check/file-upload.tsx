import { useState, useRef, useCallback } from "react";
import { Button } from "@/components/ui/button";

interface FileUploadZoneProps {
  onAnalyze: (
    modelFile: File | null,
    queryFile: File | null,
    llmEndpoint: string,
  ) => void;
  loading: boolean;
}

export function FileUploadZone({ onAnalyze, loading }: FileUploadZoneProps) {
  const [modelFile, setModelFile] = useState<File | null>(null);
  const [queryFile, setQueryFile] = useState<File | null>(null);
  const [llmEndpoint, setLlmEndpoint] = useState(
    () => localStorage.getItem("pbi_llm_endpoint") || "",
  );
  const [showLlmConfig, setShowLlmConfig] = useState(false);
  const [aiEnabled, setAiEnabled] = useState(!!llmEndpoint);
  const [dragOver, setDragOver] = useState(false);
  const modelInputRef = useRef<HTMLInputElement>(null);
  const queryInputRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      const files = Array.from(e.dataTransfer.files);
      for (const file of files) {
        const name = file.name.toLowerCase();
        if (
          name.endsWith(".bim") ||
          name.endsWith(".pbix") ||
          name.endsWith(".pbit") ||
          name.endsWith(".zip")
        ) {
          setModelFile(file);
        } else if (name.endsWith(".json")) {
          setQueryFile(file);
        }
      }
    },
    [],
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setDragOver(false);
  }, []);

  const canAnalyze = modelFile || queryFile;

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      {/* Model file upload */}
      <div
        className={`relative border-2 border-dashed rounded-xl p-10 text-center transition-colors cursor-pointer ${
          dragOver
            ? "border-primary bg-primary/5"
            : modelFile
              ? "border-green-500/50 bg-green-500/5"
              : "border-border hover:border-primary/50"
        }`}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => modelInputRef.current?.click()}
      >
        <input
          ref={modelInputRef}
          type="file"
          className="hidden"
          accept=".bim,.pbix,.pbit,.zip"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) setModelFile(file);
          }}
        />
        <div className="space-y-3">
          <div className="text-4xl">
            {modelFile ? "\u2705" : "\uD83D\uDCC4"}
          </div>
          {modelFile ? (
            <div>
              <p className="font-medium text-green-600 dark:text-green-400">
                {modelFile.name}
              </p>
              <p className="text-sm text-muted-foreground mt-1">
                {(modelFile.size / 1024).toFixed(1)} KB
              </p>
            </div>
          ) : (
            <div>
              <p className="font-medium">
                Drop your .bim, .pbix, .pbit, or .pbip (ZIP) file here
              </p>
              <p className="text-sm text-muted-foreground mt-1">
                or click to browse
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Query file (history or profile) */}
      <div
        className={`relative border rounded-xl p-6 text-center transition-colors cursor-pointer ${
          queryFile
            ? "border-blue-500/50 bg-blue-500/5"
            : "border-border hover:border-primary/30"
        }`}
        onClick={() => queryInputRef.current?.click()}
      >
        <input
          ref={queryInputRef}
          type="file"
          className="hidden"
          accept=".json"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) setQueryFile(file);
          }}
        />
        <div className="space-y-2">
          {queryFile ? (
            <div>
              <p className="text-sm font-medium text-blue-600 dark:text-blue-400">
                Query data: {queryFile.name}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {(queryFile.size / 1024).toFixed(1)} KB &middot; Auto-detects
                query history or query profile format
              </p>
            </div>
          ) : (
            <div>
              <p className="text-sm text-muted-foreground">
                <span className="font-medium text-foreground">Optional:</span>{" "}
                Add DBSQL query history JSON <em>or</em> a Query Profile export
                for SQL performance analysis
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                Tip: Export a Query Profile from the DBSQL Query History UI for
                deep plan analysis
              </p>
            </div>
          )}
        </div>
      </div>

      {/* LLM Endpoint config */}
      <div
        className={`rounded-lg border p-4 space-y-3 transition-colors ${
          aiEnabled && llmEndpoint
            ? "border-violet-500/50 bg-violet-500/5"
            : "bg-card/50"
        }`}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button
              type="button"
              role="switch"
              aria-checked={aiEnabled}
              className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors ${
                aiEnabled ? "bg-violet-600" : "bg-muted"
              }`}
              onClick={() => {
                const next = !aiEnabled;
                setAiEnabled(next);
                if (next && !llmEndpoint) {
                  setLlmEndpoint("databricks-claude-opus-4-6");
                  localStorage.setItem(
                    "pbi_llm_endpoint",
                    "databricks-claude-opus-4-6",
                  );
                  setShowLlmConfig(true);
                }
              }}
            >
              <span
                className={`pointer-events-none inline-block h-5 w-5 rounded-full bg-white shadow transform transition-transform ${
                  aiEnabled ? "translate-x-5" : "translate-x-0"
                }`}
              />
            </button>
            <div>
              <span className="text-sm font-medium">AI-Powered Analysis</span>
              {aiEnabled && llmEndpoint && (
                <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-violet-100 text-violet-800 dark:bg-violet-900/30 dark:text-violet-400 border border-violet-200 dark:border-violet-800">
                  {llmEndpoint}
                </span>
              )}
            </div>
          </div>
          {aiEnabled && (
            <button
              type="button"
              className="text-xs text-muted-foreground hover:text-foreground"
              onClick={() => setShowLlmConfig(!showLlmConfig)}
            >
              {showLlmConfig ? "Hide" : "Configure"}
            </button>
          )}
        </div>

        {aiEnabled && showLlmConfig && (
          <div className="space-y-2">
            <p className="text-xs text-muted-foreground">
              Databricks serving endpoint for LLM-powered analysis. Uses
              Foundation Model API or a custom endpoint.
            </p>
            <input
              type="text"
              className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-violet-500/50"
              placeholder="databricks-claude-opus-4-6"
              value={llmEndpoint}
              onChange={(e) => {
                setLlmEndpoint(e.target.value);
                localStorage.setItem("pbi_llm_endpoint", e.target.value);
              }}
              onClick={(e) => e.stopPropagation()}
            />
          </div>
        )}
      </div>

      {/* Analyze button */}
      <div className="flex justify-center">
        <Button
          size="lg"
          className="px-8"
          disabled={!canAnalyze || loading}
          onClick={() =>
            onAnalyze(
              modelFile,
              queryFile,
              aiEnabled ? llmEndpoint : "",
            )
          }
        >
          {loading ? (
            <span className="flex items-center gap-2">
              <span className="h-4 w-4 border-2 border-current border-t-transparent rounded-full animate-spin" />
              Analyzing{llmEndpoint ? " (incl. AI)" : ""}...
            </span>
          ) : (
            "Run Health Check"
          )}
        </Button>
      </div>

      {/* Info box */}
      <div className="rounded-lg border bg-card p-4 text-sm text-muted-foreground space-y-2">
        <p className="font-medium text-foreground">How it works</p>
        <ul className="space-y-1 list-disc list-inside">
          <li>
            Upload a <code>.bim</code> file (Tabular Model) for the most
            complete analysis, a <code>.pbix</code> /{" "}
            <code>.pbit</code> file (Power BI Desktop), or a{" "}
            <code>.pbip</code> project as a ZIP.{" "}
            <span className="text-muted-foreground">
              Tip: export .bim from{" "}
              <a
                href="https://www.sqlbi.com/tools/tabular-editor/"
                target="_blank"
                rel="noopener noreferrer"
                className="underline hover:text-primary"
              >
                Tabular Editor
              </a>{" "}
              (free) for the richest metadata.
            </span>
          </li>
          <li>
            Optionally add a <strong>Query Profile JSON</strong> (exported from
            DBSQL Query History) for deep execution plan analysis, or query
            history JSON for aggregate performance checks
          </li>
          <li>
            Enable <strong>AI-Powered Analysis</strong> with a Databricks
            serving endpoint to get LLM-driven insights on SQL and plan
            structure
          </li>
          <li>
            The analyzer checks 40+ rules across 7 categories derived from{" "}
            <a
              href="https://github.com/databricks-solutions/power-bi-on-databricks-quickstarts"
              target="_blank"
              rel="noopener noreferrer"
              className="underline text-primary hover:text-primary/80"
            >
              Databricks best practices
            </a>
          </li>
          <li>
            Get a scored health report with prioritized, actionable
            recommendations
          </li>
        </ul>
      </div>
    </div>
  );
}
