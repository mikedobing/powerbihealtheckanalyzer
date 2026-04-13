import { createFileRoute } from "@tanstack/react-router";
import Navbar from "@/components/apx/navbar";
import { useState, useCallback } from "react";
import { FileUploadZone } from "@/components/health-check/file-upload";
import { HealthDashboard } from "@/components/health-check/health-dashboard";
import type { AnalysisResult } from "@/components/health-check/types";

export const Route = createFileRoute("/")({
  component: () => <Index />,
});

function Index() {
  const [result, setResult] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastModelFile, setLastModelFile] = useState<File | null>(null);

  const handleAnalyze = useCallback(
    async (
      modelFile: File | null,
      queryFile: File | null,
      llmEndpoint: string,
    ) => {
      setLoading(true);
      setError(null);
      setResult(null);
      setLastModelFile(modelFile);

      try {
        const formData = new FormData();

        let endpoint: string;

        if (modelFile) {
          formData.append("model_file", modelFile);
          if (queryFile) {
            formData.append("query_file", queryFile);
          }
          if (llmEndpoint) {
            formData.append("llm_endpoint", llmEndpoint);
          }
          endpoint = "/api/analyze";
        } else if (queryFile) {
          formData.append("query_file", queryFile);
          if (llmEndpoint) {
            formData.append("llm_endpoint", llmEndpoint);
          }
          endpoint = "/api/analyze-profile";
        } else {
          throw new Error("Please upload at least one file.");
        }

        const response = await fetch(endpoint, {
          method: "POST",
          body: formData,
        });

        if (!response.ok) {
          const errBody = await response.text();
          throw new Error(`Analysis failed: ${errBody}`);
        }

        const data: AnalysisResult = await response.json();
        setResult(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const handleReset = useCallback(() => {
    setResult(null);
    setError(null);
    setLastModelFile(null);
  }, []);

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Navbar />

      <main className="flex-1 container mx-auto max-w-7xl px-4 py-8">
        {!result ? (
          <div className="space-y-8">
            <div className="text-center space-y-3">
              <h1 className="text-4xl font-bold tracking-tight">
                Power BI Health Check
              </h1>
              <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
                Upload your .bim or .pbix file to analyze your Power BI model
                against Databricks SQL best practices. Add a Query Profile JSON
                for deep execution plan analysis.
              </p>
            </div>

            <FileUploadZone onAnalyze={handleAnalyze} loading={loading} />

            {error && (
              <div className="mx-auto max-w-xl p-4 rounded-lg border border-destructive/50 bg-destructive/10 text-destructive text-sm">
                {error}
              </div>
            )}
          </div>
        ) : (
          <HealthDashboard result={result} onReset={handleReset} modelFile={lastModelFile} />
        )}
      </main>
    </div>
  );
}
