export interface Finding {
  rule_id: string;
  category: string;
  name: string;
  severity: "error" | "warning" | "info";
  description: string;
  recommendation: string;
  impact: "high" | "medium" | "low";
  reference_url: string;
  details: Record<string, unknown>;
}

export interface CategoryScore {
  category: string;
  display_name: string;
  score: number;
  max_score: number;
  findings: Finding[];
  assessed: boolean;
}

export interface HealthReport {
  overall_score: number;
  categories: CategoryScore[];
  total_findings: number;
  error_count: number;
  warning_count: number;
  info_count: number;
  mode: string;
}

export interface QueryProfileSummary {
  query_id: string;
  status: string;
  total_time_ms: number;
  rows_read: number;
  rows_produced: number;
  read_bytes: number;
  tables_scanned: string[];
  join_types: string[];
  is_pbi_generated: boolean;
  has_llm_analysis: boolean;
}

export interface AnalysisResult {
  report: HealthReport;
  model_name: string;
  tables_count: number;
  relationships_count: number;
  measures_count: number;
  has_report_layout: boolean;
  parse_warnings: string[];
  query_profile_summary: QueryProfileSummary | null;
}
