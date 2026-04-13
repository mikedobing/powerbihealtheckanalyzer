import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";
export class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;
    constructor(status: number, statusText: string, body: unknown){
        super(`HTTP ${status}: ${statusText}`);
        this.name = "ApiError";
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
export interface AIStepTranslation {
    confidence?: string;
    notes?: string;
    sql?: string | null;
    sql_type?: string;
    step_name?: string;
}
export interface AnalysisResponse-Input {
    has_report_layout?: boolean;
    measures_count?: number;
    model_name?: string;
    parse_warnings?: string[];
    query_profile_summary?: QueryProfileSummary | null;
    relationships_count?: number;
    report: HealthReport-Input;
    tables_count?: number;
}
export interface AnalysisResponse-Output {
    has_report_layout?: boolean;
    measures_count?: number;
    model_name?: string;
    parse_warnings?: string[];
    query_profile_summary?: QueryProfileSummary | null;
    relationships_count?: number;
    report: HealthReport-Output;
    tables_count?: number;
}
export interface Body_analyzeFiles {
    llm_endpoint?: string;
    model_file: string;
    query_file?: string | null;
}
export interface Body_analyzeLive {
    days?: number;
    model_file: string;
    warehouse_id: string;
}
export interface Body_analyzeMQueries {
    llm_endpoint?: string;
    model_file: string;
    skip_ai?: boolean;
}
export interface Body_analyzeMetrics {
    model_file: string;
}
export interface Body_analyzeProfile {
    llm_endpoint?: string;
    query_file: string;
}
export interface Body_generateMetricView {
    catalog?: string;
    model_file: string;
    schema_name?: string;
    view_name?: string;
}
export interface Body_generatePipelineBundle {
    bundle_name?: string;
    catalog?: string;
    include_metrics?: boolean;
    model_file: string;
    schema_name?: string;
}
export interface CategoryScore-Input {
    assessed?: boolean;
    category: string;
    display_name: string;
    findings?: Finding[];
    max_score?: number;
    score: number;
}
export interface CategoryScore-Output {
    assessed?: boolean;
    category: string;
    display_name: string;
    findings?: Finding[];
    max_score?: number;
    score: number;
}
export interface ExportQueryResponse {
    description: string;
    sql: string;
}
export interface Finding {
    category: string;
    description: string;
    details?: Record<string, unknown>;
    impact?: Impact;
    name: string;
    recommendation: string;
    reference_url?: string;
    rule_id: string;
    severity: Severity;
}
export interface HTTPValidationError {
    detail?: ValidationError[];
}
export interface HealthReport-Input {
    categories?: CategoryScore-Input[];
    error_count?: number;
    info_count?: number;
    mode?: string;
    overall_score?: number;
    total_findings?: number;
    warning_count?: number;
}
export interface HealthReport-Output {
    categories?: CategoryScore-Output[];
    error_count?: number;
    info_count?: number;
    mode?: string;
    overall_score?: number;
    total_findings?: number;
    warning_count?: number;
}
export const Impact = {
    high: "high",
    medium: "medium",
    low: "low"
} as const;
export type Impact = typeof Impact[keyof typeof Impact];
export interface MQueryAnalysisResponse {
    ai_enhanced?: boolean;
    auto_count?: number;
    databricks_source_count?: number;
    manual_count?: number;
    migration_score?: number;
    non_databricks_source_count?: number;
    partial_count?: number;
    tables?: TableMigrationOut[];
    tables_with_m?: number;
    total_tables?: number;
    unique_sources?: SourceInfo[];
    warnings?: string[];
}
export interface MeasureClassificationOut {
    dax_expression: string;
    measure_name: string;
    notes?: string;
    pattern_matched?: string;
    sql_expression?: string | null;
    table_name: string;
    tier: "direct" | "translatable" | "manual";
}
export interface MetricViewGenerateResponse {
    sql_statement?: string;
    warnings?: string[];
    yaml_content?: string;
}
export interface MetricsAnalysisResponse {
    classifications?: MeasureClassificationOut[];
    direct_count?: number;
    feasibility_score?: number;
    manual_count?: number;
    proposed_dimensions?: ProposedDimensionOut[];
    proposed_joins?: ProposedJoinOut[];
    source_table?: string;
    translatable_count?: number;
    warnings?: string[];
}
export interface ProposedDimensionOut {
    comment?: string;
    expr: string;
    name: string;
    source_table: string;
}
export interface ProposedJoinOut {
    name: string;
    on: string;
    source: string;
}
export interface QueryProfileSummary {
    has_llm_analysis?: boolean;
    is_pbi_generated?: boolean;
    join_types?: string[];
    query_id?: string;
    read_bytes?: number;
    rows_produced?: number;
    rows_read?: number;
    status?: string;
    tables_scanned?: string[];
    total_time_ms?: number;
}
export const Severity = {
    error: "error",
    warning: "warning",
    info: "info"
} as const;
export type Severity = typeof Severity[keyof typeof Severity];
export interface SourceInfo {
    fqn: string;
    is_databricks?: boolean;
    type: string;
}
export interface TableMigrationOut {
    ai_changes?: string[];
    ai_confidence?: string;
    ai_enhanced?: boolean;
    ai_sql?: string;
    ai_step_translations?: AIStepTranslation[];
    auto_steps?: number;
    generated_sql?: string;
    has_m_query?: boolean;
    is_calculated_table?: boolean;
    is_databricks_source?: boolean;
    manual_step_names?: string[];
    manual_steps?: number;
    original_m?: string;
    partial_steps?: number;
    source_fqn?: string;
    source_type?: string;
    step_count?: number;
    suggested_layer?: string;
    table_name: string;
    tier: "auto" | "partial" | "manual";
    warnings?: string[];
}
export interface ValidationError {
    ctx?: Record<string, unknown>;
    input?: unknown;
    loc: (string | number)[];
    msg: string;
    type: string;
}
export interface VersionOut {
    version: string;
}
export const analyzeFiles = async (data: FormData, options?: RequestInit): Promise<{
    data: AnalysisResponse-Output;
}> =>{
    const res = await fetch("/api/analyze", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useAnalyzeFiles(options?: {
    mutation?: UseMutationOptions<{
        data: AnalysisResponse-Output;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>analyzeFiles(data),
        ...options?.mutation
    });
}
export const analyzeLive = async (data: FormData, options?: RequestInit): Promise<{
    data: AnalysisResponse-Output;
}> =>{
    const res = await fetch("/api/analyze-live", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useAnalyzeLive(options?: {
    mutation?: UseMutationOptions<{
        data: AnalysisResponse-Output;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>analyzeLive(data),
        ...options?.mutation
    });
}
export const analyzeMQueries = async (data: FormData, options?: RequestInit): Promise<{
    data: MQueryAnalysisResponse;
}> =>{
    const res = await fetch("/api/analyze-m-queries", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useAnalyzeMQueries(options?: {
    mutation?: UseMutationOptions<{
        data: MQueryAnalysisResponse;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>analyzeMQueries(data),
        ...options?.mutation
    });
}
export const analyzeMetrics = async (data: FormData, options?: RequestInit): Promise<{
    data: MetricsAnalysisResponse;
}> =>{
    const res = await fetch("/api/analyze-metrics", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useAnalyzeMetrics(options?: {
    mutation?: UseMutationOptions<{
        data: MetricsAnalysisResponse;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>analyzeMetrics(data),
        ...options?.mutation
    });
}
export const analyzeProfile = async (data: FormData, options?: RequestInit): Promise<{
    data: AnalysisResponse-Output;
}> =>{
    const res = await fetch("/api/analyze-profile", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useAnalyzeProfile(options?: {
    mutation?: UseMutationOptions<{
        data: AnalysisResponse-Output;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>analyzeProfile(data),
        ...options?.mutation
    });
}
export const exportPdf = async (data: AnalysisResponse-Input, options?: RequestInit): Promise<{
    data: unknown;
}> =>{
    const res = await fetch("/api/export-pdf", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useExportPdf(options?: {
    mutation?: UseMutationOptions<{
        data: unknown;
    }, ApiError, AnalysisResponse-Input>;
}) {
    return useMutation({
        mutationFn: (data)=>exportPdf(data),
        ...options?.mutation
    });
}
export interface GetExportQueryParams {
    warehouse_id?: string;
    days?: number;
}
export const getExportQuery = async (params?: GetExportQueryParams, options?: RequestInit): Promise<{
    data: ExportQueryResponse;
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.warehouse_id != null) searchParams.set("warehouse_id", String(params?.warehouse_id));
    if (params?.days != null) searchParams.set("days", String(params?.days));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/export-query?${queryString}` : "/api/export-query";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getExportQueryKey = (params?: GetExportQueryParams)=>{
    return [
        "/api/export-query",
        params
    ] as const;
};
export function useGetExportQuery<TData = {
    data: ExportQueryResponse;
}>(options?: {
    params?: GetExportQueryParams;
    query?: Omit<UseQueryOptions<{
        data: ExportQueryResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getExportQueryKey(options?.params),
        queryFn: ()=>getExportQuery(options?.params),
        ...options?.query
    });
}
export function useGetExportQuerySuspense<TData = {
    data: ExportQueryResponse;
}>(options?: {
    params?: GetExportQueryParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ExportQueryResponse;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getExportQueryKey(options?.params),
        queryFn: ()=>getExportQuery(options?.params),
        ...options?.query
    });
}
export const generateMetricView = async (data: FormData, options?: RequestInit): Promise<{
    data: MetricViewGenerateResponse;
}> =>{
    const res = await fetch("/api/generate-metric-view", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGenerateMetricView(options?: {
    mutation?: UseMutationOptions<{
        data: MetricViewGenerateResponse;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>generateMetricView(data),
        ...options?.mutation
    });
}
export const generatePipelineBundle = async (data: FormData, options?: RequestInit): Promise<{
    data: unknown;
}> =>{
    const res = await fetch("/api/generate-pipeline-bundle", {
        ...options,
        method: "POST",
        headers: {
            ...options?.headers
        },
        body: data
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGeneratePipelineBundle(options?: {
    mutation?: UseMutationOptions<{
        data: unknown;
    }, ApiError, FormData>;
}) {
    return useMutation({
        mutationFn: (data)=>generatePipelineBundle(data),
        ...options?.mutation
    });
}
export const version = async (options?: RequestInit): Promise<{
    data: VersionOut;
}> =>{
    const res = await fetch("/api/version", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const versionKey = ()=>{
    return [
        "/api/version"
    ] as const;
};
export function useVersion<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
export function useVersionSuspense<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
