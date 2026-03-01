const API_BASE = process.env.NEXT_PUBLIC_ORION_API_BASE || "/api/orion";

// ---------------------------------------------------------------------------
// Types — mirror the FastAPI backend (snake_case)
// ---------------------------------------------------------------------------

export type RiskLevel = "HIGH" | "MEDIUM" | "LOW";
export type AlertLevel = "HIGH" | "MEDIUM";
export type RiskType = "FLOOD" | "DROUGHT";
export type Trend = "UP" | "DOWN" | "STABLE";

export interface Province {
  id: string;           // plate number as string ("34", "6", …)
  plate: number;
  name: string;
  region: string;
  lat: number;
  lng: number;
  population: number;
  insured_assets: number;   // total USD (not millions)
  flood_score: number;
  drought_score: number;
  overall_score: number;
  risk_level: RiskLevel;
  trend: Trend;
  trend_pct: number;
}

export interface Alert {
  id: string;
  province_id: string;
  province_name: string;
  level: AlertLevel;
  risk_type: RiskType;
  affected_policies: number;
  estimated_loss_usd: number;
  estimated_loss?: number;   // deprecated alias
  message: string;
  issued_at: string;        // ISO datetime string
}

export interface ApiEnvelope<T> {
  status: string;
  generated_at: string;
  model_version: string;
  confidence_score: number;
  data_source?: string | null;
  as_of_date?: string | null;
  data: T;
}

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

async function apiFetch<T>(path: string, params?: Record<string, string>): Promise<T> {
  const query = params ? `?${new URLSearchParams(params).toString()}` : "";
  const res = await fetch(`${API_BASE}${path}${query}`, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API ${res.status}: ${text}`);
  }
  return res.json();
}

export async function fetchProvinces(opts?: {
  risk_level?: RiskLevel;
  min_score?: number;
  region?: string;
  limit?: number;
}): Promise<ApiEnvelope<Province[]>> {
  const params: Record<string, string> = {};
  if (opts?.risk_level) params.risk_level = opts.risk_level;
  if (opts?.min_score != null) params.min_score = String(opts.min_score);
  if (opts?.region) params.region = opts.region;
  if (opts?.limit != null) params.limit = String(opts.limit);

  return apiFetch<ApiEnvelope<Province[]>>("/v1/risk/provinces", params);
}

export async function fetchAlerts(opts?: { level?: AlertLevel }): Promise<ApiEnvelope<Alert[]>> {
  const params: Record<string, string> = {};
  if (opts?.level) params.level = opts.level;

  return apiFetch<ApiEnvelope<Alert[]>>("/v1/alerts/active", params);
}

// ---------------------------------------------------------------------------
// Colour utilities
// ---------------------------------------------------------------------------

export const getRiskColor = (score: number): string => {
  if (score >= 75) return "#ef4444";
  if (score >= 50) return "#f97316";
  return "#22c55e";
};

export const getRiskTextColor = (level: RiskLevel | AlertLevel): string => {
  switch (level) {
    case "HIGH":   return "#ef4444";
    case "MEDIUM": return "#f97316";
    case "LOW":    return "#22c55e";
  }
};
