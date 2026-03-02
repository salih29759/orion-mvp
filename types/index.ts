// API Contract Types — strictly follows backend OpenAPI contract
// DO NOT add fields that aren't in the contract.

export type PerilKey = "heat" | "rain" | "wind" | "drought" | "wildfire";
export type AllPerilKey = "all" | PerilKey;
export type BandKey = "minimal" | "minor" | "moderate" | "major" | "extreme";
export type DateRangeLabel = "30d" | "90d" | "365d";

// ── GET /portfolios ──────────────────────────────────────────────────────────
export interface Portfolio {
  portfolio_id: string;
  name: string;
}

// ── GET /portfolios/{id}/risk-summary ────────────────────────────────────────
export interface RiskSummaryTopAsset {
  asset_id: string;
  name: string;
  lat: number;
  lon: number;
  band: BandKey;
  scores: {
    all: number;
    heat: number;
    rain: number;
    wind: number;
    drought: number;
    wildfire?: number | null;
  };
}

export interface RiskSummaryTrendPoint {
  date: string;
  all: number;
  heat: number;
  rain: number;
  wind: number;
  drought: number;
  wildfire?: number | null;
}

export interface RiskSummary {
  portfolio_id: string;
  period: { start: string; end: string };
  bands: Record<BandKey, number>;
  peril_averages: {
    all: number | null;
    heat: number | null;
    rain: number | null;
    wind: number | null;
    drought: number | null;
    wildfire: number | null;
  };
  top_assets: RiskSummaryTopAsset[];
  trend: RiskSummaryTrendPoint[];
}

// ── POST /scores/batch ───────────────────────────────────────────────────────
export interface BatchAssetInput {
  asset_id: string;
  lat: number;
  lon: number;
  name: string;
}

export interface BatchScoreRequest {
  assets: BatchAssetInput[];
  start_date: string;
  end_date: string;
  climatology_version: string;
  include_perils: PerilKey[];
}

export interface ScorePoint {
  date: string;
  scores: Partial<Record<AllPerilKey, number | null>>;
  bands: Partial<Record<AllPerilKey, BandKey | null>>;
  drivers: Partial<Record<PerilKey, string[]>>;
}

export interface BatchScoreResponse {
  run_id: string;
  results: Array<{
    asset_id: string;
    series: ScorePoint[];
  }>;
}

// ── POST /export/portfolio ───────────────────────────────────────────────────
export interface ExportRequest {
  portfolio_id: string;
  start_date: string;
  end_date: string;
  format: "csv";
  include_drivers: boolean;
}

export interface ExportResponse {
  export_id: string;
  status: "queued" | "processing" | "done" | "error";
  path: string;
  download_url: string | null;
}

// ── GET /notifications ───────────────────────────────────────────────────────
export interface Notification {
  id: string;
  severity: "high" | "medium";
  type: string;
  asset_id: string;
  created_at: string;
  payload: Record<string, unknown>;
  acknowledged?: boolean;
}
