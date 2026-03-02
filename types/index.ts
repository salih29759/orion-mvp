// API Contract Types — generated from backend/openapi.yaml
// Single source of truth: backend/openapi.yaml
// DO NOT add fields not present in that file.

export type PerilKey = "heat" | "rain" | "wind" | "drought" | "wildfire";
export type AllPerilKey = "all" | PerilKey;
export type BandKey = "minimal" | "minor" | "moderate" | "major" | "extreme";
export type DateRangeLabel = "30d" | "90d" | "365d";

// ── Shared primitives (openapi.yaml component schemas) ────────────────────────

/** ScoresObject: open map of peril → 0-100 float */
export type ScoresObject = Partial<Record<AllPerilKey, number | null>>;

/** BandsObject: open map of peril → band label */
export type BandsObject = Partial<Record<AllPerilKey, BandKey | null>>;

/** DriversObject: open map of peril → string[] */
export type DriversObject = Partial<Record<PerilKey, string[]>>;

export interface BandCounts {
  minimal?: number;
  minor?: number;
  moderate?: number;
  major?: number;
  extreme?: number;
}

// ── GET /portfolios ──────────────────────────────────────────────────────────

export interface Portfolio {
  portfolio_id: string;
  name: string;
}

// ── GET /portfolios/{portfolio_id}/risk-summary ───────────────────────────────

export interface RiskTrendPoint {
  date: string;
  scores: ScoresObject;
}

export interface TopAsset {
  asset_id: string;
  name: string;
  lat: number;
  lon: number;
  band: BandKey;
  scores: ScoresObject;
}

export interface RiskSummaryResponse {
  portfolio_id: string;
  period: { start: string; end: string };
  bands: BandCounts;
  peril_averages: ScoresObject;
  top_assets: TopAsset[];
  trend: RiskTrendPoint[];
}

// ── POST /scores/batch ───────────────────────────────────────────────────────

export interface BatchAssetInput {
  asset_id: string;
  lat: number;
  lon: number;
  name?: string;
}

export interface BatchScoresRequest {
  assets: BatchAssetInput[];
  start_date: string;
  end_date: string;
  climatology_version: string;
  include_perils: PerilKey[];
}

export interface ScoreSeriesPoint {
  date: string;
  scores: ScoresObject;
  bands: BandsObject;
  drivers?: DriversObject;
}

export interface BatchScoresResponse {
  run_id: string;
  climatology_version: string;
  results: Array<{
    asset_id: string;
    series: ScoreSeriesPoint[];
  }>;
}

// ── POST /export/portfolio ───────────────────────────────────────────────────

export interface ExportPortfolioRequest {
  portfolio_id: string;
  start_date: string;
  end_date: string;
  format: "csv";
  include_drivers?: boolean;
}

export interface ExportPortfolioResponse {
  export_id: string;
  status: "queued" | "running" | "success" | "failed";
  path: string;
  download_url: string | null;
}

// ── GET /notifications ───────────────────────────────────────────────────────

export interface Notification {
  id: string;
  severity: "low" | "medium" | "high";
  type: string;
  portfolio_id?: string | null;
  asset_id: string;
  created_at: string;
  acknowledged_at: string | null;
  payload: Record<string, unknown>;
}

// ── POST /notifications/{notification_id}/ack ────────────────────────────────

export interface AckNotificationResponse {
  id: string;
  acknowledged_at: string;
}
