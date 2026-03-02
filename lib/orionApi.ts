// Typed API client for the new portfolio/risk-scoring contract.
// All calls go through the Next.js proxy at /api/orion which adds Bearer auth.
// DO NOT call endpoints that aren't in the contract.

import type {
  Portfolio,
  RiskSummary,
  BatchScoreRequest,
  BatchScoreResponse,
  ExportRequest,
  ExportResponse,
  Notification,
} from "@/types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "/api/orion";

// ── Low-level helpers ────────────────────────────────────────────────────────

async function apiGet<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = params
    ? `${API_BASE}${path}?${new URLSearchParams(params)}`
    : `${API_BASE}${path}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    const err = new Error(`API ${res.status}: ${text}`) as Error & { status: number };
    err.status = res.status;
    throw err;
  }
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`API ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

// ── API surface ──────────────────────────────────────────────────────────────

export const portfoliosApi = {
  list: () => apiGet<Portfolio[]>("/portfolios"),

  riskSummary: (portfolioId: string, start: string, end: string) =>
    apiGet<RiskSummary>(`/portfolios/${portfolioId}/risk-summary`, { start, end }),
};

export const scoresApi = {
  batch: (req: BatchScoreRequest) =>
    apiPost<BatchScoreResponse>("/scores/batch", req),
};

export const exportApi = {
  portfolio: (req: ExportRequest) =>
    apiPost<ExportResponse>("/export/portfolio", req),
};

export const notificationsApi = {
  list: (portfolioId?: string) =>
    apiGet<Notification[]>(
      "/notifications",
      portfolioId ? { portfolio_id: portfolioId } : undefined
    ),

  ack: (id: string) =>
    apiPost<{ success: boolean }>(`/notifications/${id}/ack`, {}),
};
