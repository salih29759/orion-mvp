// Typed API client — strictly follows backend/openapi.yaml contract.
// All calls go through the Next.js proxy at /api/orion (server adds Bearer auth).
// DO NOT add endpoints not in the contract.

import type {
  Portfolio,
  RiskSummaryResponse,
  BatchScoresRequest,
  BatchScoresResponse,
  ExportPortfolioRequest,
  ExportPortfolioResponse,
  Notification,
  AckNotificationResponse,
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
    const err = new Error(`API ${res.status}: ${text}`) as Error & { status: number };
    err.status = res.status;
    throw err;
  }
  return res.json() as Promise<T>;
}

// ── API surface ──────────────────────────────────────────────────────────────

export const portfoliosApi = {
  /** GET /portfolios */
  list: () => apiGet<Portfolio[]>("/portfolios"),

  /** GET /portfolios/{portfolio_id}/risk-summary?start=...&end=... */
  riskSummary: (portfolioId: string, start: string, end: string) =>
    apiGet<RiskSummaryResponse>(`/portfolios/${portfolioId}/risk-summary`, {
      start,
      end,
    }),
};

export const scoresApi = {
  /** POST /scores/batch */
  batch: (req: BatchScoresRequest) =>
    apiPost<BatchScoresResponse>("/scores/batch", req),
};

export const exportApi = {
  /** POST /export/portfolio */
  portfolio: (req: ExportPortfolioRequest) =>
    apiPost<ExportPortfolioResponse>("/export/portfolio", req),
};

export const notificationsApi = {
  /** GET /notifications?portfolio_id=... */
  list: (portfolioId?: string) =>
    apiGet<Notification[]>(
      "/notifications",
      portfolioId ? { portfolio_id: portfolioId } : undefined
    ),

  /** POST /notifications/{notification_id}/ack */
  ack: (id: string) =>
    apiPost<AckNotificationResponse>(`/notifications/${id}/ack`, {}),
};
