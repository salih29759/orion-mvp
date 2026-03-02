"use client";

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  portfoliosApi,
  scoresApi,
  exportApi,
  notificationsApi,
} from "@/lib/orionApi";
import type {
  BatchScoresRequest,
  ExportPortfolioRequest,
  BatchAssetInput,
  PerilKey,
} from "@/types";

const PERILS: PerilKey[] = ["heat", "rain", "wind", "drought"];
const CLIMATOLOGY_VERSION = "v1_baseline_2015_2024";

// ── Query keys ────────────────────────────────────────────────────────────────

export const QK = {
  portfolios: ["portfolios"] as const,
  riskSummary: (id: string, start: string, end: string) =>
    ["riskSummary", id, start, end] as const,
  batchScores: (assetId: string, start: string, end: string) =>
    ["batchScores", assetId, start, end] as const,
  notifications: (portfolioId?: string) =>
    ["notifications", portfolioId ?? "all"] as const,
};

// ── Hooks ─────────────────────────────────────────────────────────────────────

export function usePortfolios() {
  return useQuery({
    queryKey: QK.portfolios,
    queryFn: portfoliosApi.list,
    staleTime: 10 * 60 * 1000,
  });
}

export function useRiskSummary(
  portfolioId: string | null,
  start: string,
  end: string
) {
  return useQuery({
    queryKey: QK.riskSummary(portfolioId ?? "", start, end),
    queryFn: () => portfoliosApi.riskSummary(portfolioId!, start, end),
    enabled: !!portfolioId,
  });
}

/** Fetches batch scores for a single asset (used on asset detail page). */
export function useBatchScores(
  asset: Pick<BatchAssetInput, "asset_id" | "lat" | "lon"> & { name?: string } | null,
  start: string,
  end: string
) {
  const req: BatchScoresRequest | null = asset
    ? {
        assets: [
          {
            asset_id: asset.asset_id,
            lat: asset.lat,
            lon: asset.lon,
            name: asset.name,
          },
        ],
        start_date: start,
        end_date: end,
        climatology_version: CLIMATOLOGY_VERSION,
        include_perils: PERILS,
      }
    : null;

  return useQuery({
    queryKey: QK.batchScores(asset?.asset_id ?? "", start, end),
    queryFn: () => scoresApi.batch(req!),
    enabled: !!asset,
  });
}

export function useNotifications(portfolioId?: string) {
  return useQuery({
    queryKey: QK.notifications(portfolioId),
    queryFn: () => notificationsApi.list(portfolioId),
    retry: (failureCount, error) => {
      // don't retry 404 — notifications endpoint not live yet
      if ((error as Error & { status?: number }).status === 404) return false;
      return failureCount < 1;
    },
  });
}

export function useAckNotification() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => notificationsApi.ack(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["notifications"] });
    },
  });
}

export function useExportPortfolio() {
  return useMutation({
    mutationFn: (req: ExportPortfolioRequest) => exportApi.portfolio(req),
  });
}
