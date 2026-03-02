"use client";

import { create } from "zustand";
import type { DateRangeLabel } from "@/types";

function daysAgoStr(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().split("T")[0];
}

function todayStr(): string {
  return new Date().toISOString().split("T")[0];
}

const RANGE_DAYS: Record<DateRangeLabel, number> = {
  "30d": 30,
  "90d": 90,
  "365d": 365,
};

interface SelectedAsset {
  asset_id: string;
  name: string;
  lat: number;
  lon: number;
}

interface GlobalStore {
  selectedPortfolioId: string | null;
  setSelectedPortfolioId: (id: string | null) => void;

  dateRangeLabel: DateRangeLabel;
  startDate: string;
  endDate: string;
  setDateRange: (label: DateRangeLabel) => void;

  // set before navigating to /assets/[id]
  selectedAsset: SelectedAsset | null;
  setSelectedAsset: (asset: SelectedAsset | null) => void;
}

export const useGlobalStore = create<GlobalStore>((set) => ({
  selectedPortfolioId: null,
  setSelectedPortfolioId: (id) => set({ selectedPortfolioId: id }),

  dateRangeLabel: "90d",
  startDate: daysAgoStr(90),
  endDate: todayStr(),
  setDateRange: (label) =>
    set({
      dateRangeLabel: label,
      startDate: daysAgoStr(RANGE_DAYS[label]),
      endDate: todayStr(),
    }),

  selectedAsset: null,
  setSelectedAsset: (asset) => set({ selectedAsset: asset }),
}));
