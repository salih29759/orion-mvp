"use client";

import { useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import { useRiskSummary, usePortfolios } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { BandBadge } from "@/components/ui/BandBadge";
import { SkeletonRow, Skeleton } from "@/components/ui/Skeleton";
import type { BandKey, RiskSummaryTopAsset } from "@/types";

const PERIL_COLOR: Record<string, string> = {
  all:     "#1e6fff",
  heat:    "#f97316",
  rain:    "#3b82f6",
  wind:    "#06b6d4",
  drought: "#d97706",
};

const BANDS: BandKey[] = ["minimal", "minor", "moderate", "major", "extreme"];

type SortKey = "all" | "heat" | "rain" | "wind" | "drought";

export default function AssetsPage() {
  const router = useRouter();
  const { selectedPortfolioId, startDate, endDate, setSelectedAsset } = useGlobalStore();

  const { data: portfolios } = usePortfolios();
  const { data: summary, isLoading, error, refetch } =
    useRiskSummary(selectedPortfolioId, startDate, endDate);

  const [search, setSearch] = useState("");
  const [bandFilter, setBandFilter] = useState<BandKey | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("all");
  const [sortAsc, setSortAsc] = useState(false);

  const portfolioName = portfolios?.find(
    (p) => p.portfolio_id === selectedPortfolioId
  )?.name;

  const assets: RiskSummaryTopAsset[] = useMemo(() => {
    const base = summary?.top_assets ?? [];
    return base
      .filter((a) => {
        const matchSearch = a.name.toLowerCase().includes(search.toLowerCase());
        const matchBand = bandFilter === "all" || a.band === bandFilter;
        return matchSearch && matchBand;
      })
      .sort((a, b) => {
        const diff = (a.scores[sortKey] ?? 0) - (b.scores[sortKey] ?? 0);
        return sortAsc ? diff : -diff;
      });
  }, [summary, search, bandFilter, sortKey, sortAsc]);

  function handleRowClick(asset: RiskSummaryTopAsset) {
    setSelectedAsset({
      asset_id: asset.asset_id,
      name: asset.name,
      lat: asset.lat,
      lon: asset.lon,
    });
    router.push(`/assets/${asset.asset_id}`);
  }

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortAsc((v) => !v);
    else { setSortKey(key); setSortAsc(false); }
  }

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey === k ? (
      <span className="ml-1 text-[#1e6fff]">{sortAsc ? "↑" : "↓"}</span>
    ) : null;

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-white tracking-tight">Assets</h1>
        <p className="text-sm text-white/40 mt-0.5">
          {portfolioName ? `Portfolio: ${portfolioName}` : "Select a portfolio in Portfolio view"}
          {summary && ` · ${summary.top_assets.length} assets`}
        </p>
      </div>

      {/* No portfolio selected */}
      {!selectedPortfolioId && (
        <div className="bg-[#070f1f] border border-white/8 rounded-xl p-12 text-center">
          <div className="text-4xl mb-4">📂</div>
          <div className="text-white/60 font-medium">No portfolio selected</div>
          <div className="text-white/30 text-sm mt-1">
            Go to <a href="/portfolio" className="text-[#1e6fff] hover:underline">Portfolio</a> and select a portfolio first
          </div>
        </div>
      )}

      {selectedPortfolioId && (
        <>
          {/* Filters */}
          <div className="flex flex-wrap items-center gap-3">
            {/* Search */}
            <div className="relative">
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/30" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                placeholder="Search by name…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="bg-[#0f2040] border border-white/10 text-white text-sm rounded-lg pl-9 pr-4 py-2 w-64 focus:outline-none focus:border-blue-500/50 placeholder:text-white/30"
              />
            </div>

            {/* Band filter */}
            <select
              value={bandFilter}
              onChange={(e) => setBandFilter(e.target.value as BandKey | "all")}
              className="bg-[#0f2040] border border-white/10 text-white text-sm rounded-lg px-3 py-2 focus:outline-none"
            >
              <option value="all">All bands</option>
              {BANDS.map((b) => (
                <option key={b} value={b}>{b.charAt(0).toUpperCase() + b.slice(1)}</option>
              ))}
            </select>
          </div>

          {/* Error */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-5 flex items-center justify-between">
              <div className="text-red-400 text-sm">{(error as Error).message}</div>
              <button onClick={() => refetch()} className="px-3 py-1.5 rounded bg-red-500/15 border border-red-500/25 text-red-400 text-xs hover:bg-red-500/25 transition-colors">
                Retry
              </button>
            </div>
          )}

          {/* Table */}
          <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/8">
                    <th className="px-5 py-3 text-left text-[11px] font-semibold text-white/40 uppercase tracking-wider">
                      Asset
                    </th>
                    {(["all", "heat", "rain", "wind", "drought"] as SortKey[]).map((k) => (
                      <th
                        key={k}
                        className="px-5 py-3 text-left text-[11px] font-semibold text-white/40 uppercase tracking-wider cursor-pointer hover:text-white/70 select-none"
                        onClick={() => toggleSort(k)}
                      >
                        {k} <SortIcon k={k} />
                      </th>
                    ))}
                    <th className="px-5 py-3 text-left text-[11px] font-semibold text-white/40 uppercase tracking-wider">
                      Band
                    </th>
                    <th className="px-5 py-3 text-left text-[11px] font-semibold text-white/40 uppercase tracking-wider">
                      Coords
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {isLoading
                    ? Array.from({ length: 6 }).map((_, i) => (
                        <tr key={i}>
                          <td colSpan={8} className="p-0">
                            <SkeletonRow />
                          </td>
                        </tr>
                      ))
                    : assets.length === 0
                    ? (
                        <tr>
                          <td colSpan={8} className="px-5 py-12 text-center text-white/30 text-sm">
                            {search || bandFilter !== "all"
                              ? "No assets match your filters"
                              : "No assets in this portfolio"}
                          </td>
                        </tr>
                      )
                    : assets.map((asset) => (
                        <tr
                          key={asset.asset_id}
                          className="hover:bg-white/3 cursor-pointer transition-colors group"
                          onClick={() => handleRowClick(asset)}
                        >
                          <td className="px-5 py-3.5">
                            <div className="font-medium text-white/90 group-hover:text-white transition-colors max-w-[180px] truncate">
                              {asset.name}
                            </div>
                            <div className="text-[10px] text-white/30 font-mono mt-0.5">
                              {asset.asset_id.slice(0, 8)}…
                            </div>
                          </td>
                          {(["all", "heat", "rain", "wind", "drought"] as SortKey[]).map((k) => (
                            <td key={k} className="px-5 py-3.5 tabular-nums">
                              <span className="font-bold" style={{ color: PERIL_COLOR[k] }}>
                                {asset.scores[k] ?? "—"}
                              </span>
                            </td>
                          ))}
                          <td className="px-5 py-3.5">
                            <BandBadge band={asset.band} />
                          </td>
                          <td className="px-5 py-3.5 font-mono text-[11px] text-white/30">
                            {asset.lat.toFixed(2)}, {asset.lon.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                </tbody>
              </table>
            </div>

            {!isLoading && assets.length > 0 && (
              <div className="px-5 py-3 border-t border-white/5 text-[11px] text-white/30">
                Showing {assets.length} asset{assets.length !== 1 ? "s" : ""} · Sourced from portfolio risk summary
              </div>
            )}
          </div>

          {/* Skeleton for loading charts placeholder */}
          {isLoading && <Skeleton className="h-4 w-48" />}
        </>
      )}
    </div>
  );
}
