"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend,
} from "recharts";
import { usePortfolios, useRiskSummary, useExportPortfolio } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { BandBadge } from "@/components/ui/BandBadge";
import { SkeletonCard, SkeletonRow, Skeleton } from "@/components/ui/Skeleton";
import type { BandKey, DateRangeLabel } from "@/types";

// ── Colour maps ───────────────────────────────────────────────────────────────

const BAND_COLOR: Record<BandKey, string> = {
  minimal:  "#22c55e",
  minor:    "#eab308",
  moderate: "#f97316",
  major:    "#ef4444",
  extreme:  "#7f1d1d",
};

const PERIL_COLOR: Record<string, string> = {
  all:     "#1e6fff",
  heat:    "#f97316",
  rain:    "#3b82f6",
  wind:    "#06b6d4",
  drought: "#d97706",
};

// ── Score card ────────────────────────────────────────────────────────────────

function ScoreCard({
  label, value, color, sub,
}: {
  label: string; value: number | null; color: string; sub?: string;
}) {
  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
      <div className="text-[11px] text-white/50 uppercase tracking-widest mb-2">{label}</div>
      <div
        className="text-3xl font-bold tabular-nums"
        style={{ color: value != null ? color : "#ffffff30" }}
      >
        {value != null ? value : "—"}
      </div>
      {sub && <div className="text-[11px] text-white/30 mt-1">{sub}</div>}
    </div>
  );
}

// ── Export modal ──────────────────────────────────────────────────────────────

function ExportModal({
  onClose, portfolioId, start, end,
}: {
  onClose: () => void;
  portfolioId: string;
  start: string;
  end: string;
}) {
  const { mutate, isPending, data, error } = useExportPortfolio();

  useEffect(() => {
    mutate({ portfolio_id: portfolioId, start_date: start, end_date: end, format: "csv", include_drivers: true });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[#0f2040] border border-white/10 rounded-2xl p-8 w-full max-w-md mx-4 shadow-2xl">
        <h3 className="text-lg font-semibold text-white mb-4">Export Portfolio</h3>

        {isPending && (
          <div className="flex items-center gap-3 text-white/60">
            <div className="w-4 h-4 border-2 border-blue-400/40 border-t-blue-400 rounded-full animate-spin" />
            Queuing export…
          </div>
        )}

        {error && (
          <div className="text-red-400 text-sm">
            Failed: {(error as Error).message}
          </div>
        )}

        {data && (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded bg-blue-500/15 text-blue-400 border border-blue-500/20">
                {data.status}
              </span>
              <span className="text-white/60 text-sm">Export queued</span>
            </div>
            <div className="bg-[#070f1f] rounded-lg px-3 py-2 font-mono text-xs text-white/50 break-all">
              {data.path}
            </div>
            {data.download_url && (
              <a
                href={data.download_url}
                target="_blank"
                rel="noopener noreferrer"
                className="block text-center py-2 rounded-lg bg-[#1e6fff] hover:bg-[#1e6fff]/80 text-white text-sm font-medium transition-colors"
              >
                Download CSV
              </a>
            )}
            {!data.download_url && (
              <button
                onClick={() => navigator.clipboard.writeText(data.path)}
                className="w-full py-2 rounded-lg border border-white/10 text-white/60 hover:text-white text-sm transition-colors"
              >
                Copy path
              </button>
            )}
          </div>
        )}

        <button
          onClick={onClose}
          className="mt-6 w-full py-2 rounded-lg border border-white/10 text-white/40 hover:text-white text-sm transition-colors"
        >
          Close
        </button>
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function PortfolioPage() {
  const { selectedPortfolioId, setSelectedPortfolioId, dateRangeLabel, startDate, endDate, setDateRange } =
    useGlobalStore();

  const { data: portfolios, isLoading: loadingPortfolios } = usePortfolios();
  const { data: summary, isLoading: loadingSummary, error: summaryError, refetch } =
    useRiskSummary(selectedPortfolioId, startDate, endDate);

  const [showExport, setShowExport] = useState(false);
  const [sortField, setSortField] = useState<"all" | "heat" | "rain" | "wind" | "drought">("all");

  // auto-select first portfolio
  useEffect(() => {
    if (!selectedPortfolioId && portfolios && portfolios.length > 0) {
      setSelectedPortfolioId(portfolios[0].portfolio_id);
    }
  }, [portfolios, selectedPortfolioId, setSelectedPortfolioId]);

  const bandData = summary
    ? (["minimal", "minor", "moderate", "major", "extreme"] as BandKey[]).map((b) => ({
        band: b,
        count: summary.bands[b],
        fill: BAND_COLOR[b],
      }))
    : [];

  const sortedTopAssets = summary
    ? [...summary.top_assets].sort((a, b) => (b.scores[sortField] ?? 0) - (a.scores[sortField] ?? 0))
    : [];

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header row */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Portfolio Overview</h1>
          <p className="text-sm text-white/40 mt-0.5">All-hazards climate risk across your portfolio</p>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          {/* Portfolio selector */}
          <select
            value={selectedPortfolioId ?? ""}
            onChange={(e) => setSelectedPortfolioId(e.target.value)}
            className="bg-[#0f2040] border border-white/10 text-white text-sm rounded-lg px-3 py-2 focus:outline-none focus:border-blue-500/50"
            disabled={loadingPortfolios}
          >
            {loadingPortfolios && <option>Loading…</option>}
            {!loadingPortfolios && (!portfolios || portfolios.length === 0) && (
              <option value="">No portfolios</option>
            )}
            {portfolios?.map((p) => (
              <option key={p.portfolio_id} value={p.portfolio_id}>
                {p.name}
              </option>
            ))}
          </select>

          {/* Date range */}
          <div className="flex rounded-lg border border-white/10 overflow-hidden text-sm">
            {(["30d", "90d", "365d"] as DateRangeLabel[]).map((label) => (
              <button
                key={label}
                onClick={() => setDateRange(label)}
                className={`px-3 py-2 transition-colors ${
                  dateRangeLabel === label
                    ? "bg-[#1e6fff] text-white"
                    : "bg-[#0f2040] text-white/50 hover:text-white"
                }`}
              >
                {label}
              </button>
            ))}
          </div>

          {/* Export */}
          <button
            onClick={() => setShowExport(true)}
            disabled={!selectedPortfolioId}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-[#1e6fff]/15 border border-[#1e6fff]/30 text-[#1e6fff] hover:bg-[#1e6fff]/25 text-sm font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5}
                d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
            Export CSV
          </button>
        </div>
      </div>

      {/* Error state */}
      {summaryError && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-5 flex items-center justify-between">
          <div>
            <div className="text-red-400 font-medium">Failed to load portfolio data</div>
            <div className="text-red-400/60 text-sm mt-0.5">{(summaryError as Error).message}</div>
          </div>
          <button
            onClick={() => refetch()}
            className="px-4 py-2 rounded-lg bg-red-500/15 border border-red-500/25 text-red-400 text-sm hover:bg-red-500/25 transition-colors"
          >
            Retry
          </button>
        </div>
      )}

      {/* Empty state — no portfolio */}
      {!selectedPortfolioId && !loadingPortfolios && (
        <div className="bg-[#070f1f] border border-white/8 rounded-xl p-12 text-center">
          <div className="text-4xl mb-4">📂</div>
          <div className="text-white/60 font-medium">No portfolio selected</div>
          <div className="text-white/30 text-sm mt-1">Select a portfolio above to view risk data</div>
        </div>
      )}

      {/* Score cards */}
      {(loadingSummary || summary) && selectedPortfolioId && (
        <>
          <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-4">
            {loadingSummary
              ? Array.from({ length: 5 }).map((_, i) => <SkeletonCard key={i} />)
              : [
                  { label: "All Hazards", key: "all" as const, color: PERIL_COLOR.all },
                  { label: "Heat", key: "heat" as const, color: PERIL_COLOR.heat },
                  { label: "Rain", key: "rain" as const, color: PERIL_COLOR.rain },
                  { label: "Wind", key: "wind" as const, color: PERIL_COLOR.wind },
                  { label: "Drought", key: "drought" as const, color: PERIL_COLOR.drought },
                ].map(({ label, key, color }) => (
                  <ScoreCard
                    key={key}
                    label={label}
                    value={summary!.peril_averages[key]}
                    color={color}
                    sub={`avg · ${startDate} → ${endDate}`}
                  />
                ))}
          </div>

          {/* Charts row */}
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
            {/* Band distribution */}
            <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
              <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-4">
                Risk Band Distribution
              </div>
              {loadingSummary ? (
                <Skeleton className="h-48 w-full" />
              ) : bandData.every((d) => d.count === 0) ? (
                <div className="h-48 flex items-center justify-center text-white/30 text-sm">
                  No band data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={bandData} barCategoryGap="30%">
                    <XAxis
                      dataKey="band"
                      tick={{ fill: "#ffffff60", fontSize: 11 }}
                      axisLine={false}
                      tickLine={false}
                    />
                    <YAxis
                      tick={{ fill: "#ffffff40", fontSize: 11 }}
                      axisLine={false}
                      tickLine={false}
                    />
                    <Tooltip
                      contentStyle={{ background: "#0f2040", border: "1px solid #ffffff15", borderRadius: 8 }}
                      labelStyle={{ color: "#fff" }}
                      itemStyle={{ color: "#ffffff80" }}
                    />
                    <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                      {bandData.map((entry) => (
                        <rect key={entry.band} fill={entry.fill} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>

            {/* Trend line chart */}
            <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
              <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-4">
                Score Trend
              </div>
              {loadingSummary ? (
                <Skeleton className="h-48 w-full" />
              ) : !summary?.trend?.length ? (
                <div className="h-48 flex items-center justify-center text-white/30 text-sm">
                  No trend data available
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={summary.trend}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#ffffff08" />
                    <XAxis
                      dataKey="date"
                      tick={{ fill: "#ffffff40", fontSize: 10 }}
                      axisLine={false}
                      tickLine={false}
                      tickFormatter={(v: string) => v.slice(5)}
                    />
                    <YAxis
                      domain={[0, 100]}
                      tick={{ fill: "#ffffff40", fontSize: 11 }}
                      axisLine={false}
                      tickLine={false}
                    />
                    <Tooltip
                      contentStyle={{ background: "#0f2040", border: "1px solid #ffffff15", borderRadius: 8 }}
                      labelStyle={{ color: "#fff" }}
                    />
                    <Legend wrapperStyle={{ fontSize: 11, color: "#ffffff60" }} />
                    {(["all", "heat", "rain", "wind", "drought"] as const).map((k) => (
                      <Line
                        key={k}
                        type="monotone"
                        dataKey={k}
                        stroke={PERIL_COLOR[k]}
                        strokeWidth={k === "all" ? 2 : 1.5}
                        dot={false}
                        strokeOpacity={k === "all" ? 1 : 0.7}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          {/* Top assets table */}
          <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
            <div className="px-5 py-4 border-b border-white/8 flex items-center justify-between">
              <div className="text-xs font-semibold text-white/60 uppercase tracking-wider">
                Top Risk Assets
              </div>
              {/* Sort selector */}
              <select
                value={sortField}
                onChange={(e) => setSortField(e.target.value as typeof sortField)}
                className="bg-[#0f2040] border border-white/10 text-white/60 text-xs rounded px-2 py-1 focus:outline-none"
              >
                <option value="all">Sort: All Hazards</option>
                <option value="heat">Sort: Heat</option>
                <option value="rain">Sort: Rain</option>
                <option value="wind">Sort: Wind</option>
                <option value="drought">Sort: Drought</option>
              </select>
            </div>

            {loadingSummary ? (
              <div className="divide-y divide-white/5">
                {Array.from({ length: 5 }).map((_, i) => <SkeletonRow key={i} />)}
              </div>
            ) : sortedTopAssets.length === 0 ? (
              <div className="p-10 text-center text-white/30 text-sm">
                No assets found in this portfolio
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-white/5">
                      {["Asset", "All", "Heat", "Rain", "Wind", "Drought", "Band"].map((h) => (
                        <th key={h} className="px-5 py-3 text-left text-[11px] font-semibold text-white/40 uppercase tracking-wider">
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {sortedTopAssets.map((asset) => (
                      <tr key={asset.asset_id} className="hover:bg-white/3 transition-colors">
                        <td className="px-5 py-3.5 font-medium text-white/90 max-w-[200px] truncate">
                          {asset.name}
                        </td>
                        {(["all", "heat", "rain", "wind", "drought"] as const).map((k) => (
                          <td key={k} className="px-5 py-3.5">
                            <span
                              className="font-bold tabular-nums"
                              style={{ color: PERIL_COLOR[k] }}
                            >
                              {asset.scores[k] ?? "—"}
                            </span>
                          </td>
                        ))}
                        <td className="px-5 py-3.5">
                          <BandBadge band={asset.band} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}

      {/* Export modal */}
      {showExport && selectedPortfolioId && (
        <ExportModal
          portfolioId={selectedPortfolioId}
          start={startDate}
          end={endDate}
          onClose={() => setShowExport(false)}
        />
      )}
    </div>
  );
}
