"use client";

import { useEffect, useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import {
  BarChart, Bar, Cell, XAxis, YAxis, Tooltip, ResponsiveContainer,
  AreaChart, Area, CartesianGrid, Legend,
} from "recharts";
import { Download, ChevronDown, ChevronUp, ChevronLeft, ChevronRight } from "lucide-react";
import { usePortfolios, useRiskSummary, useExportPortfolio } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { ScoreDisplay, ScoreCell, BAND_COLOR } from "@/components/ui/BandBadge";
import { PerilChip } from "@/components/ui/PerilChip";
import { Skeleton, SkeletonCard } from "@/components/ui/Skeleton";
import type { BandKey, TopAsset, AllPerilKey } from "@/types";

const BANDS: BandKey[] = ["minimal", "minor", "moderate", "major", "extreme"];
const BAND_COLOR_HEX: Record<BandKey, string> = {
  minimal: "#8BBF8B", minor: "#C8C84A", moderate: "#E8903A", major: "#D44A2A", extreme: "#AA1A1A",
};
const PERIL_COLOR_HEX: Record<string, string> = {
  all: "#1B3A4B", heat: "#E05520", rain: "#2460C8", wind: "#6B40B0", drought: "#C09020",
};
const PERILS: AllPerilKey[] = ["all", "heat", "rain", "wind", "drought"];
const PERIL_LABELS: Record<string, string> = {
  all: "All", heat: "Heat", rain: "Rain", wind: "Wind", drought: "Drought",
};

// ── Export Modal ──────────────────────────────────────────────────────────────

function ExportModal({
  onClose, portfolioId, start, end,
}: {
  onClose: () => void; portfolioId: string; start: string; end: string;
}) {
  const { mutate, isPending, data, error } = useExportPortfolio();

  useEffect(() => {
    mutate({ portfolio_id: portfolioId, start_date: start, end_date: end, format: "csv", include_drivers: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm">
      <div
        className="rounded-2xl p-7 w-full max-w-md mx-4 shadow-popup"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <h3 className="font-serif text-lg mb-5" style={{ color: "var(--text-primary)" }}>
          Export Portfolio
        </h3>
        {isPending && (
          <div className="flex items-center gap-3 mb-4" style={{ color: "var(--text-secondary)" }}>
            <div className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin opacity-50" />
            <span className="text-sm">Queuing export…</span>
          </div>
        )}
        {error && (
          <p className="text-sm mb-4" style={{ color: "var(--extreme)" }}>
            {(error as Error).message}
          </p>
        )}
        {data && (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <span
                className="text-[10px] font-bold uppercase px-2 py-0.5 rounded border"
                style={{
                  color: data.status === "success" ? "var(--minimal)" : data.status === "failed" ? "var(--extreme)" : "var(--rain)",
                  borderColor: "currentColor",
                  backgroundColor: "transparent",
                }}
              >
                {data.status}
              </span>
              <span className="font-mono text-xs" style={{ color: "var(--text-muted)" }}>{data.export_id}</span>
            </div>
            <div
              className="rounded-lg px-3 py-2 font-mono text-xs break-all"
              style={{ backgroundColor: "var(--bg-page)", color: "var(--text-secondary)" }}
            >
              {data.path}
            </div>
            {data.download_url ? (
              <a
                href={data.download_url}
                target="_blank"
                rel="noopener noreferrer"
                className="block text-center py-2 rounded-lg text-sm font-medium text-white"
                style={{ backgroundColor: "var(--accent)" }}
              >
                Download CSV
              </a>
            ) : (
              <button
                onClick={() => navigator.clipboard.writeText(data.path)}
                className="w-full py-2 rounded-lg border text-sm transition-colors"
                style={{ borderColor: "var(--border)", color: "var(--text-secondary)" }}
              >
                📋 Copy GCS path
              </button>
            )}
            <p className="text-[11px]" style={{ color: "var(--text-muted)" }}>
              Download link will appear when export completes.
            </p>
          </div>
        )}
        <button
          onClick={onClose}
          className="mt-5 w-full py-2 rounded-lg border text-sm transition-colors"
          style={{ borderColor: "var(--border)", color: "var(--text-secondary)" }}
        >
          Close
        </button>
      </div>
    </div>
  );
}

// ── HazardBar (right panel) ───────────────────────────────────────────────────

function HazardFactorRow({ peril, score }: { peril: string; score: number | null | undefined }) {
  const pct = score ?? 0;
  const color = PERIL_COLOR_HEX[peril] ?? "#5C5850";
  return (
    <div className="flex items-center gap-3 py-2 border-b" style={{ borderColor: "var(--border)" }}>
      <span className="w-16 text-xs font-semibold" style={{ color }}>
        {PERIL_LABELS[peril] ?? peril}
      </span>
      <span className="w-14 font-mono text-sm text-right tabular-nums" style={{ color }}>
        {score ?? "—"}/100
      </span>
      <div className="flex-1 h-2 rounded-full overflow-hidden" style={{ backgroundColor: "var(--border)" }}>
        <div
          className="h-full rounded-full score-segment"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
    </div>
  );
}

// ── Main Page ─────────────────────────────────────────────────────────────────

type SortKey = AllPerilKey;

const ROWS_PER_PAGE = 10;

function SortIcon({ k, sortKey, sortAsc }: { k: SortKey; sortKey: SortKey; sortAsc: boolean }) {
  return sortKey === k
    ? sortAsc ? <ChevronUp size={12} /> : <ChevronDown size={12} />
    : <ChevronDown size={12} className="opacity-30" />;
}

export default function PortfolioPage() {
  const router = useRouter();
  const { selectedPortfolioId, setSelectedPortfolioId, startDate, endDate } = useGlobalStore();
  const { data: portfolios, isLoading: loadingPortfolios } = usePortfolios();
  const { data: summary, isLoading: loadingSummary, error, refetch } =
    useRiskSummary(selectedPortfolioId, startDate, endDate);

  const [showExport, setShowExport]   = useState(false);
  const [sortKey, setSortKey]         = useState<SortKey>("all");
  const [sortAsc, setSortAsc]         = useState(false);
  const [perilFilter, setPerilFilter] = useState<AllPerilKey>("all");
  const [search, setSearch]           = useState("");
  const [bandFilter, setBandFilter]   = useState<BandKey | "all">("all");
  const [page, setPage]               = useState(1);
  const [trendPeril, setTrendPeril]   = useState<AllPerilKey>("all");
  const [showPct, setShowPct]         = useState(true);

  // Auto-select first portfolio
  useEffect(() => {
    if (!selectedPortfolioId && portfolios?.length) {
      setSelectedPortfolioId(portfolios[0].portfolio_id);
    }
  }, [portfolios, selectedPortfolioId, setSelectedPortfolioId]);

  // Band distribution for bar chart
  const totalAssets = summary ? Object.values(summary.bands).reduce((a: number, b) => a + (b ?? 0), 0) : 0;
  const bandChartData = BANDS.map((b) => {
    const count = summary?.bands[b] ?? 0;
    return { band: b, count, pct: totalAssets ? Math.round((count / totalAssets) * 1000) / 10 : 0 };
  });

  // Trend data
  const trendData = summary?.trend ?? [];

  // Sorted + filtered assets
  const sortedAssets: TopAsset[] = useMemo(() => {
    const base: TopAsset[] = summary?.top_assets ?? [];
    return base
      .filter((a) => {
        const matchSearch = a.name.toLowerCase().includes(search.toLowerCase()) ||
          a.asset_id.toLowerCase().includes(search.toLowerCase());
        const matchBand = bandFilter === "all" || a.band === bandFilter;
        return matchSearch && matchBand;
      })
      .sort((a, b) => {
        const diff = ((a.scores[sortKey] ?? 0) as number) - ((b.scores[sortKey] ?? 0) as number);
        return sortAsc ? diff : -diff;
      });
  }, [summary, search, bandFilter, sortKey, sortAsc]);

  const totalPages = Math.ceil(sortedAssets.length / ROWS_PER_PAGE);
  const pageAssets = sortedAssets.slice((page - 1) * ROWS_PER_PAGE, page * ROWS_PER_PAGE);

  function handleSort(key: SortKey) {
    if (key === sortKey) setSortAsc((v) => !v);
    else { setSortKey(key); setSortAsc(false); }
    setPage(1);
  }

  function handleRowClick(asset: TopAsset) {
    const { setSelectedAsset } = useGlobalStore.getState();
    setSelectedAsset({ asset_id: asset.asset_id, name: asset.name, lat: asset.lat, lon: asset.lon });
    router.push(`/assets/${asset.asset_id}`);
  }

  if (!selectedPortfolioId && !loadingPortfolios) {
    return (
      <div
        className="rounded-2xl p-16 text-center"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <div className="text-4xl mb-4">📂</div>
        <p className="font-serif text-lg" style={{ color: "var(--text-primary)" }}>No portfolio selected</p>
        <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>Select a portfolio in the top bar</p>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto">

      {/* Export button */}
      <div className="flex justify-end">
        <button
          onClick={() => setShowExport(true)}
          disabled={!selectedPortfolioId}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          style={{
            backgroundColor: "var(--bg-surface)",
            borderColor: "var(--border)",
            color: "var(--accent)",
          }}
        >
          <Download size={15} /> Export CSV
        </button>
      </div>

      {/* Error */}
      {error && (
        <div
          className="rounded-xl p-5 flex items-center justify-between"
          style={{ backgroundColor: "rgba(170,26,26,0.06)", border: "1px solid rgba(170,26,26,0.2)" }}
        >
          <span className="text-sm" style={{ color: "var(--extreme)" }}>{(error as Error).message}</span>
          <button
            onClick={() => refetch()}
            className="px-3 py-1.5 rounded text-xs font-medium"
            style={{ backgroundColor: "rgba(170,26,26,0.1)", color: "var(--extreme)" }}
          >
            Retry
          </button>
        </div>
      )}

      {/* ── SECTION A: Score cards ── */}
      <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-4">
        {loadingSummary
          ? Array.from({ length: 5 }).map((_, i) => <SkeletonCard key={i} />)
          : PERILS.map((peril) => {
              const score = summary?.peril_averages[peril];
              return (
                <div
                  key={peril}
                  className="rounded-xl p-5"
                  style={{
                    backgroundColor: "var(--bg-surface)",
                    border: `1px solid var(--border)`,
                    borderLeft: peril !== "all" ? `3px solid ${PERIL_COLOR_HEX[peril]}` : `1px solid var(--border)`,
                  }}
                >
                  <p className="text-[10px] uppercase tracking-widest mb-3 font-semibold" style={{ color: "var(--text-muted)" }}>
                    {PERIL_LABELS[peril]} Factor
                  </p>
                  <ScoreDisplay
                    score={score != null ? Math.round(score) : null}
                    band={undefined}
                    size={peril === "all" ? "lg" : "md"}
                  />
                  <p className="text-[10px] mt-2" style={{ color: "var(--text-muted)" }}>
                    avg · {startDate} → {endDate}
                  </p>
                </div>
              );
            })}
      </div>

      {/* ── SECTION B: Band chart + Hazard factor bars ── */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        {/* Left — Band distribution */}
        <div
          className="rounded-xl p-5"
          style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
        >
          <div className="flex items-center justify-between mb-4">
            <p className="text-xs font-bold uppercase tracking-wider" style={{ color: "var(--text-secondary)" }}>
              All-Hazard Factors
            </p>
            <div
              className="flex rounded border overflow-hidden text-[10px] font-semibold"
              style={{ borderColor: "var(--border)" }}
            >
              {(["% of assets", "Count"] as const).map((opt) => {
                const isPct = opt === "% of assets";
                return (
                  <button
                    key={opt}
                    onClick={() => setShowPct(isPct)}
                    className="px-2 py-1 transition-colors"
                    style={{
                      backgroundColor: showPct === isPct ? "var(--accent)" : "transparent",
                      color: showPct === isPct ? "#fff" : "var(--text-muted)",
                    }}
                  >
                    {opt}
                  </button>
                );
              })}
            </div>
          </div>
          {loadingSummary ? <Skeleton className="h-48 w-full" /> : (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={bandChartData} margin={{ top: 20, bottom: 0 }}>
                <XAxis dataKey="band" tick={{ fill: "var(--text-muted)", fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis hide />
                <Tooltip
                  contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8, fontSize: 12 }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={(val: any, name: any) => [showPct ? `${val}%` : val, name]}
                />
                {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
                <Bar dataKey={showPct ? "pct" : "count"} radius={[4, 4, 0, 0]} label={{ position: "top", fontSize: 11, fill: "var(--text-secondary)", formatter: (v: any) => showPct ? `${v}%` : v }}>
                  {bandChartData.map((entry) => (
                    <Cell key={entry.band} fill={BAND_COLOR_HEX[entry.band as BandKey]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Right — Hazard factor bars */}
        <div
          className="rounded-xl p-5"
          style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
        >
          <p className="text-xs font-bold uppercase tracking-wider mb-4" style={{ color: "var(--text-secondary)" }}>
            Hazard Factors
          </p>
          {loadingSummary
            ? Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-8 w-full mb-2" />)
            : ["heat", "rain", "wind", "drought"].map((p) => (
                <HazardFactorRow key={p} peril={p} score={summary?.peril_averages[p as AllPerilKey] != null ? Math.round(summary!.peril_averages[p as AllPerilKey]!) : null} />
              ))
          }
        </div>
      </div>

      {/* ── SECTION C: Asset table ── */}
      <div
        className="rounded-xl overflow-hidden"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        {/* Controls */}
        <div className="p-4 border-b flex flex-wrap items-center gap-3" style={{ borderColor: "var(--border)" }}>
          {/* Controls row 1 */}
          <div className="flex gap-1.5 flex-wrap">
            {PERILS.map((p) => (
              <PerilChip key={p} peril={p} active={perilFilter === p} onClick={() => { setPerilFilter(p); setPage(1); }} />
            ))}
          </div>
          <div className="ml-auto flex items-center gap-2 flex-wrap">
            <select
              value={bandFilter}
              onChange={(e) => { setBandFilter(e.target.value as BandKey | "all"); setPage(1); }}
              className="text-xs rounded-lg px-2.5 py-1.5 border outline-none"
              style={{ backgroundColor: "var(--bg-page)", borderColor: "var(--border)", color: "var(--text-primary)" }}
            >
              <option value="all">Band: All</option>
              {BANDS.map((b) => <option key={b} value={b}>{b.charAt(0).toUpperCase() + b.slice(1)}</option>)}
            </select>
            <div
              className="flex items-center gap-1.5 rounded-lg border px-2.5 py-1.5"
              style={{ borderColor: "var(--border)", backgroundColor: "var(--bg-page)" }}
            >
              <svg className="w-3.5 h-3.5" fill="none" stroke="var(--text-muted)" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                value={search}
                onChange={(e) => { setSearch(e.target.value); setPage(1); }}
                placeholder="Search assets…"
                className="bg-transparent outline-none text-xs w-40"
                style={{ color: "var(--text-primary)" }}
              />
            </div>
          </div>
        </div>

        {/* Table */}
        {loadingSummary ? (
          <div>{Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="flex gap-4 px-5 py-3.5 border-b" style={{ borderColor: "var(--border)" }}>
              <Skeleton className="h-3 w-32" />
              {Array.from({ length: 5 }).map((__, j) => <Skeleton key={j} className="h-3 w-16 ml-auto" />)}
            </div>
          ))}</div>
        ) : sortedAssets.length === 0 ? (
          <div className="p-16 text-center">
            <div className="text-3xl mb-3">🧭</div>
            <p className="font-medium" style={{ color: "var(--text-secondary)" }}>No assets found</p>
            <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>Try adjusting filters or search</p>
          </div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b" style={{ borderColor: "var(--border)" }}>
                    <th className="px-5 py-3 text-left text-[11px] font-bold uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>
                      Asset
                    </th>
                    {PERILS.map((p) => (
                      <th
                        key={p}
                        className="px-4 py-3 text-left text-[11px] font-bold uppercase tracking-widest cursor-pointer select-none"
                        style={{ color: p === perilFilter ? PERIL_COLOR_HEX[p] : "var(--text-muted)" }}
                        onClick={() => handleSort(p)}
                      >
                        <span className="flex items-center gap-1">
                          {PERIL_LABELS[p]}
                          <SortIcon k={p} sortKey={sortKey} sortAsc={sortAsc} />
                        </span>
                      </th>
                    ))}
                    <th className="px-4 py-3 text-left text-[11px] font-bold uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>
                      Band
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {pageAssets.map((asset) => (
                    <tr
                      key={asset.asset_id}
                      className="border-b cursor-pointer transition-colors hover:bg-[var(--bg-page)]"
                      style={{ borderColor: "var(--border)" }}
                      onClick={() => handleRowClick(asset)}
                    >
                      <td className="px-5 py-3.5">
                        <p className="font-medium text-sm max-w-[180px] truncate" style={{ color: "var(--text-primary)" }}>{asset.name}</p>
                        <p className="font-mono text-[10px] mt-0.5" style={{ color: "var(--text-muted)" }}>{asset.asset_id}</p>
                      </td>
                      {PERILS.map((p) => (
                        <td key={p} className="px-4 py-3.5">
                          <ScoreCell score={asset.scores[p] != null ? Math.round(asset.scores[p]!) : null} band={p === "all" ? asset.band : undefined} />
                        </td>
                      ))}
                      <td className="px-4 py-3.5">
                        <span
                          className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border"
                          style={{ color: BAND_COLOR[asset.band], backgroundColor: BAND_COLOR[asset.band] + "15", borderColor: BAND_COLOR[asset.band] + "40" }}
                        >
                          {asset.band}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            <div className="px-5 py-3 flex items-center justify-between border-t text-sm" style={{ borderColor: "var(--border)" }}>
              <span style={{ color: "var(--text-muted)" }}>
                {(page - 1) * ROWS_PER_PAGE + 1}–{Math.min(page * ROWS_PER_PAGE, sortedAssets.length)} of {sortedAssets.length}
              </span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage((p) => p - 1)}
                  disabled={page === 1}
                  className="p-1.5 rounded disabled:opacity-30"
                  style={{ color: "var(--text-secondary)" }}
                >
                  <ChevronLeft size={16} />
                </button>
                <span className="px-2 font-mono text-xs" style={{ color: "var(--text-secondary)" }}>
                  {page} / {totalPages}
                </span>
                <button
                  onClick={() => setPage((p) => p + 1)}
                  disabled={page >= totalPages}
                  className="p-1.5 rounded disabled:opacity-30"
                  style={{ color: "var(--text-secondary)" }}
                >
                  <ChevronRight size={16} />
                </button>
              </div>
            </div>
          </>
        )}
      </div>

      {/* ── SECTION D: Trend chart ── */}
      <div
        className="rounded-xl p-5"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
          <p className="text-xs font-bold uppercase tracking-wider" style={{ color: "var(--text-secondary)" }}>
            Risk Trend
          </p>
          <div className="flex gap-1.5">
            {PERILS.map((p) => (
              <PerilChip key={p} peril={p} active={trendPeril === p} onClick={() => setTrendPeril(p)} />
            ))}
          </div>
        </div>
        {loadingSummary ? <Skeleton className="h-52 w-full" /> : trendData.length === 0 ? (
          <div className="h-52 flex items-center justify-center text-sm" style={{ color: "var(--text-muted)" }}>
            No trend data available
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={trendData}>
              <defs>
                {PERILS.map((p) => (
                  <linearGradient key={p} id={`grad-${p}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={PERIL_COLOR_HEX[p]} stopOpacity={0.15} />
                    <stop offset="95%" stopColor={PERIL_COLOR_HEX[p]} stopOpacity={0} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                dataKey="date"
                tick={{ fill: "var(--text-muted)", fontSize: 10 }}
                axisLine={false} tickLine={false}
                tickFormatter={(v: string) => v.slice(5)}
              />
              <YAxis domain={[0, 100]} tick={{ fill: "var(--text-muted)", fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8 }}
                labelStyle={{ color: "var(--text-primary)" }}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: "var(--text-secondary)" }} />
              {PERILS.filter((p) => trendPeril === "all" || p === trendPeril || p === "all").map((k) => (
                <Area
                  key={k}
                  type="monotone"
                  dataKey={`scores.${k}`}
                  name={PERIL_LABELS[k]}
                  stroke={PERIL_COLOR_HEX[k]}
                  strokeWidth={k === "all" ? 2.5 : 1.5}
                  fill={`url(#grad-${k})`}
                  dot={false}
                  strokeOpacity={k === "all" ? 1 : 0.8}
                />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {showExport && selectedPortfolioId && (
        <ExportModal
          onClose={() => setShowExport(false)}
          portfolioId={selectedPortfolioId}
          start={startDate}
          end={endDate}
        />
      )}
    </div>
  );
}
