"use client";

import { useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import { ChevronDown, ChevronUp, ChevronLeft, ChevronRight } from "lucide-react";
import { useRiskSummary } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { ScoreCell, BAND_COLOR } from "@/components/ui/BandBadge";
import { PerilChip } from "@/components/ui/PerilChip";
import { Skeleton } from "@/components/ui/Skeleton";
import type { BandKey, TopAsset, AllPerilKey } from "@/types";

const PERILS: AllPerilKey[] = ["all", "heat", "rain", "wind", "drought"];
const PERIL_LABELS: Record<string, string> = {
  all: "All Hazards", heat: "Heat", rain: "Rain", wind: "Wind", drought: "Drought",
};
const BANDS: BandKey[] = ["minimal", "minor", "moderate", "major", "extreme"];
const ROWS_PER_PAGE = 10;

export default function AssetsPage() {
  const router = useRouter();
  const { selectedPortfolioId, startDate, endDate } = useGlobalStore();
  const { data: summary, isLoading, error, refetch } =
    useRiskSummary(selectedPortfolioId, startDate, endDate);

  const [perilFilter, setPerilFilter] = useState<AllPerilKey>("all");
  const [bandFilter, setBandFilter]   = useState<BandKey | "all">("all");
  const [search, setSearch]           = useState("");
  const [sortKey, setSortKey]         = useState<AllPerilKey>("all");
  const [sortAsc, setSortAsc]         = useState(false);
  const [page, setPage]               = useState(1);

  const sortedAssets: TopAsset[] = useMemo(() => {
    const base = summary?.top_assets ?? [];
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

  const totalPages  = Math.ceil(sortedAssets.length / ROWS_PER_PAGE);
  const pageAssets  = sortedAssets.slice((page - 1) * ROWS_PER_PAGE, page * ROWS_PER_PAGE);

  function handleSort(k: AllPerilKey) {
    if (k === sortKey) setSortAsc((v) => !v);
    else { setSortKey(k); setSortAsc(false); }
    setPage(1);
  }

  function handleRowClick(asset: TopAsset) {
    const { setSelectedAsset } = useGlobalStore.getState();
    setSelectedAsset({ asset_id: asset.asset_id, name: asset.name, lat: asset.lat, lon: asset.lon });
    router.push(`/assets/${asset.asset_id}`);
  }

  const SortIcon = ({ k }: { k: AllPerilKey }) =>
    sortKey === k
      ? sortAsc ? <ChevronUp size={12} /> : <ChevronDown size={12} />
      : <ChevronDown size={12} className="opacity-30" />;

  return (
    <div className="space-y-5 max-w-[1600px] mx-auto">

      {/* Error */}
      {error && (
        <div
          className="rounded-xl p-5 flex items-center justify-between"
          style={{ backgroundColor: "rgba(170,26,26,0.06)", border: "1px solid rgba(170,26,26,0.2)" }}
        >
          <span className="text-sm" style={{ color: "var(--extreme)" }}>{(error as Error).message}</span>
          <button onClick={() => refetch()} className="text-xs px-3 py-1.5 rounded" style={{ backgroundColor: "rgba(170,26,26,0.1)", color: "var(--extreme)" }}>
            Retry
          </button>
        </div>
      )}

      {/* Table card */}
      <div
        className="rounded-xl overflow-hidden"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        {/* Filter bar */}
        <div className="p-4 border-b flex flex-wrap items-center gap-3" style={{ borderColor: "var(--border)" }}>
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
                placeholder="Search by Asset ID or name…"
                className="bg-transparent outline-none text-xs w-44"
                style={{ color: "var(--text-primary)" }}
              />
            </div>
          </div>
        </div>

        {/* Table */}
        {!selectedPortfolioId ? (
          <div className="p-16 text-center">
            <div className="text-3xl mb-3">🏗️</div>
            <p className="font-medium" style={{ color: "var(--text-secondary)" }}>No portfolio selected</p>
            <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>Choose a portfolio in the top bar</p>
          </div>
        ) : isLoading ? (
          <div>{Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="flex gap-4 px-5 py-3.5 border-b" style={{ borderColor: "var(--border)" }}>
              <Skeleton className="h-8 w-40" />
              {Array.from({ length: 5 }).map((__, j) => <Skeleton key={j} className="h-8 w-20" />)}
            </div>
          ))}</div>
        ) : sortedAssets.length === 0 ? (
          <div className="p-16 text-center">
            <div className="text-3xl mb-3">🧭</div>
            <p className="font-medium" style={{ color: "var(--text-secondary)" }}>
              {summary?.top_assets.length === 0 ? "Upload assets to begin scoring" : "No assets match filters"}
            </p>
            <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>
              {summary?.top_assets.length === 0 ? "Add assets via POST /scores/batch" : "Try adjusting search or band filter"}
            </p>
          </div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b" style={{ borderColor: "var(--border)", backgroundColor: "var(--bg-page)" }}>
                    <th className="px-5 py-3 text-left text-[11px] font-bold uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>
                      #
                    </th>
                    <th className="px-3 py-3 text-left text-[11px] font-bold uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>
                      Asset
                    </th>
                    {PERILS.map((p) => (
                      <th
                        key={p}
                        className="px-4 py-3 text-left text-[11px] font-bold uppercase tracking-widest cursor-pointer select-none"
                        style={{ color: "var(--text-muted)" }}
                        onClick={() => handleSort(p)}
                      >
                        <span className="flex items-center gap-1">
                          {PERIL_LABELS[p]} <SortIcon k={p} />
                        </span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pageAssets.map((asset, idx) => (
                    <tr
                      key={asset.asset_id}
                      className="border-b cursor-pointer transition-colors hover:bg-[var(--bg-page)]"
                      style={{ borderColor: "var(--border)" }}
                      onClick={() => handleRowClick(asset)}
                    >
                      <td className="px-5 py-3.5 font-mono text-xs" style={{ color: "var(--text-muted)" }}>
                        {(page - 1) * ROWS_PER_PAGE + idx + 1}
                      </td>
                      <td className="px-3 py-3.5">
                        <p className="font-semibold text-sm max-w-[200px] truncate" style={{ color: "var(--text-primary)" }}>{asset.name}</p>
                        <p className="font-mono text-[10px] mt-0.5" style={{ color: "var(--text-muted)" }}>{asset.asset_id}</p>
                      </td>
                      {PERILS.map((p) => (
                        <td key={p} className="px-4 py-3.5">
                          <ScoreCell
                            score={asset.scores[p] != null ? Math.round(asset.scores[p]!) : null}
                            band={p === "all" ? asset.band : undefined}
                          />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            <div className="px-5 py-3 flex items-center justify-between border-t text-sm" style={{ borderColor: "var(--border)" }}>
              <span style={{ color: "var(--text-muted)" }}>
                Rows per page: 10 &nbsp;·&nbsp;
                {(page - 1) * ROWS_PER_PAGE + 1}–{Math.min(page * ROWS_PER_PAGE, sortedAssets.length)} of {sortedAssets.length}
              </span>
              <div className="flex items-center gap-1">
                <button onClick={() => setPage((p) => p - 1)} disabled={page === 1} className="p-1.5 rounded disabled:opacity-30" style={{ color: "var(--text-secondary)" }}>
                  <ChevronLeft size={16} />
                </button>
                <span className="px-2 font-mono text-xs" style={{ color: "var(--text-secondary)" }}>{page} / {totalPages}</span>
                <button onClick={() => setPage((p) => p + 1)} disabled={page >= totalPages} className="p-1.5 rounded disabled:opacity-30" style={{ color: "var(--text-secondary)" }}>
                  <ChevronRight size={16} />
                </button>
              </div>
            </div>
          </>
        )}
      </div>

      <p className="text-[11px]" style={{ color: "var(--text-muted)" }}>
        Asset list sourced from portfolio risk summary · {summary?.top_assets.length ?? 0} assets in portfolio
      </p>
    </div>
  );
}
