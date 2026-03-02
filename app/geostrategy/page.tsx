"use client";

import dynamic from "next/dynamic";
import { useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import { ChevronDown, ChevronUp, ChevronLeft, ChevronRight, Download } from "lucide-react";
import { PROVINCE_RISK, type ProvinceRisk } from "@/lib/turkeyRiskData";
import { ScoreCell, BAND_COLOR } from "@/components/ui/BandBadge";
import { SkeletonMap } from "@/components/ui/Skeleton";
import toast from "react-hot-toast";

const TurkeyMap = dynamic(
  () => import("@/components/map/TurkeyMap").then((m) => m.TurkeyMap),
  { ssr: false, loading: () => <SkeletonMap /> }
);

function scoreToBand(score: number): "minimal" | "minor" | "moderate" | "major" | "extreme" {
  if (score < 20) return "minimal";
  if (score < 40) return "minor";
  if (score < 60) return "moderate";
  if (score < 80) return "major";
  return "extreme";
}

// Mock active alerts for map sidebar
const MOCK_ALERTS = [
  { province: "Şanlıurfa", type: "Extreme Heat",    severity: "high",   ago: "2h" },
  { province: "Diyarbakır", type: "Drought Alert",  severity: "high",   ago: "5h" },
  { province: "Rize",       type: "Heavy Rainfall", severity: "medium", ago: "8h" },
  { province: "İzmir",      type: "Heat Wave",      severity: "medium", ago: "12h" },
  { province: "Ağrı",       type: "Cold Snap",      severity: "low",    ago: "1d" },
];

const SEVERITY_COLOR: Record<string, string> = {
  high:   "var(--extreme)",
  medium: "var(--moderate)",
  low:    "var(--minimal)",
};

const ROWS_PER_PAGE = 10;

type SortKey = "risk" | "heat" | "rain" | "drought" | "pop_change" | "gdp_change";

export default function GeostrategyPage() {
  const router = useRouter();
  const [search, setSearch]       = useState("");
  const [sortKey, setSortKey]     = useState<SortKey>("risk");
  const [sortAsc, setSortAsc]     = useState(false);
  const [page, setPage]           = useState(1);
  const [activeProvince, setActiveProvince] = useState<ProvinceRisk | null>(null);

  const sorted: ProvinceRisk[] = useMemo(() => {
    return PROVINCE_RISK
      .filter((p) =>
        p.nameEn.toLowerCase().includes(search.toLowerCase()) ||
        p.name.toLowerCase().includes(search.toLowerCase())
      )
      .sort((a, b) => {
        const diff = (a[sortKey] as number) - (b[sortKey] as number);
        return sortAsc ? diff : -diff;
      });
  }, [search, sortKey, sortAsc]);

  const totalPages = Math.ceil(sorted.length / ROWS_PER_PAGE);
  const pageData   = sorted.slice((page - 1) * ROWS_PER_PAGE, page * ROWS_PER_PAGE);

  function handleSort(k: SortKey) {
    if (k === sortKey) setSortAsc((v) => !v);
    else { setSortKey(k); setSortAsc(false); }
    setPage(1);
  }

  function handleProvinceClick(p: ProvinceRisk) {
    setActiveProvince(p);
    toast(`Opening ${p.nameEn} report — coming soon`, { icon: "🏛" });
  }

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey === k
      ? sortAsc ? <ChevronUp size={12} /> : <ChevronDown size={12} />
      : <ChevronDown size={12} className="opacity-30" />;

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto">
      {/* Controls row */}
      <div className="flex flex-wrap items-center gap-3">
        {[
          { label: "Geographic Level", options: ["Province", "District"] },
          { label: "Time Horizon",     options: ["30 years", "50 years", "Current"] },
          { label: "Scenario",         options: ["SSP 2-4.5", "SSP 5-8.5", "SSP 1-2.6"] },
        ].map((ctrl) => (
          <select
            key={ctrl.label}
            className="text-sm rounded-lg px-3 py-2 border outline-none"
            style={{ backgroundColor: "var(--bg-surface)", borderColor: "var(--border)", color: "var(--text-primary)" }}
          >
            {ctrl.options.map((o) => <option key={o}>{o === ctrl.options[0] ? `${ctrl.label}: ` : ""}{o}</option>)}
          </select>
        ))}
        <button
          onClick={() => { /* export */ }}
          className="ml-auto flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border transition-colors"
          style={{ backgroundColor: "var(--bg-surface)", borderColor: "var(--border)", color: "var(--accent)" }}
        >
          <Download size={15} /> Export
        </button>
      </div>

      {/* Map + Alerts sidebar */}
      <div className="flex gap-5">
        {/* Map */}
        <div
          className="flex-1 rounded-xl overflow-hidden"
          style={{ border: "1px solid var(--border)" }}
        >
          <TurkeyMap onProvinceClick={handleProvinceClick} />
        </div>

        {/* Alerts sidebar */}
        <div
          className="w-[260px] shrink-0 rounded-xl flex flex-col"
          style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
        >
          <div className="px-4 py-3 border-b flex items-center justify-between" style={{ borderColor: "var(--border)" }}>
            <p className="text-xs font-bold uppercase tracking-wider" style={{ color: "var(--text-secondary)" }}>Active Alerts</p>
            <span className="flex items-center gap-1 text-[10px] font-semibold text-green-600">
              <span className="w-1.5 h-1.5 rounded-full bg-green-500 pulse-dot" />
              LIVE
            </span>
          </div>
          <div className="flex-1 overflow-y-auto divide-y" style={{ borderColor: "var(--border)" }}>
            {MOCK_ALERTS.map((alert, i) => (
              <div key={i} className="px-4 py-3">
                <div className="flex items-center justify-between mb-1">
                  <span
                    className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded border"
                    style={{ color: SEVERITY_COLOR[alert.severity], borderColor: SEVERITY_COLOR[alert.severity] + "60" }}
                  >
                    {alert.severity}
                  </span>
                  <span className="text-[10px]" style={{ color: "var(--text-muted)" }}>{alert.ago} ago</span>
                </div>
                <p className="text-xs font-semibold" style={{ color: "var(--text-primary)" }}>{alert.province}</p>
                <p className="text-[11px]" style={{ color: "var(--text-secondary)" }}>{alert.type}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Macroeconomic data table */}
      <div
        className="rounded-xl overflow-hidden"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <div className="px-5 py-4 border-b flex flex-wrap items-center gap-3" style={{ borderColor: "var(--border)" }}>
          <p className="text-xs font-bold uppercase tracking-wider" style={{ color: "var(--text-secondary)" }}>
            Macroeconomic Data
          </p>
          <div
            className="flex items-center gap-1.5 rounded-lg border px-2.5 py-1.5 ml-auto"
            style={{ borderColor: "var(--border)", backgroundColor: "var(--bg-page)" }}
          >
            <svg className="w-3.5 h-3.5" fill="none" stroke="var(--text-muted)" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              value={search}
              onChange={(e) => { setSearch(e.target.value); setPage(1); }}
              placeholder="Search province…"
              className="bg-transparent outline-none text-xs w-36"
              style={{ color: "var(--text-primary)" }}
            />
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b" style={{ borderColor: "var(--border)", backgroundColor: "var(--bg-page)" }}>
                {[
                  { label: "Province",          key: null },
                  { label: "Economic Factor",   key: "risk" as SortKey },
                  { label: "Heat",              key: "heat" as SortKey },
                  { label: "Rain",              key: "rain" as SortKey },
                  { label: "Drought",           key: "drought" as SortKey },
                  { label: "Population",        key: "pop_change" as SortKey },
                  { label: "GDP Impact",        key: "gdp_change" as SortKey },
                ].map((col) => (
                  <th
                    key={col.label}
                    className="px-5 py-3 text-left text-[11px] font-bold uppercase tracking-widest"
                    style={{ color: "var(--text-muted)", cursor: col.key ? "pointer" : "default" }}
                    onClick={() => col.key && handleSort(col.key)}
                  >
                    <span className="flex items-center gap-1">
                      {col.label}
                      {col.key && <SortIcon k={col.key} />}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pageData.map((province) => {
                const band = scoreToBand(province.risk);
                const popColor = province.pop_change >= 0 ? "var(--minimal)" : "var(--major)";
                const gdpColor = province.gdp_change >= 0 ? "var(--minimal)" : "var(--major)";
                return (
                  <tr
                    key={province.nameEn}
                    className="border-b cursor-pointer transition-colors hover:bg-[var(--bg-page)]"
                    style={{ borderColor: "var(--border)", backgroundColor: activeProvince?.nameEn === province.nameEn ? "rgba(27,58,75,0.05)" : undefined }}
                    onClick={() => handleProvinceClick(province)}
                  >
                    <td className="px-5 py-3.5 font-semibold" style={{ color: "var(--text-primary)" }}>
                      {province.nameEn}
                    </td>
                    <td className="px-5 py-3.5">
                      <ScoreCell score={province.risk} band={band} />
                    </td>
                    <td className="px-5 py-3.5">
                      <ScoreCell score={province.heat} />
                    </td>
                    <td className="px-5 py-3.5">
                      <ScoreCell score={province.rain} />
                    </td>
                    <td className="px-5 py-3.5">
                      <ScoreCell score={province.drought} />
                    </td>
                    <td className="px-5 py-3.5">
                      <span className="font-mono text-sm font-bold" style={{ color: popColor }}>
                        {province.pop_change >= 0 ? "+" : ""}{province.pop_change.toFixed(1)}%
                      </span>
                    </td>
                    <td className="px-5 py-3.5">
                      <span className="font-mono text-sm font-bold" style={{ color: gdpColor }}>
                        {province.gdp_change >= 0 ? "+" : ""}{province.gdp_change.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div className="px-5 py-3 flex items-center justify-between border-t text-sm" style={{ borderColor: "var(--border)" }}>
          <span style={{ color: "var(--text-muted)" }}>
            {(page - 1) * ROWS_PER_PAGE + 1}–{Math.min(page * ROWS_PER_PAGE, sorted.length)} of {sorted.length} provinces
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
      </div>
    </div>
  );
}
