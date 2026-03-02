"use client";

import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { useState } from "react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine,
} from "recharts";
import { ChevronRight } from "lucide-react";
import { useBatchScores } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { ScoreDisplay, BAND_COLOR, PERIL_COLOR } from "@/components/ui/BandBadge";
import { PerilChip } from "@/components/ui/PerilChip";
import { Skeleton, SkeletonCard } from "@/components/ui/Skeleton";
import type { BandKey, PerilKey, AllPerilKey, ScoreSeriesPoint } from "@/types";

const AssetMap = dynamic(
  () => import("@/components/AssetMap").then((m) => m.AssetMap),
  { ssr: false, loading: () => <Skeleton className="w-full min-h-[280px]" /> }
);

const PERILS: PerilKey[] = ["heat", "rain", "wind", "drought"];
const ALL_PERILS: AllPerilKey[] = ["all", "heat", "rain", "wind", "drought"];

const PERIL_LABEL: Record<string, string> = {
  all: "All Hazards", heat: "Heat", rain: "Rain", wind: "Wind", drought: "Drought",
};
const PERIL_COLOR_HEX: Record<string, string> = {
  all: "#1B3A4B", heat: "#E05520", rain: "#2460C8", wind: "#6B40B0", drought: "#C09020",
};
const PERIL_ICON: Record<string, string> = {
  heat: "🌡️", rain: "🌧️", wind: "💨", drought: "🏜️", wildfire: "🔥",
};

const BAND_THRESHOLDS = [
  { y: 20, label: "Minor",    color: "#C8C84A" },
  { y: 40, label: "Moderate", color: "#E8903A" },
  { y: 60, label: "Major",    color: "#D44A2A" },
  { y: 80, label: "Extreme",  color: "#AA1A1A" },
];

export default function AssetDetailPage() {
  const router = useRouter();
  const { selectedAsset, startDate, endDate } = useGlobalStore();
  const { data, isLoading, error, refetch } = useBatchScores(selectedAsset, startDate, endDate);
  const [trendPeril, setTrendPeril] = useState<AllPerilKey>("all");

  const series: ScoreSeriesPoint[] = data?.results?.[0]?.series ?? [];
  const latest: ScoreSeriesPoint | undefined = series[series.length - 1];

  if (!selectedAsset) {
    return (
      <div className="rounded-2xl p-16 text-center max-w-md mx-auto mt-16"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
        <div className="text-4xl mb-4">🏗️</div>
        <p className="font-serif text-lg" style={{ color: "var(--text-primary)" }}>No asset selected</p>
        <p className="text-sm mt-1 mb-6" style={{ color: "var(--text-muted)" }}>Navigate from the Assets table</p>
        <button onClick={() => router.push("/assets")} className="px-5 py-2.5 rounded-lg text-sm font-medium text-white" style={{ backgroundColor: "var(--accent)" }}>
          ← Back to Assets
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto">
      {/* Breadcrumb */}
      <div className="flex items-center gap-1.5 text-sm" style={{ color: "var(--text-muted)" }}>
        <button onClick={() => router.push("/assets")} className="hover:underline">Assets</button>
        <ChevronRight size={14} />
        <span style={{ color: "var(--text-primary)" }}>{selectedAsset.name}</span>
      </div>

      {/* Error */}
      {error && (
        <div className="rounded-xl p-5 flex items-center justify-between"
          style={{ backgroundColor: "rgba(170,26,26,0.06)", border: "1px solid rgba(170,26,26,0.2)" }}>
          <span className="text-sm" style={{ color: "var(--extreme)" }}>{(error as Error).message}</span>
          <button onClick={() => refetch()} className="text-xs px-3 py-1.5 rounded"
            style={{ backgroundColor: "rgba(170,26,26,0.1)", color: "var(--extreme)" }}>Retry</button>
        </div>
      )}

      {/* HERO: All Hazards */}
      <div className="rounded-xl p-6" style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
        <p className="text-xs font-bold uppercase tracking-widest mb-3" style={{ color: "var(--text-muted)" }}>
          All Hazards Factor (Current year)
        </p>
        {isLoading ? <Skeleton className="h-20 w-48" /> : (
          <ScoreDisplay
            score={latest?.scores?.all != null ? Math.round(latest.scores.all) : null}
            band={latest?.bands?.all ?? undefined}
            size="xl"
          />
        )}

        <div className="mt-6 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b" style={{ borderColor: "var(--border)" }}>
                {["Hazard", "Factor", "Exposure", "Damage & Cost"].map((h) => (
                  <th key={h} className="pb-2 text-left text-[11px] font-bold uppercase tracking-widest pr-8" style={{ color: "var(--text-muted)" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {isLoading
                ? Array.from({ length: 4 }).map((_, i) => (
                    <tr key={i} className="border-b" style={{ borderColor: "var(--border)" }}>
                      {Array.from({ length: 4 }).map((__, j) => (
                        <td key={j} className="py-3 pr-8"><Skeleton className="h-4 w-24" /></td>
                      ))}
                    </tr>
                  ))
                : ([...PERILS, "wildfire" as const]).map((peril) => {
                    const score = latest?.scores?.[peril as AllPerilKey];
                    const band  = latest?.bands?.[peril as AllPerilKey];
                    const drivers = latest?.drivers?.[peril as PerilKey] ?? [];
                    const exposure = drivers[0] ?? "—";
                    const isComingSoon = peril === "wildfire";
                    return (
                      <tr key={peril} className="border-b" style={{ borderColor: "var(--border)" }}>
                        <td className="py-3 pr-8">
                          <span className="flex items-center gap-1.5">
                            <span>{PERIL_ICON[peril] ?? "🌐"}</span>
                            <span className="font-medium capitalize">{peril}</span>
                          </span>
                        </td>
                        <td className="py-3 pr-8">
                          {isComingSoon ? (
                            <span className="text-xs italic" style={{ color: "var(--text-muted)" }}>Coming soon</span>
                          ) : (
                            <div className="flex items-baseline gap-1">
                              <span style={{ color: band ? BAND_COLOR[band as BandKey] : "var(--text-muted)" }} className="text-sm">■</span>
                              <span className="font-mono font-bold" style={{ color: band ? BAND_COLOR[band as BandKey] : "var(--text-muted)" }}>
                                {score != null ? Math.round(score) : "—"}/100
                              </span>
                              {band && <span className="text-xs capitalize ml-1" style={{ color: BAND_COLOR[band as BandKey] }}>{band}</span>}
                            </div>
                          )}
                        </td>
                        <td className="py-3 pr-8 max-w-[220px]">
                          <span className="text-xs" style={{ color: "var(--text-secondary)" }}>{isComingSoon ? "—" : exposure}</span>
                        </td>
                        <td className="py-3">
                          <span className="text-xs italic" style={{ color: "var(--text-muted)" }}>N/A</span>
                        </td>
                      </tr>
                    );
                  })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Peril score cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {isLoading
          ? Array.from({ length: 4 }).map((_, i) => <SkeletonCard key={i} />)
          : PERILS.map((peril) => {
              const score = latest?.scores?.[peril];
              const band  = latest?.bands?.[peril];
              return (
                <div key={peril} className="rounded-xl p-5"
                  style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)", borderLeft: `3px solid ${PERIL_COLOR_HEX[peril]}` }}>
                  <div className="flex items-center gap-1.5 mb-3">
                    <span>{PERIL_ICON[peril]}</span>
                    <span className="text-[10px] font-bold uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>{PERIL_LABEL[peril]}</span>
                  </div>
                  <ScoreDisplay score={score != null ? Math.round(score) : null} band={band ?? undefined} size="sm" />
                </div>
              );
            })}
      </div>

      {/* Drivers + Map */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        <div className="rounded-xl p-5" style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
          <p className="text-xs font-bold uppercase tracking-wider mb-4" style={{ color: "var(--text-secondary)" }}>Risk Drivers</p>
          {isLoading ? (
            <div className="space-y-3">{Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-16 w-full" />)}</div>
          ) : !latest?.drivers || Object.keys(latest.drivers).length === 0 ? (
            <p className="text-sm italic" style={{ color: "var(--text-muted)" }}>No driver data available for this period</p>
          ) : (
            <div className="space-y-4">
              {(Object.entries(latest.drivers) as [PerilKey, string[]][])
                .filter(([, items]) => items?.length > 0)
                .map(([peril, items]) => (
                  <details key={peril} open>
                    <summary className="flex items-center gap-2 cursor-pointer list-none py-2 border-b" style={{ borderColor: "var(--border)" }}>
                      <span>{PERIL_ICON[peril]}</span>
                      <span className="text-xs font-bold uppercase tracking-widest" style={{ color: PERIL_COLOR_HEX[peril] }}>{PERIL_LABEL[peril]}</span>
                      {latest.scores?.[peril] != null && (
                        <span className="ml-auto text-xs font-mono" style={{ color: PERIL_COLOR_HEX[peril] }}>
                          ■ {Math.round(latest.scores[peril]!)}/100 {latest.bands?.[peril]}
                        </span>
                      )}
                    </summary>
                    <ul className="mt-2 space-y-1.5 pl-5">
                      {items.map((item, i) => (
                        <li key={i} className="text-sm flex gap-2" style={{ color: "var(--text-secondary)" }}>
                          <span style={{ color: "var(--text-muted)" }}>›</span>
                          <span>{item}</span>
                        </li>
                      ))}
                    </ul>
                  </details>
                ))}
            </div>
          )}
        </div>

        <div className="rounded-xl overflow-hidden" style={{ border: "1px solid var(--border)", minHeight: 280 }}>
          <AssetMap
            lat={selectedAsset.lat}
            lon={selectedAsset.lon}
            name={selectedAsset.name}
            score={latest?.scores?.all != null ? Math.round(latest.scores.all) : null}
            band={latest?.bands?.all ?? null}
          />
        </div>
      </div>

      {/* Time series */}
      <div className="rounded-xl p-5" style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
        <div className="flex items-center justify-between flex-wrap gap-2 mb-4">
          <p className="text-xs font-bold uppercase tracking-wider" style={{ color: "var(--text-secondary)" }}>
            Risk History — {PERIL_LABEL[trendPeril]}
          </p>
          <div className="flex gap-1.5 flex-wrap">
            {ALL_PERILS.map((p) => (
              <PerilChip key={p} peril={p} active={trendPeril === p} onClick={() => setTrendPeril(p)} />
            ))}
          </div>
        </div>
        {isLoading ? <Skeleton className="h-52 w-full" /> : series.length === 0 ? (
          <div className="h-52 flex items-center justify-center text-sm" style={{ color: "var(--text-muted)" }}>No time series data</div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={series}>
              <defs>
                {ALL_PERILS.map((p) => (
                  <linearGradient key={p} id={`ag-${p}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={PERIL_COLOR_HEX[p]} stopOpacity={0.2} />
                    <stop offset="95%" stopColor={PERIL_COLOR_HEX[p]} stopOpacity={0} />
                  </linearGradient>
                ))}
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              {BAND_THRESHOLDS.map((t) => (
                <ReferenceLine key={t.y} y={t.y} stroke={t.color} strokeDasharray="4 4" strokeWidth={1}
                  label={{ value: t.label, position: "right", fontSize: 9, fill: t.color }} />
              ))}
              <XAxis dataKey="date" tick={{ fill: "var(--text-muted)", fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={(v: string) => v.slice(5)} />
              <YAxis domain={[0, 100]} tick={{ fill: "var(--text-muted)", fontSize: 11 }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8 }}
                labelStyle={{ color: "var(--text-primary)" }}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => [typeof value === "number" ? `${Math.round(value)}/100` : value, PERIL_LABEL[name] ?? name]}
              />
              {ALL_PERILS.filter((p) => trendPeril === "all" || p === trendPeril || p === "all").map((k) => (
                <Area key={k} type="monotone" dataKey={`scores.${k}`} name={k}
                  stroke={PERIL_COLOR_HEX[k]} strokeWidth={k === "all" ? 2.5 : 1.5}
                  fill={`url(#ag-${k})`} dot={false} strokeOpacity={k === "all" ? 1 : 0.8} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Flood placeholder */}
      <div className="rounded-xl p-5 relative overflow-hidden" style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
        <p className="text-xs font-bold uppercase tracking-wider mb-1" style={{ color: "var(--text-secondary)" }}>Flood Damage Likelihood</p>
        <p className="text-xs mb-4" style={{ color: "var(--text-muted)" }}>Coming soon — loss engine required</p>
        <div className="opacity-20 pointer-events-none">
          <ResponsiveContainer width="100%" height={120}>
            <AreaChart data={[
              { x: "20%", gross: 3.2 }, { x: "5%", gross: 8.1 }, { x: "1%", gross: 14.5 },
              { x: "0.5%", gross: 19.2 }, { x: "0.2%", gross: 24.8 },
            ]}>
              <XAxis dataKey="x" tick={{ fontSize: 9, fill: "var(--text-muted)" }} />
              <YAxis tick={{ fontSize: 9, fill: "var(--text-muted)" }} tickFormatter={(v: number) => `$${v}M`} />
              <Area type="monotone" dataKey="gross" stroke="var(--flood)" fill="var(--flood)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        <div className="absolute inset-0 flex items-center justify-center rounded-xl" style={{ backgroundColor: "rgba(245,243,238,0.75)", backdropFilter: "blur(2px)" }}>
          <div className="text-center">
            <p className="font-serif text-base" style={{ color: "var(--text-primary)" }}>Loss Engine Integration Required</p>
            <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>Connect ERA5 data + fragility curves to activate</p>
            <button className="mt-3 text-xs px-4 py-1.5 rounded-lg border" style={{ borderColor: "var(--border)", color: "var(--text-secondary)" }}>View Roadmap →</button>
          </div>
        </div>
      </div>
    </div>
  );
}
