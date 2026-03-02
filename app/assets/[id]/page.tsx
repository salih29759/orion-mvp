"use client";

import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { useBatchScores } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { BandBadge } from "@/components/ui/BandBadge";
import { SkeletonCard, Skeleton } from "@/components/ui/Skeleton";
import type { BandKey, PerilKey, ScorePoint } from "@/types";

// Load map only client-side (WebGL)
const AssetMap = dynamic(
  () => import("@/components/AssetMap").then((m) => m.AssetMap),
  { ssr: false, loading: () => <Skeleton className="w-full h-full min-h-[260px]" /> }
);

const PERIL_COLOR: Record<string, string> = {
  all:     "#1e6fff",
  heat:    "#f97316",
  rain:    "#3b82f6",
  wind:    "#06b6d4",
  drought: "#d97706",
};

const PERIL_LABEL: Record<string, string> = {
  heat: "Heat", rain: "Rain", wind: "Wind", drought: "Drought",
};

const PERIL_ICON: Record<string, string> = {
  heat: "🌡️", rain: "🌧️", wind: "💨", drought: "🏜️",
};

function ScoreCard({
  peril, score, band,
}: {
  peril: string; score: number | null | undefined; band: BandKey | null | undefined;
}) {
  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5 space-y-2">
      <div className="flex items-center gap-2">
        <span className="text-lg">{PERIL_ICON[peril] ?? "🌐"}</span>
        <span className="text-[11px] text-white/50 uppercase tracking-widest">
          {PERIL_LABEL[peril] ?? peril}
        </span>
      </div>
      <div
        className="text-3xl font-bold tabular-nums"
        style={{ color: score != null ? PERIL_COLOR[peril] : "#ffffff30" }}
      >
        {score ?? "—"}
      </div>
      {band && <BandBadge band={band} />}
    </div>
  );
}

function DriversList({
  drivers,
}: {
  drivers: Partial<Record<PerilKey, string[]>>;
}) {
  const entries = (Object.entries(drivers) as [PerilKey, string[]][]).filter(
    ([, arr]) => arr && arr.length > 0
  );

  if (entries.length === 0) {
    return (
      <div className="text-white/30 text-sm italic">No driver data available</div>
    );
  }

  return (
    <div className="space-y-4">
      {entries.map(([peril, items]) => (
        <div key={peril}>
          <div className="flex items-center gap-2 mb-2">
            <span className="text-sm">{PERIL_ICON[peril]}</span>
            <span
              className="text-xs font-semibold uppercase tracking-widest"
              style={{ color: PERIL_COLOR[peril] }}
            >
              {PERIL_LABEL[peril] ?? peril}
            </span>
          </div>
          <ul className="space-y-1">
            {items.map((item, i) => (
              <li key={i} className="flex items-start gap-2 text-sm text-white/70">
                <span className="text-white/20 mt-0.5">›</span>
                <span>{item}</span>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  );
}

export default function AssetDetailPage() {
  const router = useRouter();
  const { selectedAsset, startDate, endDate } = useGlobalStore();

  const { data, isLoading, error, refetch } = useBatchScores(
    selectedAsset,
    startDate,
    endDate
  );

  // Get the latest score point for the asset
  const series: ScorePoint[] = data?.results?.[0]?.series ?? [];
  const latest: ScorePoint | undefined = series[series.length - 1];

  if (!selectedAsset) {
    return (
      <div className="p-6">
        <div className="bg-[#070f1f] border border-white/8 rounded-xl p-12 text-center max-w-lg mx-auto mt-20">
          <div className="text-4xl mb-4">🏗️</div>
          <div className="text-white/60 font-medium">No asset selected</div>
          <div className="text-white/30 text-sm mt-1 mb-6">
            Navigate to an asset from the Assets table
          </div>
          <button
            onClick={() => router.push("/assets")}
            className="px-5 py-2.5 rounded-lg bg-[#1e6fff]/15 border border-[#1e6fff]/30 text-[#1e6fff] text-sm font-medium hover:bg-[#1e6fff]/25 transition-colors"
          >
            ← Back to Assets
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <button
            onClick={() => router.back()}
            className="flex items-center gap-1.5 text-white/40 hover:text-white text-sm mb-3 transition-colors group"
          >
            <svg className="w-4 h-4 group-hover:-translate-x-0.5 transition-transform" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
            Back to portfolio
          </button>
          <h1 className="text-2xl font-bold text-white tracking-tight">{selectedAsset.name}</h1>
          <p className="text-sm text-white/40 mt-0.5 font-mono">
            {selectedAsset.asset_id} · {selectedAsset.lat.toFixed(4)}, {selectedAsset.lon.toFixed(4)}
          </p>
        </div>
        <div className="text-right">
          <div className="text-[11px] text-white/30">Period</div>
          <div className="text-sm text-white/70 font-mono">{startDate} → {endDate}</div>
        </div>
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

      {/* Score cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {isLoading
          ? Array.from({ length: 4 }).map((_, i) => <SkeletonCard key={i} />)
          : (["heat", "rain", "wind", "drought"] as PerilKey[]).map((peril) => (
              <ScoreCard
                key={peril}
                peril={peril}
                score={latest?.scores?.[peril] ?? null}
                band={latest?.bands?.[peril] ?? null}
              />
            ))}
      </div>

      {/* All-hazards score — large */}
      {!isLoading && latest && (
        <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5 flex items-center gap-6">
          <div>
            <div className="text-[11px] text-white/50 uppercase tracking-widest mb-1">All Hazards</div>
            <div className="text-5xl font-bold tabular-nums" style={{ color: "#1e6fff" }}>
              {latest.scores.all ?? "—"}
            </div>
          </div>
          {latest.bands.all && (
            <div>
              <div className="text-[11px] text-white/40 mb-1">Risk band</div>
              <BandBadge band={latest.bands.all} />
            </div>
          )}
          <div className="ml-auto text-right">
            <div className="text-[11px] text-white/30">Last data point</div>
            <div className="text-sm text-white/60 font-mono mt-0.5">{latest.date}</div>
          </div>
        </div>
      )}

      {/* Main content grid */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        {/* Drivers / Explainability */}
        <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
          <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-4">
            Risk Drivers — Latest Period
          </div>
          {isLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <Skeleton key={i} className="h-3 w-full" />
              ))}
            </div>
          ) : !latest?.drivers || Object.keys(latest.drivers).length === 0 ? (
            <div className="text-white/30 text-sm italic">No driver data available for this period</div>
          ) : (
            <DriversList drivers={latest.drivers} />
          )}
        </div>

        {/* Map */}
        <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden" style={{ minHeight: 280 }}>
          <AssetMap lat={selectedAsset.lat} lon={selectedAsset.lon} name={selectedAsset.name} />
        </div>
      </div>

      {/* Time series chart */}
      <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
        <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-4">
          Score Time Series
        </div>
        {isLoading ? (
          <Skeleton className="h-52 w-full" />
        ) : series.length === 0 ? (
          <div className="h-52 flex items-center justify-center text-white/30 text-sm">
            No time series data available
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={series}>
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
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(value: any, name: any) => [value, PERIL_LABEL[name] ?? name]}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: "#ffffff60" }} />
              {(["all", "heat", "rain", "wind", "drought"] as const).map((k) => (
                <Line
                  key={k}
                  type="monotone"
                  dataKey={`scores.${k}`}
                  name={k}
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
  );
}
