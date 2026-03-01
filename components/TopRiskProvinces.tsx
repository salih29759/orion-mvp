"use client";

import { useDashboard } from "@/lib/useDashboard";
import { Province, getRiskColor, getRiskTextColor } from "@/lib/api";

export default function TopRiskProvinces() {
  const { provinces, loading, error } = useDashboard();

  const top = [...provinces]
    .sort((a, b) => b.overall_score - a.overall_score)
    .slice(0, 5);

  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
      {/* Header */}
      <div className="px-5 py-3.5 border-b border-white/8 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-red-400" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M3.293 9.707a1 1 0 010-1.414l6-6a1 1 0 011.414 0l6 6a1 1 0 01-1.414 1.414L11 5.414V17a1 1 0 11-2 0V5.414L4.707 9.707a1 1 0 01-1.414 0z" clipRule="evenodd" />
          </svg>
          <span className="text-sm font-semibold text-white">Top Risk Provinces</span>
        </div>
        <span className="text-xs text-white/40">Ranked by overall score</span>
      </div>

      <div className="divide-y divide-white/5">
        {error ? (
          <div className="px-5 py-8 text-center">
            <p className="text-xs text-red-400 mb-1">⚠ Failed to load provinces</p>
            <p className="text-[11px] text-white/30 font-mono">{error}</p>
          </div>
        ) : loading ? (
          Array.from({ length: 5 }).map((_, i) => <ProvinceSkeleton key={i} />)
        ) : (
          top.map((province, i) => (
            <ProvinceRow key={province.id} province={province} rank={i + 1} />
          ))
        )}
      </div>
    </div>
  );
}

function ProvinceSkeleton() {
  return (
    <div className="px-5 py-3.5 flex items-center gap-4">
      <div className="w-7 h-7 rounded-lg bg-white/5 animate-pulse shrink-0" />
      <div className="flex-1 space-y-1.5">
        <div className="h-3 w-24 bg-white/8 rounded animate-pulse" />
        <div className="h-1.5 w-full bg-white/5 rounded-full animate-pulse" />
      </div>
      <div className="w-12 h-6 rounded-md bg-white/5 animate-pulse shrink-0" />
    </div>
  );
}

function ProvinceRow({ province, rank }: { province: Province; rank: number }) {
  const color = getRiskColor(province.overall_score);
  const textColor = getRiskTextColor(province.risk_level);

  return (
    <div className="px-5 py-3.5 flex items-center gap-4 hover:bg-white/3 transition-colors group">
      {/* Rank */}
      <div
        className="w-7 h-7 rounded-lg flex items-center justify-center text-xs font-bold shrink-0"
        style={{
          background: rank <= 2 ? `${color}18` : "rgba(255,255,255,0.05)",
          color: rank <= 2 ? color : "rgba(255,255,255,0.4)",
          border: rank <= 2 ? `1px solid ${color}30` : "1px solid rgba(255,255,255,0.08)",
        }}
      >
        {rank}
      </div>

      {/* Province info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-white">{province.name}</span>
          <TrendBadge trend={province.trend} pct={province.trend_pct} />
        </div>
        {/* Score bar */}
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 bg-white/8 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all"
              style={{
                width: `${province.overall_score}%`,
                backgroundColor: color,
                boxShadow: `0 0 8px ${color}60`,
              }}
            />
          </div>
          <span className="text-[11px] font-bold w-8 text-right" style={{ color }}>
            {province.overall_score}
          </span>
        </div>
      </div>

      {/* Risk level badge */}
      <div
        className="shrink-0 text-[10px] font-bold px-2 py-1 rounded-md"
        style={{
          color: textColor,
          backgroundColor: `${textColor}12`,
          border: `1px solid ${textColor}30`,
        }}
      >
        {province.risk_level}
      </div>

      {/* Sub scores */}
      <div className="hidden lg:flex items-center gap-3 shrink-0">
        <SubScore label="Flood" value={province.flood_score} color="#3b82f6" />
        <SubScore label="Drought" value={province.drought_score} color="#f59e0b" />
      </div>
    </div>
  );
}

function TrendBadge({ trend, pct }: { trend: string; pct: number }) {
  if (trend === "UP") {
    return (
      <span className="text-[10px] text-red-400 font-semibold flex items-center gap-0.5">
        <span>▲</span>{pct}%
      </span>
    );
  }
  if (trend === "DOWN") {
    return (
      <span className="text-[10px] text-green-400 font-semibold flex items-center gap-0.5">
        <span>▼</span>{pct}%
      </span>
    );
  }
  return <span className="text-[10px] text-white/30">—</span>;
}

function SubScore({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="text-center">
      <div className="text-[10px] text-white/30">{label}</div>
      <div className="text-xs font-bold" style={{ color }}>{value}</div>
    </div>
  );
}
