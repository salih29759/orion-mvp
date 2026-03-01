"use client";
import { useState } from "react";
import { useDashboard } from "@/lib/useDashboard";
import { Province, getRiskColor } from "@/lib/api";

// ── Geographic projection ─────────────────────────────────────────────────────
const LON_MIN = 25.3;
const LAT_MAX = 42.5;
const X_SCALE = 46.9;
const Y_SCALE = 61.0;
const PAD = 20;

function proj(lon: number, lat: number): [number, number] {
  return [(lon - LON_MIN) * X_SCALE + PAD, (LAT_MAX - lat) * Y_SCALE + PAD];
}

function toPath(coords: [number, number][]): string {
  return (
    coords
      .map(([lon, lat], i) => {
        const [x, y] = proj(lon, lat);
        return `${i === 0 ? "M" : "L"} ${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ") + " Z"
  );
}

// ── Turkey border data ────────────────────────────────────────────────────────
const THRACE: [number, number][] = [
  [26.05, 41.73], [26.32, 41.75], [26.70, 41.98], [27.26, 42.07],
  [27.70, 41.97], [28.11, 41.83], [28.30, 41.50],
  [28.97, 41.10],
  [28.85, 40.98], [28.40, 40.98], [27.90, 41.00], [27.30, 41.00], [26.90, 41.00],
  [26.72, 40.82], [26.65, 40.52], [26.75, 40.28], [26.71, 40.18],
  [26.55, 40.15], [26.40, 40.25],
  [26.14, 40.44], [26.00, 40.72], [26.05, 40.97], [26.05, 41.35],
];

const ANATOLIA: [number, number][] = [
  [29.10, 41.05], [29.55, 41.20], [29.80, 41.37], [30.41, 41.32],
  [31.21, 41.42], [31.92, 41.62], [32.69, 42.00], [33.45, 41.99],
  [34.10, 41.95], [34.80, 41.88], [35.57, 42.02], [36.41, 41.81],
  [37.14, 41.46], [37.82, 41.32], [38.59, 41.28], [39.51, 41.17],
  [40.38, 40.96], [41.02, 40.77], [41.55, 41.52],
  [42.78, 41.46], [43.37, 41.10], [43.65, 40.46], [44.04, 40.04],
  [44.41, 39.72], [44.79, 39.70], [44.79, 39.11], [44.77, 38.60],
  [44.58, 38.22], [44.22, 37.83], [44.29, 37.04], [43.96, 37.35],
  [43.24, 37.33], [42.92, 37.32], [42.43, 37.11], [42.17, 37.21],
  [41.54, 37.11], [40.96, 37.11], [40.68, 37.14], [40.09, 37.07],
  [39.68, 36.95], [38.94, 36.89], [38.47, 37.14], [38.10, 37.19],
  [37.66, 37.10], [37.22, 37.07], [36.98, 36.91], [36.67, 36.81],
  [36.41, 36.78], [36.15, 36.12],
  [35.88, 36.43], [35.64, 36.52], [35.26, 36.79], [34.98, 36.78],
  [34.56, 36.56], [34.14, 36.49], [33.72, 36.18], [33.41, 36.11],
  [33.00, 36.13], [32.53, 36.10], [32.15, 36.46], [31.58, 36.75],
  [31.34, 37.00], [30.84, 36.90], [30.55, 36.77], [30.17, 36.51],
  [29.69, 36.45], [29.19, 36.72], [28.96, 36.90], [28.42, 37.05],
  [28.22, 37.25], [27.57, 37.17], [27.02, 37.34], [26.69, 37.47],
  [26.40, 37.60],
  [26.30, 37.85], [26.55, 38.10], [26.65, 38.34], [26.59, 38.75],
  [26.17, 38.97], [25.96, 39.23], [26.05, 39.61], [26.37, 39.89],
  [26.62, 40.10], [26.70, 40.33],
  [26.84, 40.12], [27.12, 40.04], [27.50, 40.10], [27.95, 40.22],
  [28.05, 40.38], [28.55, 40.52], [29.00, 40.72],
];

// ── Province boundary polygons ────────────────────────────────────────────────
// Keys map to plate numbers via BOUNDARY_PLATE below
const PROVINCE_BOUNDS: Record<string, [number, number][]> = {
  IST_EU: [
    [28.02, 41.55], [28.30, 41.57], [28.58, 41.53], [28.97, 41.25],
    [28.97, 41.08], [28.62, 41.00], [28.25, 41.05], [28.00, 41.20], [27.98, 41.45],
  ],
  IST_AS: [
    [29.10, 41.05], [29.92, 41.15], [29.85, 40.78], [29.30, 40.70], [29.05, 40.73],
  ],
  ANK: [
    [31.55, 40.60], [32.30, 40.92], [33.30, 40.82], [33.80, 40.70],
    [33.80, 39.25], [32.60, 38.75], [31.55, 38.92],
  ],
  IZM: [
    [26.35, 39.15], [27.20, 39.30], [27.60, 39.30], [28.18, 39.10],
    [28.30, 38.50], [27.80, 38.10], [27.50, 37.85], [26.70, 37.80],
    [26.35, 38.35], [26.18, 38.77],
  ],
  ANT: [
    [29.50, 37.55], [30.10, 37.70], [30.65, 37.80], [31.35, 36.95],
    [31.90, 37.20], [32.50, 37.20], [32.55, 36.55], [32.20, 36.30],
    [29.85, 36.10], [29.19, 36.72], [29.50, 37.25],
  ],
  BUR: [
    [28.60, 40.70], [29.55, 40.72], [30.08, 40.55], [30.08, 40.10],
    [30.05, 39.82], [29.20, 39.65], [28.92, 39.65], [28.40, 39.85], [28.55, 40.52],
  ],
  ADA: [
    [35.05, 38.05], [35.85, 38.10], [36.20, 37.60], [36.20, 37.10],
    [36.15, 36.55], [36.15, 36.15], [35.05, 36.45], [34.56, 36.57],
    [34.80, 37.30], [35.05, 38.05],
  ],
  KON: [
    [31.55, 39.28], [33.00, 39.28], [33.60, 39.00], [34.50, 38.80],
    [34.50, 37.50], [34.00, 37.00], [32.55, 36.95], [31.35, 36.98], [31.55, 38.00],
  ],
  GAZ: [
    [36.75, 37.65], [37.40, 37.70], [37.70, 37.70], [37.85, 37.10],
    [37.50, 36.90], [37.15, 36.85], [36.80, 36.95],
  ],
  KAY: [
    [34.85, 39.35], [35.80, 39.55], [36.50, 39.25], [36.80, 38.80],
    [36.50, 38.10], [35.55, 37.80], [34.85, 38.15],
  ],
  TRA: [
    [39.00, 41.20], [39.60, 41.22], [40.08, 41.20], [40.50, 41.15],
    [40.38, 40.72], [40.10, 40.35], [39.60, 40.20], [39.00, 40.55],
  ],
};

// Maps boundary key → province plate number (which is the backend's document ID)
const BOUNDARY_PLATE: Record<string, string> = {
  IST_EU: "34",
  IST_AS: "34",
  ANK:    "6",
  IZM:    "35",
  ANT:    "7",
  BUR:    "16",
  ADA:    "1",
  KON:    "42",
  GAZ:    "27",
  KAY:    "38",
  TRA:    "61",
};

// ── Types ─────────────────────────────────────────────────────────────────────
interface TooltipState {
  province: Province | null;
  x: number;
  y: number;
}

// ── Component ─────────────────────────────────────────────────────────────────
export default function TurkeyRiskMap() {
  const { provinces, loading, error } = useDashboard();
  const [tooltip, setTooltip] = useState<TooltipState>({ province: null, x: 0, y: 0 });
  const [selected, setSelected] = useState<string | null>(null);

  // Lookup by plate ID (backend ID)
  const provinceMap = Object.fromEntries(provinces.map((p) => [p.id, p]));

  const handleHover = (province: Province, e: React.MouseEvent) => {
    const svg = (e.currentTarget as SVGElement).closest("svg");
    const rect = svg?.getBoundingClientRect();
    if (!rect) return;
    setSelected(province.id);
    setTooltip({ province, x: e.clientX - rect.left, y: e.clientY - rect.top });
  };

  const clearHover = () => {
    setSelected(null);
    setTooltip({ province: null, x: 0, y: 0 });
  };

  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
      {/* Panel header */}
      <div className="flex items-center justify-between px-5 py-3.5 border-b border-white/8">
        <div className="flex items-center gap-3">
          <div className="w-2 h-2 rounded-full bg-cyan-400 pulse-dot" />
          <span className="text-sm font-semibold text-white">Turkey — Province Risk Map</span>
          <span className="text-xs text-white/35">
            {loading ? "Loading data…" : error ? "Error loading data" : "Hover provinces for risk details"}
          </span>
        </div>
        <div className="hidden sm:flex items-center gap-4 text-[11px]">
          <LegendDot color="#ef4444" label="High Risk (75–100)" />
          <LegendDot color="#f97316" label="Medium (50–74)" />
          <LegendDot color="#22c55e" label="Low Risk (0–49)" />
        </div>
      </div>

      {/* Map wrapper */}
      <div className="relative" style={{ paddingBottom: "48%" }}>
        <svg
          viewBox="0 0 960 440"
          className="absolute inset-0 w-full h-full"
          xmlns="http://www.w3.org/2000/svg"
          onMouseLeave={clearHover}
        >
          <defs>
            <linearGradient id="land-grad" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#0f2445" />
              <stop offset="100%" stopColor="#0a1c38" />
            </linearGradient>
            <radialGradient id="sea-grad" cx="40%" cy="40%">
              <stop offset="0%" stopColor="#091730" />
              <stop offset="100%" stopColor="#050e1f" />
            </radialGradient>
            <filter id="glow-h" x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur in="SourceGraphic" stdDeviation="5" result="b" />
              <feColorMatrix in="b" type="matrix"
                values="1 0 0 0 0.94  0 0 0 0 0.27  0 0 0 0 0.27  0 0 0 0.6 0" result="c" />
              <feMerge><feMergeNode in="c" /><feMergeNode in="SourceGraphic" /></feMerge>
            </filter>
            <filter id="glow-m" x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur in="SourceGraphic" stdDeviation="5" result="b" />
              <feColorMatrix in="b" type="matrix"
                values="1 0 0 0 0.98  0 0 0 0 0.45  0 0 0 0 0.09  0 0 0 0.6 0" result="c" />
              <feMerge><feMergeNode in="c" /><feMergeNode in="SourceGraphic" /></feMerge>
            </filter>
            <filter id="glow-l" x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur in="SourceGraphic" stdDeviation="5" result="b" />
              <feColorMatrix in="b" type="matrix"
                values="0 0 0 0 0.13  0 0 0 0 0.77  0 0 0 0 0.37  0 0 0 0.6 0" result="c" />
              <feMerge><feMergeNode in="c" /><feMergeNode in="SourceGraphic" /></feMerge>
            </filter>
          </defs>

          {/* Sea background */}
          <rect width="960" height="440" fill="url(#sea-grad)" />

          {/* Graticule grid */}
          {[37, 38, 39, 40, 41, 42].map((lat) => {
            const [, y] = proj(25, lat);
            return <line key={lat} x1="0" y1={y.toFixed(0)} x2="960" y2={y.toFixed(0)}
              stroke="rgba(30,111,255,0.06)" strokeWidth="1" />;
          })}
          {[26, 28, 30, 32, 34, 36, 38, 40, 42, 44].map((lon) => {
            const [x] = proj(lon, 42);
            return <line key={lon} x1={x.toFixed(0)} y1="0" x2={x.toFixed(0)} y2="440"
              stroke="rgba(30,111,255,0.06)" strokeWidth="1" />;
          })}

          {/* Graticule labels */}
          {[37, 39, 41].map((lat) => {
            const [, y] = proj(25.4, lat);
            return (
              <text key={lat} x="22" y={y.toFixed(0)} fill="rgba(255,255,255,0.18)"
                fontSize="8" fontFamily="monospace" dominantBaseline="middle">
                {lat}°N
              </text>
            );
          })}
          {[28, 32, 36, 40, 44].map((lon) => {
            const [x] = proj(lon, 35.85);
            return (
              <text key={lon} x={x.toFixed(0)} y="430" fill="rgba(255,255,255,0.18)"
                fontSize="8" fontFamily="monospace" textAnchor="middle">
                {lon}°E
              </text>
            );
          })}

          {/* Turkey landmass */}
          <path d={toPath(THRACE)} fill="url(#land-grad)"
            stroke="rgba(100,160,255,0.55)" strokeWidth="1.4" strokeLinejoin="round" />
          <path d={toPath(ANATOLIA)} fill="url(#land-grad)"
            stroke="rgba(100,160,255,0.55)" strokeWidth="1.4" strokeLinejoin="round" />

          {/* Province boundary fills — only rendered once data is loaded */}
          {Object.entries(PROVINCE_BOUNDS).map(([key, coords]) => {
            const plateId = BOUNDARY_PLATE[key];
            const p = plateId ? provinceMap[plateId] : undefined;
            if (!p) return null;
            const color = getRiskColor(p.overall_score);
            const isActive = selected === p.id;
            const glowId = p.risk_level === "HIGH" ? "glow-h" : p.risk_level === "MEDIUM" ? "glow-m" : "glow-l";
            return (
              <path
                key={key}
                d={toPath(coords)}
                fill={color}
                fillOpacity={isActive ? 0.28 : 0.14}
                stroke={color}
                strokeWidth={isActive ? 1.6 : 0.9}
                strokeOpacity={isActive ? 0.9 : 0.45}
                strokeLinejoin="round"
                filter={isActive ? `url(#${glowId})` : undefined}
                className="cursor-pointer"
                style={{ transition: "fill-opacity 0.2s, stroke-opacity 0.2s" }}
                onMouseEnter={(e) => handleHover(p, e)}
              />
            );
          })}

          {/* Sea labels */}
          {([
            { label: "BLACK  SEA", lon: 35.5, lat: 42.15 },
            { label: "MEDITERRANEAN  SEA", lon: 32.5, lat: 36.32 },
            { label: "AEGEAN", lon: 25.85, lat: 38.5 },
          ] as { label: string; lon: number; lat: number }[]).map(({ label, lon, lat }) => {
            const [x, y] = proj(lon, lat);
            return (
              <text key={label} x={x.toFixed(0)} y={y.toFixed(0)}
                fill="rgba(255,255,255,0.14)" fontSize="9.5" fontFamily="Inter, sans-serif"
                fontWeight="300" letterSpacing="2.5" textAnchor="middle">
                {label}
              </text>
            );
          })}

          {/* Neighboring country labels */}
          {([
            { label: "GEORGIA",  lon: 42.5, lat: 42.0  },
            { label: "IRAQ",     lon: 43.5, lat: 36.5  },
            { label: "SYRIA",    lon: 37.5, lat: 35.85 },
            { label: "GREECE",   lon: 25.0, lat: 40.5  },
            { label: "BULGARIA", lon: 27.0, lat: 42.35 },
          ] as { label: string; lon: number; lat: number }[]).map(({ label, lon, lat }) => {
            const [x, y] = proj(lon, lat);
            if (x < 0 || x > 960 || y < 0 || y > 440) return null;
            return (
              <text key={label} x={x.toFixed(0)} y={y.toFixed(0)}
                fill="rgba(255,255,255,0.10)" fontSize="8" fontFamily="Inter, sans-serif"
                fontWeight="300" letterSpacing="1.5" textAnchor="middle">
                {label}
              </text>
            );
          })}

          {/* Province markers */}
          {provinces.map((province) => {
            const [cx, cy] = proj(province.lng, province.lat);
            const color = getRiskColor(province.overall_score);
            const isActive = selected === province.id;
            const glowId = province.risk_level === "HIGH" ? "glow-h" : province.risk_level === "MEDIUM" ? "glow-m" : "glow-l";

            return (
              <g
                key={province.id}
                transform={`translate(${cx.toFixed(1)}, ${cy.toFixed(1)})`}
                className="cursor-pointer"
                onMouseEnter={(e) => handleHover(province, e)}
              >
                <circle r={isActive ? 22 : 15} fill={color} fillOpacity={isActive ? 0.18 : 0.08}
                  stroke={color} strokeOpacity={0.30} strokeWidth="1"
                  style={{ transition: "r 0.2s ease" }} />
                {province.risk_level === "HIGH" && (
                  <circle r={isActive ? 30 : 22} fill="none" stroke={color}
                    strokeOpacity={0.10} strokeWidth="1"
                    style={{ transition: "r 0.2s ease" }} />
                )}
                <circle r={isActive ? 9 : 7} fill={color} filter={`url(#${glowId})`}
                  style={{ transition: "r 0.2s ease" }} />
                <circle r={isActive ? 3.5 : 2.8} fill="white" fillOpacity="0.92" />
                <text x="0" y={isActive ? -16 : -12} textAnchor="middle" fill={color}
                  fontSize={isActive ? "11" : "9.5"} fontWeight="700" fontFamily="Inter, sans-serif"
                  style={{ transition: "all 0.15s ease" }}>
                  {province.overall_score}
                </text>
                <text x="0" y={isActive ? 22 : 18} textAnchor="middle" fill="white"
                  fillOpacity={isActive ? 0.95 : 0.65} fontSize="8.5"
                  fontWeight={isActive ? "600" : "400"} fontFamily="Inter, sans-serif"
                  style={{ transition: "all 0.15s ease" }}>
                  {province.name}
                </text>
              </g>
            );
          })}
        </svg>

        {/* Loading overlay */}
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-[#050e1f]/70 backdrop-blur-sm">
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 rounded-full border-2 border-cyan-400/30 border-t-cyan-400 animate-spin" />
              <span className="text-xs text-white/50 font-mono">Fetching province data…</span>
            </div>
          </div>
        )}

        {/* Error overlay */}
        {error && !loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-[#050e1f]/70 backdrop-blur-sm">
            <div className="text-center px-6">
              <p className="text-xs text-red-400 mb-1">⚠ Could not load map data</p>
              <p className="text-[11px] text-white/30 font-mono">{error}</p>
            </div>
          </div>
        )}

        {/* Tooltip */}
        {tooltip.province && (
          <div
            className="absolute z-20 pointer-events-none"
            style={{
              left: Math.min(Math.max(tooltip.x + 14, 8), 700),
              top: Math.max(tooltip.y - 90, 8),
            }}
          >
            <ProvinceTooltip province={tooltip.province} />
          </div>
        )}
      </div>
    </div>
  );
}

// ── Sub-components ─────────────────────────────────────────────────────────────
function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
      <span className="text-white/45">{label}</span>
    </div>
  );
}

function ProvinceTooltip({ province }: { province: Province }) {
  const color = getRiskColor(province.overall_score);
  const insuredM = (province.insured_assets / 1_000_000).toFixed(0);

  return (
    <div
      className="rounded-xl border p-4 w-56 shadow-2xl"
      style={{
        background: "rgba(6, 16, 38, 0.97)",
        borderColor: `${color}45`,
        backdropFilter: "blur(16px)",
        boxShadow: `0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px ${color}20`,
      }}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="font-bold text-white text-sm leading-tight">{province.name}</div>
          <div className="text-[10px] text-white/35 mt-0.5">{province.region} Region</div>
        </div>
        <div
          className="text-[10px] font-bold px-2 py-1 rounded-lg"
          style={{ color, backgroundColor: `${color}18`, border: `1px solid ${color}40` }}
        >
          {province.risk_level}
        </div>
      </div>

      {/* Overall score bar */}
      <div className="mb-3">
        <div className="flex justify-between items-center mb-1.5">
          <span className="text-[10px] text-white/45 font-medium">Overall Risk Score</span>
          <span className="text-sm font-bold" style={{ color }}>{province.overall_score}/100</span>
        </div>
        <div className="h-2 bg-white/8 rounded-full overflow-hidden">
          <div className="h-full rounded-full"
            style={{ width: `${province.overall_score}%`, backgroundColor: color }} />
        </div>
      </div>

      {/* Sub-scores */}
      <div className="grid grid-cols-2 gap-2 mb-3">
        <ScoreBox label="Flood"   value={province.flood_score}   color="#3b82f6" />
        <ScoreBox label="Drought" value={province.drought_score} color="#f59e0b" />
      </div>

      {/* Stats */}
      <div className="border-t border-white/8 pt-2.5 space-y-1.5">
        <InfoRow label="Insured Assets" value={`$${insuredM}M`} />
        <InfoRow
          label="30-day Trend"
          value={
            <span style={{ color: province.trend === "UP" ? "#ef4444" : province.trend === "DOWN" ? "#22c55e" : "#ffffff80" }}>
              {province.trend === "UP" ? "▲" : province.trend === "DOWN" ? "▼" : "—"} {province.trend_pct}%
            </span>
          }
        />
        <InfoRow label="Population" value={province.population.toLocaleString()} />
      </div>
    </div>
  );
}

function ScoreBox({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="rounded-lg p-2 text-center"
      style={{ background: `${color}10`, border: `1px solid ${color}20` }}>
      <div className="text-[10px] text-white/40 mb-0.5">{label}</div>
      <div className="text-sm font-bold" style={{ color }}>{value}</div>
    </div>
  );
}

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-[10px] text-white/35">{label}</span>
      <span className="text-[11px] font-medium text-white/80">{value}</span>
    </div>
  );
}
