import { twMerge } from "tailwind-merge";
import type { BandKey } from "@/types";

// CSS variable values mapped for inline styles (needed for dynamic coloring)
export const BAND_COLOR: Record<BandKey, string> = {
  minimal:  "var(--minimal)",
  minor:    "var(--minor)",
  moderate: "var(--moderate)",
  major:    "var(--major)",
  extreme:  "var(--extreme)",
};

export const BAND_BG: Record<BandKey, string> = {
  minimal:  "rgba(139,191,139,0.12)",
  minor:    "rgba(200,200,74,0.12)",
  moderate: "rgba(232,144,58,0.12)",
  major:    "rgba(212,74,42,0.12)",
  extreme:  "rgba(170,26,26,0.12)",
};

export const PERIL_COLOR: Record<string, string> = {
  all:      "var(--accent)",
  heat:     "var(--heat)",
  rain:     "var(--rain)",
  wind:     "var(--wind)",
  drought:  "var(--drought)",
  wildfire: "var(--wildfire)",
  flood:    "var(--flood)",
};

export const SEVERITY_COLOR: Record<string, string> = {
  low:    "var(--minimal)",
  medium: "var(--moderate)",
  high:   "var(--extreme)",
};

// BandBadge — inline pill
export function BandBadge({ band, className }: { band: BandKey; className?: string }) {
  return (
    <span
      className={twMerge(
        "inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border",
        className
      )}
      style={{
        color:           BAND_COLOR[band],
        backgroundColor: BAND_BG[band],
        borderColor:     BAND_COLOR[band] + "40",
      }}
    >
      {band}
    </span>
  );
}

// ScoreDisplay — "■ 88/100  Extreme" (First Street format)
interface ScoreDisplayProps {
  score:   number | null | undefined;
  band?:   BandKey | null;
  size?:   "sm" | "md" | "lg" | "xl";
  className?: string;
}

const SIZE_MAP = {
  sm: { square: "text-sm",  number: "text-xl",    denom: "text-sm",   label: "text-[10px]" },
  md: { square: "text-lg",  number: "text-3xl",   denom: "text-base", label: "text-xs" },
  lg: { square: "text-2xl", number: "text-5xl",   denom: "text-xl",   label: "text-sm" },
  xl: { square: "text-4xl", number: "text-[72px]",denom: "text-2xl",  label: "text-base" },
};

export function ScoreDisplay({ score, band, size = "md", className }: ScoreDisplayProps) {
  const s = SIZE_MAP[size];
  const color = band ? BAND_COLOR[band] : "var(--text-muted)";
  return (
    <div className={twMerge("flex flex-col gap-0.5", className)}>
      <div className="flex items-baseline gap-1.5">
        <span className={twMerge(s.square, "leading-none")} style={{ color }}>■</span>
        <span
          className={twMerge(s.number, "font-serif leading-none tabular-nums")}
          style={{ color }}
        >
          {score ?? "—"}
        </span>
        <span
          className={twMerge(s.denom, "font-mono leading-none")}
          style={{ color: "var(--text-muted)" }}
        >
          /100
        </span>
      </div>
      {band && (
        <span
          className={twMerge(s.label, "font-sans font-semibold uppercase tracking-widest leading-none")}
          style={{ color }}
        >
          {band}
        </span>
      )}
    </div>
  );
}

// SeverityBadge
export function SeverityBadge({ severity }: { severity: "low" | "medium" | "high" }) {
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border"
      style={{
        color:           SEVERITY_COLOR[severity],
        backgroundColor: SEVERITY_COLOR[severity] + "20",
        borderColor:     SEVERITY_COLOR[severity] + "40",
      }}
    >
      {severity}
    </span>
  );
}

// ScoreCell — table cell: "■ 88/100 ↵ Extreme"
export function ScoreCell({ score, band }: { score: number | null | undefined; band?: BandKey | null }) {
  if (score == null) return <span className="font-mono text-sm" style={{ color: "var(--text-muted)" }}>—</span>;
  const color = band ? BAND_COLOR[band] : "var(--text-muted)";
  return (
    <div className="inline-flex flex-col leading-tight">
      <div className="flex items-baseline gap-1">
        <span style={{ color }} className="text-sm leading-none">■</span>
        <span style={{ color }} className="font-mono font-bold text-sm tabular-nums leading-none">{score}</span>
        <span className="font-mono text-xs leading-none" style={{ color: "var(--text-muted)" }}>/100</span>
      </div>
      {band && (
        <span
          style={{ color }}
          className="text-[10px] font-semibold uppercase tracking-wider leading-tight mt-0.5"
        >
          {band}
        </span>
      )}
    </div>
  );
}
