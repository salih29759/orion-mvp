"use client";

import type { BandCounts, BandKey } from "@/types";

const BANDS: BandKey[] = ["minimal", "minor", "moderate", "major", "extreme"];

const BAND_COLOR_HEX: Record<BandKey, string> = {
  minimal:  "#8BBF8B",
  minor:    "#C8C84A",
  moderate: "#E8903A",
  major:    "#D44A2A",
  extreme:  "#AA1A1A",
};

interface ScoreBarProps {
  bands: BandCounts;
  total?: number;
  showTooltip?: boolean;
}

export function ScoreBar({ bands, total, showTooltip = true }: ScoreBarProps) {
  const t = total ?? BANDS.reduce((sum, b) => sum + (bands[b] ?? 0), 0);
  if (t === 0) {
    return <div className="h-2 w-full rounded-full bg-[var(--border)]" />;
  }
  return (
    <div className="flex w-full h-2 rounded-full overflow-hidden gap-px" title={showTooltip ? buildTitle(bands, t) : undefined}>
      {BANDS.map((band) => {
        const count = bands[band] ?? 0;
        if (count === 0) return null;
        const pct = (count / t) * 100;
        return (
          <div
            key={band}
            className="score-segment h-full"
            style={{ width: `${pct}%`, backgroundColor: BAND_COLOR_HEX[band] }}
          />
        );
      })}
    </div>
  );
}

function buildTitle(bands: BandCounts, total: number): string {
  return BANDS.filter((b) => bands[b])
    .map((b) => `${b}: ${bands[b]} (${(((bands[b] ?? 0) / total) * 100).toFixed(1)}%)`)
    .join(", ");
}
