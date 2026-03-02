import type { BandKey } from "@/types";

const BAND_STYLES: Record<BandKey, string> = {
  minimal:  "bg-green-500/15 text-green-400 border-green-500/25",
  minor:    "bg-yellow-500/15 text-yellow-300 border-yellow-500/25",
  moderate: "bg-orange-500/15 text-orange-400 border-orange-500/25",
  major:    "bg-red-500/15 text-red-400 border-red-500/25",
  extreme:  "bg-red-900/30 text-red-300 border-red-700/40",
};

export function BandBadge({ band }: { band: BandKey }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide border ${BAND_STYLES[band]}`}
    >
      {band}
    </span>
  );
}

export function SeverityBadge({ severity }: { severity: "high" | "medium" }) {
  const style =
    severity === "high"
      ? "bg-red-500/15 text-red-400 border-red-500/25"
      : "bg-yellow-500/15 text-yellow-300 border-yellow-500/25";
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide border ${style}`}
    >
      {severity}
    </span>
  );
}
