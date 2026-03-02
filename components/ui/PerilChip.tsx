"use client";

import { twMerge } from "tailwind-merge";
import type { AllPerilKey } from "@/types";

const PERIL_LABEL: Record<string, string> = {
  all:     "All Hazards",
  heat:    "Heat",
  rain:    "Rain",
  wind:    "Wind",
  drought: "Drought",
  wildfire:"Wildfire",
};

const PERIL_COLOR_HEX: Record<string, string> = {
  all:     "#1B3A4B",
  heat:    "#E05520",
  rain:    "#2460C8",
  wind:    "#6B40B0",
  drought: "#C09020",
  wildfire:"#C02820",
};

interface PerilChipProps {
  peril:    AllPerilKey | string;
  active?:  boolean;
  onClick?: () => void;
  className?: string;
}

export function PerilChip({ peril, active = false, onClick, className }: PerilChipProps) {
  const color = PERIL_COLOR_HEX[peril] ?? "#5C5850";
  const label = PERIL_LABEL[peril] ?? peril;

  return (
    <button
      onClick={onClick}
      className={twMerge(
        "px-3 py-1.5 rounded-full text-xs font-semibold border transition-all whitespace-nowrap",
        active
          ? "text-white"
          : "bg-transparent hover:bg-opacity-10",
        className
      )}
      style={{
        backgroundColor: active ? color : "transparent",
        borderColor:     color,
        color:           active ? "#fff" : color,
      }}
    >
      {label}
    </button>
  );
}
