"use client";

import { useState } from "react";
import {
  Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  Area, AreaChart, CartesianGrid,
} from "recharts";
import { PerilChip } from "@/components/ui/PerilChip";
import type { AllPerilKey } from "@/types";

const SAMPLE_DATA = [
  { x: "20%", gross: 3.2, net: 2.1, lo: 1.5, hi: 4.5 },
  { x: "5%",  gross: 8.1, net: 5.8, lo: 4.2, hi: 11.2 },
  { x: "1%",  gross: 14.5, net: 10.2, lo: 8.1, hi: 20.1 },
  { x: "0.5%",gross: 19.2, net: 13.8, lo: 11.5, hi: 26.2 },
  { x: "0.2%",gross: 24.8, net: 17.5, lo: 14.2, hi: 33.5 },
];

const PERIL_TABS: AllPerilKey[] = ["all", "drought", "rain", "wildfire"];
const PERIL_LABELS: Record<string, string> = {
  all: "All Events", heat: "Heat", rain: "Flood", wind: "Wind", drought: "Drought", wildfire: "Wildfire",
};

export default function ScenarioPage() {
  const [activeTab, setActiveTab] = useState<AllPerilKey>("all");

  return (
    <div className="space-y-6 max-w-[1600px] mx-auto">
      {/* Description */}
      <p className="text-sm italic max-w-3xl" style={{ color: "var(--text-secondary)" }}>
        The analysis encompasses synthetic climate events including droughts, floods, and heat waves,
        providing comprehensive insights into correlated effects on the portfolio.
      </p>

      {/* Controls (disabled) */}
      <div className="flex items-center gap-3 opacity-60 pointer-events-none">
        {[
          { label: "Scenario",     options: ["SSP2-4.5 (Mid)"] },
          { label: "Time Horizon", options: ["Current year"] },
        ].map((ctrl) => (
          <select
            key={ctrl.label}
            disabled
            className="text-sm rounded-lg px-3 py-2 border outline-none"
            style={{ backgroundColor: "var(--bg-surface)", borderColor: "var(--border)", color: "var(--text-primary)" }}
          >
            <option>{ctrl.label}: {ctrl.options[0]}</option>
          </select>
        ))}
      </div>

      {/* Portfolio Effects card */}
      <div
        className="rounded-xl overflow-hidden"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <div className="px-5 py-4 border-b" style={{ borderColor: "var(--border)" }}>
          <p className="text-xs font-bold uppercase tracking-wider mb-3" style={{ color: "var(--text-secondary)" }}>
            Portfolio Effects
          </p>
          <div className="flex gap-1.5">
            {PERIL_TABS.map((p) => (
              <PerilChip key={p} peril={p} active={activeTab === p} onClick={() => setActiveTab(p)} />
            ))}
          </div>
        </div>

        <div className="p-5 relative">
          <div className="flex items-start justify-between mb-5">
            <div>
              <p className="text-xs font-bold uppercase tracking-wider mb-1" style={{ color: "var(--text-secondary)" }}>
                Direct Damage Loss
              </p>
              <div className="flex items-center gap-4 text-xs" style={{ color: "var(--text-muted)" }}>
                <span className="flex items-center gap-1.5">
                  <span className="w-4 h-0.5 rounded inline-block" style={{ backgroundColor: "var(--accent)" }} />
                  Gross damage
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="w-4 h-0.5 rounded inline-block border-dashed border-t" style={{ borderColor: "var(--rain)" }} />
                  Net damage
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="w-4 h-3 rounded inline-block opacity-30" style={{ backgroundColor: "var(--accent)" }} />
                  Uncertainty
                </span>
              </div>
            </div>
            <select
              disabled
              className="text-xs rounded-lg px-2 py-1 border opacity-60"
              style={{ backgroundColor: "var(--bg-page)", borderColor: "var(--border)", color: "var(--text-secondary)" }}
            >
              <option>Loss ($)</option>
            </select>
          </div>

          {/* Chart (real but with sample data) */}
          <div className="opacity-60">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={SAMPLE_DATA} margin={{ top: 10, right: 10 }}>
                <defs>
                  <linearGradient id="uncertaintyGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="var(--accent)" stopOpacity={0.15} />
                    <stop offset="95%" stopColor="var(--accent)" stopOpacity={0.03} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="x" tick={{ fill: "var(--text-muted)", fontSize: 11 }} axisLine={false} tickLine={false}
                  label={{ value: "Annual Exceedance Probability", position: "insideBottom", offset: -2, fontSize: 10, fill: "var(--text-muted)" }} />
                <YAxis
                  tick={{ fill: "var(--text-muted)", fontSize: 11 }}
                  axisLine={false} tickLine={false}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  tickFormatter={(v: any) => `$${v}M`}
                />
                <Tooltip
                  contentStyle={{ background: "var(--bg-surface)", border: "1px solid var(--border)", borderRadius: 8 }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={(v: any) => [`$${v}M`, ""]}
                />
                <Area type="monotone" dataKey="hi" stroke="transparent" fill="url(#uncertaintyGrad)" name="Upper bound" />
                <Area type="monotone" dataKey="lo" stroke="transparent" fill="var(--bg-surface)" name="Lower bound" />
                <Line type="monotone" dataKey="gross" stroke="var(--accent)" strokeWidth={2.5} dot={{ r: 4, fill: "var(--accent)" }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  label={{ fontSize: 10, fill: "var(--accent)", formatter: (v: any) => `$${v}M`, position: "top" }} name="Gross" />
                <Line type="monotone" dataKey="net" stroke="var(--rain)" strokeWidth={1.5} strokeDasharray="6 3" dot={{ r: 3, fill: "var(--rain)" }} name="Net" />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Overlay */}
          <div
            className="absolute inset-x-5 top-24 bottom-5 flex items-center justify-center rounded-xl"
            style={{ backgroundColor: "rgba(245,243,238,0.82)", backdropFilter: "blur(4px)" }}
          >
            <div className="text-center px-6">
              <p className="font-serif text-lg mb-2" style={{ color: "var(--text-primary)" }}>
                Loss Engine Integration Required
              </p>
              <p className="text-sm mb-4" style={{ color: "var(--text-muted)" }}>
                Connect ERA5 data + fragility curves to activate scenario analysis
              </p>
              <button
                className="text-sm px-5 py-2 rounded-lg border font-medium transition-colors"
                style={{ borderColor: "var(--accent)", color: "var(--accent)", backgroundColor: "rgba(27,58,75,0.05)" }}
              >
                View Roadmap →
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Historical events card (static preview) */}
      <div
        className="rounded-xl p-5"
        style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
      >
        <p className="text-xs font-bold uppercase tracking-wider mb-4" style={{ color: "var(--text-secondary)" }}>
          Historical Climate Events — Turkey (Preview)
        </p>
        <div className="space-y-2 opacity-50 pointer-events-none">
          {[
            { year: 2021, event: "Marmara Wildfires",   loss: "$1.2B", perils: ["wildfire"], severity: "extreme" },
            { year: 2021, event: "Black Sea Floods",     loss: "$500M", perils: ["rain"],     severity: "major" },
            { year: 2023, event: "Turkey Earthquake",    loss: "$34B",  perils: ["wind"],     severity: "extreme" },
            { year: 2024, event: "Aegean Drought",       loss: "$420M", perils: ["drought"],  severity: "major" },
          ].map((ev) => (
            <div
              key={ev.year + ev.event}
              className="flex items-center gap-4 py-2.5 border-b text-sm"
              style={{ borderColor: "var(--border)" }}
            >
              <span className="font-mono text-xs w-10 shrink-0" style={{ color: "var(--text-muted)" }}>{ev.year}</span>
              <span className="flex-1 font-medium" style={{ color: "var(--text-primary)" }}>{ev.event}</span>
              <span className="font-mono font-bold text-xs" style={{ color: "var(--extreme)" }}>{ev.loss}</span>
              <span
                className="text-[10px] font-bold uppercase px-2 py-0.5 rounded border"
                style={{
                  color: ev.severity === "extreme" ? "var(--extreme)" : "var(--major)",
                  borderColor: "currentColor",
                }}
              >
                {ev.severity}
              </span>
            </div>
          ))}
        </div>
        <div className="mt-3 flex justify-center">
          <span className="text-xs italic" style={{ color: "var(--text-muted)" }}>
            Historical loss data integration — Phase 4
          </span>
        </div>
      </div>
    </div>
  );
}
