"use client";

import { usePathname, useRouter } from "next/navigation";
import { Bell } from "lucide-react";
import { useGlobalStore } from "@/lib/store";
import { usePortfolios, useNotifications } from "@/hooks/useApi";
import type { DateRangeLabel } from "@/types";

const PAGE_TITLES: Record<string, string> = {
  "/portfolio":     "Portfolio Overview",
  "/assets":        "Assets",
  "/geostrategy":   "Geostrategy",
  "/scenario":      "Scenario Analysis",
  "/notifications": "Notifications",
  "/api-docs":      "API Docs",
};

const DATE_OPTIONS: { label: string; value: DateRangeLabel }[] = [
  { label: "30d",  value: "30d" },
  { label: "90d",  value: "90d" },
  { label: "1yr",  value: "365d" },
];

export function TopBar() {
  const pathname = usePathname();
  const router   = useRouter();

  const { selectedPortfolioId, setSelectedPortfolioId, dateRangeLabel, setDateRange } =
    useGlobalStore();
  const { data: portfolios } = usePortfolios();
  const { data: notifs } = useNotifications(selectedPortfolioId ?? undefined);

  const unread = notifs?.filter((n) => !n.acknowledged_at).length ?? 0;

  const title = Object.entries(PAGE_TITLES).find(([k]) =>
    pathname === k || pathname.startsWith(k + "/")
  )?.[1] ?? "Orion";

  return (
    <header
      className="flex items-center justify-between px-6 h-16 shrink-0 border-b"
      style={{ backgroundColor: "var(--bg-surface)", borderColor: "var(--border)" }}
    >
      {/* Left — page title */}
      <h2 className="font-serif text-[20px] leading-none" style={{ color: "var(--text-primary)" }}>
        {title}
      </h2>

      {/* Center — portfolio picker + date range */}
      <div className="flex items-center gap-3">
        <select
          value={selectedPortfolioId ?? ""}
          onChange={(e) => setSelectedPortfolioId(e.target.value || null)}
          className="text-sm rounded-lg px-3 py-1.5 border outline-none focus:ring-1 font-sans"
          style={{
            backgroundColor: "var(--bg-page)",
            borderColor:     "var(--border)",
            color:           "var(--text-primary)",
          }}
        >
          <option value="">All portfolios</option>
          {portfolios?.map((p) => (
            <option key={p.portfolio_id} value={p.portfolio_id}>{p.name}</option>
          ))}
        </select>

        <div
          className="flex rounded-lg border overflow-hidden text-xs font-semibold"
          style={{ borderColor: "var(--border)" }}
        >
          {DATE_OPTIONS.map((opt) => (
            <button
              key={opt.value}
              onClick={() => setDateRange(opt.value)}
              className="px-3 py-1.5 transition-colors"
              style={{
                backgroundColor: dateRangeLabel === opt.value ? "var(--accent)" : "var(--bg-page)",
                color:           dateRangeLabel === opt.value ? "#fff" : "var(--text-secondary)",
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Right */}
      <div className="flex items-center gap-4">
        {/* LIVE indicator */}
        <div className="flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-green-500 pulse-dot" />
          <span className="text-[11px] font-semibold text-green-600 uppercase tracking-widest">LIVE</span>
        </div>

        {/* Alerts badge */}
        {unread > 0 && (
          <button
            onClick={() => router.push("/notifications")}
            className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-bold transition-colors"
            style={{
              backgroundColor: "rgba(170,26,26,0.1)",
              color:           "var(--extreme)",
              border:          "1px solid rgba(170,26,26,0.2)",
            }}
          >
            <Bell size={13} />
            {unread} HIGH
          </button>
        )}
      </div>
    </header>
  );
}
