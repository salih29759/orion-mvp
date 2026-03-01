"use client";

import { useDashboard } from "@/lib/useDashboard";

export default function DataFreshness() {
  const { asOfDate, dataSource, loading } = useDashboard();

  if (loading) {
    return <div className="text-xs text-white/30">Loading data status…</div>;
  }

  const formatted = asOfDate
    ? new Date(`${asOfDate}T00:00:00Z`).toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
        timeZone: "UTC",
      })
    : "n/a";

  return (
    <div className="text-xs text-white/25 mt-1">
      Last updated: <span className="text-white/50">{formatted} (UTC)</span>
      {dataSource ? (
        <>
          {" "}
          · Source: <span className="text-white/50">{dataSource}</span>
        </>
      ) : null}
    </div>
  );
}
