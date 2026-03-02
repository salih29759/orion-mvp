"use client";

import { useState } from "react";
import { useNotifications, useAckNotification, usePortfolios } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { SeverityBadge } from "@/components/ui/BandBadge";
import { Skeleton } from "@/components/ui/Skeleton";
import type { Notification } from "@/types";

function NotificationRow({
  notification,
  onAck,
  acking,
}: {
  notification: Notification;
  onAck: (id: string) => void;
  acking: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const isAcked = notification.acknowledged;

  return (
    <div
      className={`border-b border-white/5 transition-colors ${
        isAcked ? "opacity-50" : "hover:bg-white/2"
      }`}
    >
      <div className="flex items-center gap-4 px-5 py-4">
        {/* Severity */}
        <SeverityBadge severity={notification.severity} />

        {/* Type + asset */}
        <div className="flex-1 min-w-0">
          <div className="font-medium text-white/90 text-sm">{notification.type}</div>
          <div className="text-[11px] text-white/40 font-mono mt-0.5">
            Asset: {notification.asset_id}
          </div>
        </div>

        {/* Timestamp */}
        <div className="text-[11px] text-white/30 font-mono hidden sm:block shrink-0">
          {new Date(notification.created_at).toLocaleString("en-GB", {
            day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit",
          })}
        </div>

        {/* Expand payload */}
        <button
          onClick={() => setExpanded((v) => !v)}
          className="p-1.5 rounded text-white/30 hover:text-white hover:bg-white/5 transition-colors"
          title="View payload"
        >
          <svg className={`w-4 h-4 transition-transform ${expanded ? "rotate-180" : ""}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {/* Ack button */}
        {!isAcked && (
          <button
            onClick={() => onAck(notification.id)}
            disabled={acking}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-green-500/10 border border-green-500/20 text-green-400 text-xs font-medium hover:bg-green-500/20 transition-colors disabled:opacity-50 disabled:cursor-not-allowed shrink-0"
          >
            {acking ? (
              <div className="w-3 h-3 border border-green-400/40 border-t-green-400 rounded-full animate-spin" />
            ) : (
              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            )}
            Ack
          </button>
        )}

        {isAcked && (
          <span className="text-[10px] text-white/20 font-mono shrink-0">acknowledged</span>
        )}
      </div>

      {/* Expandable payload */}
      {expanded && (
        <div className="px-5 pb-4">
          <div className="bg-[#050d1a] rounded-lg p-4 font-mono text-xs text-white/60 overflow-x-auto border border-white/5">
            <pre>{JSON.stringify(notification.payload, null, 2)}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

export default function NotificationsPage() {
  const { selectedPortfolioId, setSelectedPortfolioId } = useGlobalStore();
  const { data: portfolios } = usePortfolios();

  const { data, isLoading, error, refetch } = useNotifications(
    selectedPortfolioId ?? undefined
  );
  const { mutate: ack, variables: ackingId, isPending: acking } = useAckNotification();

  const [severityFilter, setSeverityFilter] = useState<"all" | "high" | "medium">("all");

  const is404 = (error as (Error & { status?: number }) | null)?.status === 404;

  const filtered = (data ?? []).filter(
    (n) => severityFilter === "all" || n.severity === severityFilter
  );

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white tracking-tight">Notifications</h1>
          <p className="text-sm text-white/40 mt-0.5">
            Wildfire alerts and climate risk events
          </p>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          {/* Portfolio filter */}
          <select
            value={selectedPortfolioId ?? ""}
            onChange={(e) => setSelectedPortfolioId(e.target.value || null)}
            className="bg-[#0f2040] border border-white/10 text-white text-sm rounded-lg px-3 py-2 focus:outline-none"
          >
            <option value="">All portfolios</option>
            {portfolios?.map((p) => (
              <option key={p.portfolio_id} value={p.portfolio_id}>
                {p.name}
              </option>
            ))}
          </select>

          {/* Severity filter */}
          <div className="flex rounded-lg border border-white/10 overflow-hidden text-sm">
            {(["all", "high", "medium"] as const).map((s) => (
              <button
                key={s}
                onClick={() => setSeverityFilter(s)}
                className={`px-3 py-2 capitalize transition-colors ${
                  severityFilter === s
                    ? "bg-[#1e6fff] text-white"
                    : "bg-[#0f2040] text-white/50 hover:text-white"
                }`}
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Coming soon — 404 from backend */}
      {is404 && (
        <div className="bg-[#070f1f] border border-white/8 rounded-xl p-12 text-center">
          <div className="text-4xl mb-4">🔔</div>
          <div className="text-white/60 font-medium">Notifications coming soon</div>
          <div className="text-white/30 text-sm mt-1">
            This feature will be available after the FIRMS sprint
          </div>
        </div>
      )}

      {/* Generic error */}
      {error && !is404 && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-5 flex items-center justify-between">
          <div className="text-red-400 text-sm">{(error as Error).message}</div>
          <button onClick={() => refetch()} className="px-3 py-1.5 rounded bg-red-500/15 border border-red-500/25 text-red-400 text-xs hover:bg-red-500/25 transition-colors">
            Retry
          </button>
        </div>
      )}

      {/* Notifications list */}
      {!error && (
        <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
          {/* Summary bar */}
          <div className="px-5 py-4 border-b border-white/8 flex items-center justify-between">
            <div className="text-xs font-semibold text-white/60 uppercase tracking-wider">
              {isLoading ? "Loading…" : `${filtered.length} notification${filtered.length !== 1 ? "s" : ""}`}
            </div>
            {!isLoading && data && data.length > 0 && (
              <div className="flex items-center gap-3 text-xs text-white/40">
                <span className="text-red-400 font-semibold">
                  {data.filter((n) => n.severity === "high").length} high
                </span>
                <span>·</span>
                <span className="text-yellow-400 font-semibold">
                  {data.filter((n) => n.severity === "medium").length} medium
                </span>
              </div>
            )}
          </div>

          {isLoading ? (
            <div className="divide-y divide-white/5">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="px-5 py-4 flex items-center gap-4">
                  <Skeleton className="h-5 w-14" />
                  <div className="flex-1 space-y-2">
                    <Skeleton className="h-3 w-48" />
                    <Skeleton className="h-3 w-32" />
                  </div>
                  <Skeleton className="h-7 w-14" />
                </div>
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <div className="p-12 text-center">
              <div className="text-3xl mb-3">✅</div>
              <div className="text-white/50 font-medium">No notifications</div>
              <div className="text-white/30 text-sm mt-1">
                {severityFilter !== "all"
                  ? `No ${severityFilter} severity notifications`
                  : "All clear — no active alerts"}
              </div>
            </div>
          ) : (
            <div>
              {filtered.map((n) => (
                <NotificationRow
                  key={n.id}
                  notification={n}
                  onAck={(id) => ack(id)}
                  acking={acking && ackingId === n.id}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
