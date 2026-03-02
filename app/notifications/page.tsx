"use client";

import { useState } from "react";
import { Bell, ChevronDown, ChevronUp } from "lucide-react";
import { useNotifications, useAckNotification, usePortfolios } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";
import { SeverityBadge } from "@/components/ui/BandBadge";
import { Skeleton } from "@/components/ui/Skeleton";
import type { Notification } from "@/types";

const SEVERITY_BAR_COLOR: Record<string, string> = {
  high:   "#AA1A1A",
  medium: "#E8903A",
  low:    "#C8C84A",
};

function NotificationCard({
  notification, onAck, acking,
}: {
  notification: Notification;
  onAck: (id: string) => void;
  acking: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const isAcked = !!notification.acknowledged_at;
  const barColor = SEVERITY_BAR_COLOR[notification.severity] ?? "#9A9590";

  // Parse payload keys for clean display
  const payloadEntries = Object.entries(notification.payload ?? {}).filter(
    ([, v]) => v !== null && v !== undefined
  );

  return (
    <div
      className="rounded-xl overflow-hidden transition-opacity"
      style={{
        backgroundColor: "var(--bg-surface)",
        border: "1px solid var(--border)",
        opacity: isAcked ? 0.5 : 1,
      }}
    >
      {/* Severity bar */}
      <div className="h-1 w-full" style={{ backgroundColor: barColor }} />

      <div className="p-5">
        <div className="flex items-start gap-3">
          {/* Left content */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <SeverityBadge severity={notification.severity} />
              <span className="font-semibold text-sm" style={{ color: "var(--text-primary)" }}>
                {notification.type}
              </span>
            </div>
            <div className="flex items-center gap-3 text-xs" style={{ color: "var(--text-muted)" }}>
              <span className="font-mono">{notification.asset_id}</span>
              {notification.portfolio_id && <span>· {notification.portfolio_id}</span>}
              <span>·</span>
              <span>{new Date(notification.created_at).toLocaleString("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" })}</span>
            </div>

            {/* Summary line from payload */}
            {notification.payload?.description ? (
              <p className="text-sm mt-2" style={{ color: "var(--text-secondary)" }}>
                {String(notification.payload.description)}
              </p>
            ) : null}

            {/* Key stats from payload */}
            <div className="flex flex-wrap gap-4 mt-2">
              {["policies", "affected_policies"].map((k) => {
                const val = notification.payload[k];
                if (!val) return null;
                return (
                  <div key={k}>
                    <span className="text-[10px] uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>Policies</span>
                    <p className="font-mono text-sm font-bold" style={{ color: "var(--text-primary)" }}>
                      {String(val)}
                    </p>
                  </div>
                );
              })}
              {["estimated_loss", "loss_estimate"].map((k) => {
                const val = notification.payload[k];
                if (!val) return null;
                return (
                  <div key={k}>
                    <span className="text-[10px] uppercase tracking-widest" style={{ color: "var(--text-muted)" }}>Est. Loss</span>
                    <p className="font-mono text-sm font-bold" style={{ color: "var(--extreme)" }}>
                      ${typeof val === "number" ? (val as number).toLocaleString() : String(val)}
                    </p>
                  </div>
                );
              })}
            </div>

            <p className="font-mono text-[10px] mt-1" style={{ color: "var(--text-muted)" }}>
              ID: {notification.id}
            </p>
          </div>

          {/* Right actions */}
          <div className="flex items-center gap-2 shrink-0">
            {!isAcked ? (
              <button
                onClick={() => onAck(notification.id)}
                disabled={acking}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold border transition-colors disabled:opacity-50"
                style={{
                  backgroundColor: "rgba(139,191,139,0.1)",
                  borderColor:     "rgba(139,191,139,0.4)",
                  color:           "var(--minimal)",
                }}
              >
                {acking ? (
                  <div className="w-3 h-3 border border-current border-t-transparent rounded-full animate-spin" />
                ) : (
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                  </svg>
                )}
                Acknowledge
              </button>
            ) : (
              <span className="text-[10px] font-mono px-2 py-1 rounded" style={{ backgroundColor: "rgba(139,191,139,0.1)", color: "var(--minimal)" }}>
                ✓ Acknowledged
              </span>
            )}
          </div>
        </div>

        {/* Payload toggle */}
        <div className="mt-3 pt-3 border-t" style={{ borderColor: "var(--border)" }}>
          <button
            onClick={() => setExpanded((v) => !v)}
            className="flex items-center gap-1 text-xs transition-colors"
            style={{ color: "var(--text-muted)" }}
          >
            {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            {expanded ? "Hide payload" : "Show payload"}
          </button>

          {expanded && (
            <div className="mt-3 rounded-lg p-3 text-xs" style={{ backgroundColor: "var(--bg-page)", border: "1px solid var(--border)" }}>
              {payloadEntries.length === 0 ? (
                <span style={{ color: "var(--text-muted)" }}>No payload data</span>
              ) : (
                <dl className="space-y-1.5">
                  {payloadEntries.map(([k, v]) => (
                    <div key={k} className="flex gap-3">
                      <dt className="font-mono min-w-[120px]" style={{ color: "var(--text-muted)" }}>{k}:</dt>
                      <dd className="font-mono" style={{ color: "var(--text-secondary)" }}>
                        {typeof v === "number" ? v.toLocaleString() : String(v)}
                      </dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default function NotificationsPage() {
  const { selectedPortfolioId, setSelectedPortfolioId } = useGlobalStore();
  const { data: portfolios } = usePortfolios();
  const { data, isLoading, error, refetch } = useNotifications(selectedPortfolioId ?? undefined);
  const { mutate: ack, variables: ackingId, isPending: acking } = useAckNotification();

  const [severityFilter, setSeverityFilter] = useState<"all" | "high" | "medium" | "low">("all");

  const is404 = (error as (Error & { status?: number }) | null)?.status === 404;

  const filtered = (data ?? []).filter(
    (n) => severityFilter === "all" || n.severity === severityFilter
  );

  const highCount   = (data ?? []).filter((n) => n.severity === "high").length;
  const medCount    = (data ?? []).filter((n) => n.severity === "medium").length;
  const lowCount    = (data ?? []).filter((n) => n.severity === "low").length;

  return (
    <div className="space-y-5 max-w-[1600px] mx-auto">
      {/* Stats row */}
      {!isLoading && data && (
        <div className="flex gap-3 flex-wrap">
          {[
            { label: "HIGH",   count: highCount,  color: "var(--extreme)" },
            { label: "MEDIUM", count: medCount,   color: "var(--moderate)" },
            { label: "LOW",    count: lowCount,   color: "var(--minor)" },
          ].map(({ label, count, color }) => (
            <div
              key={label}
              className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold"
              style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)", color }}
            >
              <span>{count}</span>
              <span className="text-[10px] font-bold uppercase tracking-widest opacity-70">{label}</span>
            </div>
          ))}
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        {/* Severity */}
        <div className="flex rounded-lg border overflow-hidden text-xs font-semibold" style={{ borderColor: "var(--border)" }}>
          {(["all", "high", "medium", "low"] as const).map((s) => (
            <button
              key={s}
              onClick={() => setSeverityFilter(s)}
              className="px-3 py-2 capitalize transition-colors"
              style={{
                backgroundColor: severityFilter === s ? "var(--accent)" : "var(--bg-surface)",
                color:           severityFilter === s ? "#fff" : "var(--text-secondary)",
              }}
            >
              {s}
            </button>
          ))}
        </div>

        {/* Portfolio */}
        <select
          value={selectedPortfolioId ?? ""}
          onChange={(e) => setSelectedPortfolioId(e.target.value || null)}
          className="text-xs rounded-lg px-2.5 py-2 border outline-none"
          style={{ backgroundColor: "var(--bg-surface)", borderColor: "var(--border)", color: "var(--text-primary)" }}
        >
          <option value="">All portfolios</option>
          {portfolios?.map((p) => <option key={p.portfolio_id} value={p.portfolio_id}>{p.name}</option>)}
        </select>

        {/* Mark all read */}
        <button
          className="ml-auto text-xs px-3 py-2 rounded-lg border transition-colors"
          style={{ borderColor: "var(--border)", color: "var(--text-secondary)" }}
          onClick={() => { /* TODO batch ack */ }}
        >
          Mark all read
        </button>
      </div>

      {/* 404 / coming soon */}
      {is404 && (
        <div
          className="rounded-xl p-16 text-center"
          style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
        >
          <Bell className="mx-auto mb-4 opacity-30" size={40} />
          <p className="font-serif text-lg mb-1" style={{ color: "var(--text-primary)" }}>Notification system coming soon</p>
          <p className="text-sm" style={{ color: "var(--text-muted)" }}>
            Active when FIRMS wildfire integration is complete
          </p>
        </div>
      )}

      {/* Generic error */}
      {error && !is404 && (
        <div
          className="rounded-xl p-5 flex items-center justify-between"
          style={{ backgroundColor: "rgba(170,26,26,0.06)", border: "1px solid rgba(170,26,26,0.2)" }}
        >
          <span className="text-sm" style={{ color: "var(--extreme)" }}>{(error as Error).message}</span>
          <button onClick={() => refetch()} className="text-xs px-3 py-1.5 rounded" style={{ backgroundColor: "rgba(170,26,26,0.1)", color: "var(--extreme)" }}>Retry</button>
        </div>
      )}

      {/* Loading */}
      {isLoading && (
        <div className="space-y-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="rounded-xl p-5" style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}>
              <Skeleton className="h-4 w-48 mb-2" />
              <Skeleton className="h-3 w-72 mb-2" />
              <Skeleton className="h-8 w-32" />
            </div>
          ))}
        </div>
      )}

      {/* Empty */}
      {!isLoading && !error && filtered.length === 0 && (
        <div
          className="rounded-xl p-16 text-center"
          style={{ backgroundColor: "var(--bg-surface)", border: "1px solid var(--border)" }}
        >
          <div className="text-3xl mb-3">✅</div>
          <p className="font-medium" style={{ color: "var(--text-secondary)" }}>No notifications</p>
          <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>
            {severityFilter !== "all" ? `No ${severityFilter} severity notifications` : "All clear — no active alerts"}
          </p>
        </div>
      )}

      {/* Notification cards */}
      {!error && !isLoading && (
        <div className="space-y-3">
          {filtered.map((n) => (
            <NotificationCard
              key={n.id}
              notification={n}
              onAck={(id) => ack(id)}
              acking={acking && ackingId === n.id}
            />
          ))}
        </div>
      )}
    </div>
  );
}
