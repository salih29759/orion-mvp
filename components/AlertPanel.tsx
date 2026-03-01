"use client";

import { useDashboard } from "@/lib/useDashboard";
import { Alert, getRiskTextColor } from "@/lib/api";

export default function AlertPanel() {
  const { alerts, loading, error } = useDashboard();

  const highAlerts = alerts.filter((a) => a.level === "HIGH");
  const mediumAlerts = alerts.filter((a) => a.level === "MEDIUM");

  return (
    <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
      {/* Header */}
      <div className="px-5 py-3.5 border-b border-white/8 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-red-400 pulse-dot" />
          <span className="text-sm font-semibold text-white">Active Alerts</span>
        </div>
        <div className="flex items-center gap-2">
          {loading ? (
            <div className="h-4 w-24 rounded-full bg-white/8 animate-pulse" />
          ) : (
            <>
              <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-red-500/15 text-red-400 border border-red-500/25">
                {highAlerts.length} HIGH
              </span>
              <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-orange-500/15 text-orange-400 border border-orange-500/25">
                {mediumAlerts.length} MED
              </span>
            </>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="divide-y divide-white/5 max-h-[420px] overflow-y-auto">
        {error ? (
          <div className="px-5 py-8 text-center">
            <p className="text-xs text-red-400 mb-1">⚠ Failed to load alerts</p>
            <p className="text-[11px] text-white/30 font-mono">{error}</p>
          </div>
        ) : loading ? (
          Array.from({ length: 4 }).map((_, i) => <AlertSkeleton key={i} />)
        ) : alerts.length === 0 ? (
          <div className="px-5 py-8 text-center">
            <p className="text-xs text-green-400">✓ No active alerts</p>
          </div>
        ) : (
          alerts.map((alert) => <AlertRow key={alert.id} alert={alert} />)
        )}
      </div>
    </div>
  );
}

function AlertSkeleton() {
  return (
    <div className="px-5 py-4">
      <div className="flex items-start gap-3">
        <div className="w-8 h-8 rounded-lg bg-white/5 animate-pulse shrink-0 mt-0.5" />
        <div className="flex-1 space-y-2">
          <div className="h-3 w-3/4 bg-white/8 rounded animate-pulse" />
          <div className="h-2.5 w-full bg-white/5 rounded animate-pulse" />
          <div className="h-2.5 w-4/5 bg-white/5 rounded animate-pulse" />
          <div className="flex gap-4">
            <div className="h-2.5 w-16 bg-white/5 rounded animate-pulse" />
            <div className="h-2.5 w-16 bg-white/5 rounded animate-pulse" />
          </div>
        </div>
      </div>
    </div>
  );
}

function AlertRow({ alert }: { alert: Alert }) {
  const color = getRiskTextColor(alert.level);
  const riskTypeIcons: Record<string, string> = {
    FLOOD: "🌊",
    DROUGHT: "☀️",
    WILDFIRE: "🔥",
    EARTHQUAKE: "⚠️",
  };

  const timeAgo = (dateStr: string) => {
    const diff = Date.now() - new Date(dateStr).getTime();
    const h = Math.floor(diff / 3_600_000);
    if (h < 1) return "Just now";
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  };

  return (
    <div className="px-5 py-4 hover:bg-white/3 transition-colors group">
      <div className="flex items-start gap-3">
        {/* Icon */}
        <div
          className="w-8 h-8 rounded-lg flex items-center justify-center text-base shrink-0 mt-0.5"
          style={{ background: `${color}12`, border: `1px solid ${color}25` }}
        >
          {riskTypeIcons[alert.risk_type] ?? "⚠️"}
        </div>

        <div className="flex-1 min-w-0">
          {/* Top row */}
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <span
              className="text-[10px] font-bold px-1.5 py-0.5 rounded"
              style={{ color, backgroundColor: `${color}15`, border: `1px solid ${color}30` }}
            >
              {alert.level}
            </span>
            <span className="text-xs font-semibold text-white">{alert.province_name}</span>
            <span className="text-[10px] text-white/30 font-mono">{alert.risk_type}</span>
            <span className="text-[10px] text-white/25 ml-auto">{timeAgo(alert.issued_at)}</span>
          </div>

          {/* Message */}
          <p className="text-[11px] text-white/60 leading-relaxed mb-2 line-clamp-2">
            {alert.message}
          </p>

          {/* Stats */}
          <div className="flex items-center gap-4">
            <StatChip label="Policies" value={alert.affected_policies.toLocaleString()} color="#60a5fa" />
            <StatChip label="Est. Loss" value={`$${(alert.estimated_loss_usd / 1_000_000).toFixed(1)}M`} color={color} />
            <span className="text-[10px] text-white/20 ml-auto font-mono">{alert.id}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function StatChip({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="flex items-center gap-1">
      <span className="text-[10px] text-white/30">{label}:</span>
      <span className="text-[11px] font-semibold" style={{ color }}>{value}</span>
    </div>
  );
}
