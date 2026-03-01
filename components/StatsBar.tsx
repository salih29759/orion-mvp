"use client";

import { useDashboard } from "@/lib/useDashboard";

export default function StatsBar() {
  const { provinces, alerts, loading, error } = useDashboard();

  if (error) {
    return (
      <div className="rounded-xl border border-red-500/20 bg-red-500/5 p-4 text-center">
        <p className="text-xs text-red-400">
          ⚠ Could not connect to Orion API —{" "}
          <span className="font-mono text-[11px]">{error}</span>
        </p>
      </div>
    );
  }

  const high = loading ? "—" : provinces.filter((p) => p.risk_level === "HIGH").length.toString();
  const medium = loading ? "—" : provinces.filter((p) => p.risk_level === "MEDIUM").length.toString();
  const low = loading ? "—" : provinces.filter((p) => p.risk_level === "LOW").length.toString();
  const totalPolicies = loading
    ? "—"
    : alerts.reduce((s, a) => s + a.affected_policies, 0).toLocaleString();
  const totalExposure = loading
    ? "—"
    : `$${(alerts.reduce((s, a) => s + a.estimated_loss_usd, 0) / 1_000_000).toFixed(1)}M`;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
      <StatCard
        label="High Risk Provinces"
        value={high}
        sub={loading ? "loading…" : `of ${provinces.length} monitored`}
        color="#ef4444"
        loading={loading}
        icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        }
      />
      <StatCard
        label="Medium Risk"
        value={medium}
        sub="provinces"
        color="#f97316"
        loading={loading}
        icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        }
      />
      <StatCard
        label="Low Risk"
        value={low}
        sub="provinces"
        color="#22c55e"
        loading={loading}
        icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        }
      />
      <StatCard
        label="Affected Policies"
        value={totalPolicies}
        sub="under active alerts"
        color="#60a5fa"
        loading={loading}
        icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
        }
      />
      <StatCard
        label="Estimated Exposure"
        value={totalExposure}
        sub="potential loss"
        color="#a78bfa"
        loading={loading}
        icon={
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        }
      />
    </div>
  );
}

function StatCard({
  label,
  value,
  sub,
  color,
  icon,
  loading,
}: {
  label: string;
  value: string;
  sub: string;
  color: string;
  icon: React.ReactNode;
  loading: boolean;
}) {
  return (
    <div
      className="rounded-xl p-4 border card-hover shimmer"
      style={{
        background: `linear-gradient(135deg, ${color}08, ${color}03)`,
        borderColor: `${color}20`,
      }}
    >
      <div className="flex items-start justify-between mb-3">
        <div style={{ color: `${color}90` }}>{icon}</div>
        <div
          className="w-1.5 h-1.5 rounded-full mt-1"
          style={{ backgroundColor: color, opacity: loading ? 0.2 : 0.6 }}
        />
      </div>
      {loading ? (
        <div
          className="h-8 w-16 rounded mb-1 animate-pulse"
          style={{ backgroundColor: `${color}15` }}
        />
      ) : (
        <div className="text-2xl font-bold text-white mb-0.5">{value}</div>
      )}
      <div className="text-[11px] font-medium text-white/70">{label}</div>
      <div className="text-[10px] text-white/30 mt-0.5">{sub}</div>
    </div>
  );
}
