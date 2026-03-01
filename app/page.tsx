import TurkeyRiskMap from "@/components/TurkeyRiskMap";
import TopRiskProvinces from "@/components/TopRiskProvinces";
import AlertPanel from "@/components/AlertPanel";
import StatsBar from "@/components/StatsBar";
import DataFreshness from "@/components/DataFreshness";
import { DashboardProvider } from "@/lib/useDashboard";

export default function DashboardPage() {
  const now = new Date().toLocaleString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "Europe/Istanbul",
    timeZoneName: "short",
  });

  return (
    <DashboardProvider>
      <div className="p-6 space-y-5 max-w-[1600px] mx-auto">
        {/* Page header */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-bold text-white tracking-tight">
              Risk Intelligence Dashboard
            </h1>
            <p className="text-sm text-white/40 mt-1">
              Real-time climate risk monitoring for Turkish insurance portfolios
            </p>
          </div>
          <div className="text-right hidden sm:block">
            <div className="text-xs text-white/30 font-mono">{now}</div>
            <div className="text-xs text-white/20 mt-0.5">
              Model: Orion-Climate-v2.1
            </div>
            <DataFreshness />
          </div>
        </div>

        {/* Stats row */}
        <StatsBar />

        {/* Main grid */}
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
          {/* Map — takes 2 columns */}
          <div className="xl:col-span-2">
            <TurkeyRiskMap />
          </div>

          {/* Alert panel */}
          <div className="xl:col-span-1">
            <AlertPanel />
          </div>
        </div>

        {/* Bottom: top provinces + model info */}
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
          <div className="xl:col-span-2">
            <TopRiskProvinces />
          </div>

          {/* Model info card */}
          <div className="xl:col-span-1 space-y-3">
            {/* Data sources */}
            <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
              <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-3">
                Data Sources
              </div>
              <div className="space-y-2">
                {[
                  { label: "ERA5 Reanalysis", status: "live", detail: "Hourly weather" },
                  { label: "Copernicus EMS", status: "live", detail: "Flood monitoring" },
                  { label: "DSİ Hydrology", status: "live", detail: "River levels" },
                  { label: "AFAD Risk DB", status: "synced", detail: "Hazard zones" },
                  { label: "OSM Infrastructure", status: "synced", detail: "Asset exposure" },
                ].map((src) => (
                  <div key={src.label} className="flex items-center gap-3 py-1.5">
                    <div
                      className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                        src.status === "live" ? "bg-green-400" : "bg-blue-400"
                      }`}
                    />
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-medium text-white/80">{src.label}</div>
                      <div className="text-[10px] text-white/30">{src.detail}</div>
                    </div>
                    <div
                      className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${
                        src.status === "live"
                          ? "bg-green-500/15 text-green-400 border border-green-500/20"
                          : "bg-blue-500/15 text-blue-400 border border-blue-500/20"
                      }`}
                    >
                      {src.status}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Model accuracy */}
            <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
              <div className="text-xs font-semibold text-white/60 uppercase tracking-wider mb-3">
                Model Performance
              </div>
              <div className="space-y-2.5">
                {[
                  { label: "Flood Prediction", pct: 94, color: "#3b82f6" },
                  { label: "Drought Forecast", pct: 89, color: "#f59e0b" },
                  { label: "Loss Estimation", pct: 87, color: "#a78bfa" },
                ].map((m) => (
                  <div key={m.label}>
                    <div className="flex justify-between mb-1">
                      <span className="text-[11px] text-white/60">{m.label}</span>
                      <span className="text-[11px] font-bold" style={{ color: m.color }}>
                        {m.pct}%
                      </span>
                    </div>
                    <div className="h-1.5 bg-white/8 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full"
                        style={{
                          width: `${m.pct}%`,
                          backgroundColor: m.color,
                          boxShadow: `0 0 8px ${m.color}60`,
                        }}
                      />
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-4 pt-3 border-t border-white/8 text-[10px] text-white/30 font-mono">
                Last trained: Jan 10, 2024 · v2.1.4
              </div>
            </div>
          </div>
        </div>

        {/* Footer note */}
        <div className="text-center py-2">
          <p className="text-[11px] text-white/20">
            Orion Labs Climate Risk Intelligence Platform ·{" "}
            <span className="text-[#1e6fff]/60">Pro</span> · Data refreshes every
            15 minutes · For authorized use only
          </p>
        </div>
      </div>
    </DashboardProvider>
  );
}
