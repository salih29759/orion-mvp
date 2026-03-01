"use client";
import { useState } from "react";

const BASE_URL = "https://api.orionlabs.io/v1";

interface Endpoint {
  method: "GET" | "POST" | "DELETE";
  path: string;
  description: string;
  params?: { name: string; type: string; required: boolean; desc: string }[];
  response: object;
  tag: string;
}

const endpoints: Endpoint[] = [
  {
    method: "GET",
    path: "/risk/provinces",
    tag: "Risk Scores",
    description:
      "Returns current climate risk scores for all monitored Turkish provinces. Includes flood score, drought score, overall composite risk, trend data, and insured asset exposure.",
    params: [
      { name: "region", type: "string", required: false, desc: "Filter by region: marmara | aegean | mediterranean | central | eastern | black_sea | southeastern" },
      { name: "min_score", type: "integer", required: false, desc: "Minimum overall risk score (0–100)" },
      { name: "risk_level", type: "string", required: false, desc: "Filter: HIGH | MEDIUM | LOW" },
      { name: "limit", type: "integer", required: false, desc: "Number of results to return (default: 81, max: 81)" },
    ],
    response: {
      status: "success",
      generated_at: "2024-01-15T09:42:18Z",
      model_version: "orion-climate-v2.1",
      confidence_score: 0.942,
      data: [
        {
          id: "TRA",
          name: "Trabzon",
          region: "black_sea",
          risk_scores: {
            overall: 91,
            flood: 88,
            drought: 18,
            wildfire: 12,
            earthquake: 45,
          },
          risk_level: "HIGH",
          trend: {
            direction: "up",
            change_30d_pct: 11.2,
            change_90d_pct: 18.4,
          },
          exposure: {
            population: 811902,
            insured_assets_usd_m: 260,
            policy_count: 34820,
          },
          last_updated: "2024-01-15T09:30:00Z",
        },
        {
          id: "IST",
          name: "Istanbul",
          region: "marmara",
          risk_scores: {
            overall: 87,
            flood: 82,
            drought: 38,
            wildfire: 22,
            earthquake: 78,
          },
          risk_level: "HIGH",
          trend: {
            direction: "up",
            change_30d_pct: 4.2,
            change_90d_pct: 9.1,
          },
          exposure: {
            population: 15840900,
            insured_assets_usd_m: 4820,
            policy_count: 284500,
          },
          last_updated: "2024-01-15T09:30:00Z",
        },
      ],
      pagination: {
        total: 81,
        returned: 2,
        page: 1,
        per_page: 20,
      },
    },
  },
  {
    method: "GET",
    path: "/risk/provinces/{id}",
    tag: "Risk Scores",
    description:
      "Returns detailed risk profile for a single province including 30-day historical scores, sub-peril breakdown, and portfolio impact summary.",
    params: [
      { name: "id", type: "string", required: true, desc: "Province code (e.g. IST, ANK, IZM)" },
      { name: "include_history", type: "boolean", required: false, desc: "Include 30-day score history (default: false)" },
    ],
    response: {
      status: "success",
      data: {
        id: "IST",
        name: "Istanbul",
        risk_scores: {
          overall: 87,
          flood: 82,
          drought: 38,
        },
        history_30d: [
          { date: "2024-01-01", overall: 81 },
          { date: "2024-01-08", overall: 83 },
          { date: "2024-01-15", overall: 87 },
        ],
        portfolio_impact: {
          policies_at_risk: 12450,
          estimated_loss_usd_m: 218.6,
          var_95_usd_m: 312.4,
        },
      },
    },
  },
  {
    method: "GET",
    path: "/alerts/active",
    tag: "Alerts",
    description:
      "Returns all currently active climate risk alerts. Alerts are generated when province risk scores cross configured thresholds or when meteorological triggers are detected.",
    params: [
      { name: "level", type: "string", required: false, desc: "Filter by level: HIGH | MEDIUM | LOW" },
      { name: "risk_type", type: "string", required: false, desc: "Filter by type: FLOOD | DROUGHT | WILDFIRE | EARTHQUAKE" },
      { name: "province_id", type: "string", required: false, desc: "Filter by province code" },
    ],
    response: {
      status: "success",
      alert_count: 6,
      data: [
        {
          id: "ALT-001",
          province_id: "TRA",
          province_name: "Trabzon",
          risk_type: "FLOOD",
          level: "HIGH",
          message:
            "Extreme flood risk detected. Heavy rainfall forecast (180mm/48h). Immediate policy review recommended.",
          issued_at: "2024-01-15T06:00:00Z",
          expires_at: "2024-01-17T06:00:00Z",
          affected_policies: 1842,
          estimated_loss_usd_m: 47.3,
          recommended_actions: [
            "Suspend new policy issuance in coastal flood zones",
            "Notify claims team for pre-positioning",
            "Review reinsurance treaty triggers",
          ],
        },
      ],
    },
  },
  {
    method: "POST",
    path: "/portfolio/analyze",
    tag: "Portfolio",
    description:
      "Submit your policy portfolio for climate risk analysis. Returns aggregated risk exposure, province-level breakdown, and estimated loss distributions.",
    params: [
      { name: "policies", type: "array", required: true, desc: "Array of policy objects with province_id, sum_insured, and peril_types" },
      { name: "scenario", type: "string", required: false, desc: "Stress scenario: baseline | rcp45 | rcp85 | extreme (default: baseline)" },
      { name: "horizon_years", type: "integer", required: false, desc: "Forward-looking horizon for scenario analysis (1, 5, 10, 25)" },
    ],
    response: {
      status: "success",
      analysis_id: "ANA-20240115-8842",
      scenario: "rcp45",
      summary: {
        total_policies: 5240,
        total_sum_insured_usd_m: 2840.5,
        portfolio_risk_score: 72,
        expected_annual_loss_usd_m: 41.2,
        var_95_usd_m: 124.8,
        var_99_usd_m: 218.3,
      },
      by_province: [
        {
          province_id: "IST",
          policy_count: 2100,
          risk_score: 87,
          expected_loss_usd_m: 22.4,
        },
      ],
    },
  },
];

const methodColors: Record<string, { bg: string; text: string; border: string }> = {
  GET: { bg: "rgba(34, 197, 94, 0.1)", text: "#22c55e", border: "rgba(34, 197, 94, 0.25)" },
  POST: { bg: "rgba(30, 111, 255, 0.1)", text: "#60a5fa", border: "rgba(30, 111, 255, 0.25)" },
  DELETE: { bg: "rgba(239, 68, 68, 0.1)", text: "#ef4444", border: "rgba(239, 68, 68, 0.25)" },
};

const tagColors: Record<string, string> = {
  "Risk Scores": "#00d4ff",
  Alerts: "#f97316",
  Portfolio: "#a78bfa",
};

export default function ApiDocsPage() {
  const [activeEndpoint, setActiveEndpoint] = useState(0);
  const [copied, setCopied] = useState(false);
  const endpoint = endpoints[activeEndpoint];

  const copyKey = () => {
    navigator.clipboard.writeText("ok_live_xK9mQpR2sVj8nLwA4tY6dB3fC7hE1uZ5");
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="p-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-2">
          <h1 className="text-2xl font-bold text-white tracking-tight">
            API Reference
          </h1>
          <span className="text-[10px] font-bold px-2 py-1 rounded-full bg-blue-500/15 text-blue-400 border border-blue-500/25">
            v1.0
          </span>
          <span className="text-[10px] font-bold px-2 py-1 rounded-full bg-green-500/15 text-green-400 border border-green-500/25">
            REST
          </span>
        </div>
        <p className="text-sm text-white/50">
          Integrate Orion Labs climate risk intelligence directly into your
          underwriting and portfolio systems.
        </p>
      </div>

      {/* API Key */}
      <div className="bg-[#070f1f] rounded-xl border border-white/8 p-4 mb-6">
        <div className="flex items-center justify-between flex-wrap gap-3">
          <div>
            <div className="text-xs font-semibold text-white/50 uppercase tracking-wider mb-1">
              Base URL
            </div>
            <code className="text-sm text-[#00d4ff] font-mono">{BASE_URL}</code>
          </div>
          <div className="h-8 border-l border-white/8 mx-2 hidden sm:block" />
          <div>
            <div className="text-xs font-semibold text-white/50 uppercase tracking-wider mb-1">
              Sample API Key
            </div>
            <div className="flex items-center gap-2">
              <code className="text-sm text-white/70 font-mono bg-white/5 px-3 py-1 rounded-lg border border-white/8">
                ok_live_xK9m...Z5
              </code>
              <button
                onClick={copyKey}
                className="text-xs px-3 py-1.5 rounded-lg bg-blue-500/15 text-blue-400 border border-blue-500/25 hover:bg-blue-500/25 transition-colors font-medium"
              >
                {copied ? "✓ Copied" : "Copy"}
              </button>
            </div>
          </div>
          <div className="h-8 border-l border-white/8 mx-2 hidden sm:block" />
          <div>
            <div className="text-xs font-semibold text-white/50 uppercase tracking-wider mb-1">
              Authentication
            </div>
            <code className="text-sm text-white/60 font-mono">
              Authorization: Bearer {"{api_key}"}
            </code>
          </div>
        </div>
      </div>

      {/* Main docs area */}
      <div className="grid grid-cols-1 xl:grid-cols-4 gap-5">
        {/* Endpoint list sidebar */}
        <div className="xl:col-span-1">
          <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden sticky top-20">
            <div className="px-4 py-3 border-b border-white/8 text-xs font-semibold text-white/50 uppercase tracking-wider">
              Endpoints
            </div>
            <div className="p-2">
              {endpoints.map((ep, i) => {
                const mc = methodColors[ep.method];
                const active = i === activeEndpoint;
                return (
                  <button
                    key={i}
                    onClick={() => setActiveEndpoint(i)}
                    className={`w-full text-left px-3 py-2.5 rounded-lg mb-1 transition-all ${
                      active
                        ? "bg-blue-500/15 border border-blue-500/25"
                        : "hover:bg-white/5 border border-transparent"
                    }`}
                  >
                    <div className="flex items-center gap-2 mb-1">
                      <span
                        className="text-[9px] font-bold px-1.5 py-0.5 rounded font-mono"
                        style={{
                          color: mc.text,
                          backgroundColor: mc.bg,
                          border: `1px solid ${mc.border}`,
                        }}
                      >
                        {ep.method}
                      </span>
                      <span
                        className="text-[9px] font-semibold"
                        style={{
                          color: tagColors[ep.tag] || "#ffffff60",
                        }}
                      >
                        {ep.tag}
                      </span>
                    </div>
                    <code className="text-[11px] text-white/70 font-mono">
                      {ep.path}
                    </code>
                  </button>
                );
              })}
            </div>

            {/* Rate limits */}
            <div className="border-t border-white/8 p-4">
              <div className="text-[10px] text-white/40 uppercase tracking-wider mb-2">
                Rate Limits
              </div>
              <div className="space-y-1.5 text-[11px]">
                <div className="flex justify-between">
                  <span className="text-white/40">Free</span>
                  <span className="text-white/70 font-mono">100/hr</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/40">Pro</span>
                  <span className="text-blue-400 font-mono">10,000/hr</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/40">Enterprise</span>
                  <span className="text-[#00d4ff] font-mono">Unlimited</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Endpoint detail */}
        <div className="xl:col-span-3 space-y-4">
          {/* Endpoint header */}
          <div className="bg-[#070f1f] rounded-xl border border-white/8 p-5">
            <div className="flex items-start gap-3 mb-3">
              <span
                className="text-xs font-bold px-2.5 py-1 rounded font-mono mt-0.5"
                style={{
                  color: methodColors[endpoint.method].text,
                  backgroundColor: methodColors[endpoint.method].bg,
                  border: `1px solid ${methodColors[endpoint.method].border}`,
                }}
              >
                {endpoint.method}
              </span>
              <div>
                <code className="text-base font-bold text-white font-mono">
                  {BASE_URL}
                  {endpoint.path}
                </code>
                <p className="text-sm text-white/50 mt-2 leading-relaxed">
                  {endpoint.description}
                </p>
              </div>
            </div>

            {/* cURL example */}
            <div className="mt-4">
              <div className="text-xs font-semibold text-white/40 uppercase tracking-wider mb-2">
                cURL
              </div>
              <div className="bg-[#030810] rounded-lg p-4 border border-white/5 overflow-x-auto">
                <pre className="code-block text-white/80 whitespace-pre-wrap">
                  <span className="text-[#00d4ff]">curl</span>
                  {" -X "}
                  <span style={{ color: methodColors[endpoint.method].text }}>
                    {endpoint.method}
                  </span>
                  {` \\\n  `}
                  <span className="text-white/50">
                    &quot;{BASE_URL}
                    {endpoint.path}
                    &quot;
                  </span>
                  {` \\\n  `}
                  <span className="text-yellow-400">-H</span>
                  {` "Authorization: Bearer $ORION_API_KEY" \\\n  `}
                  <span className="text-yellow-400">-H</span>
                  {` "Content-Type: application/json"`}
                  {endpoint.method === "POST"
                    ? ` \\\n  -d '{"policies": [...], "scenario": "rcp45"}'`
                    : ""}
                </pre>
              </div>
            </div>
          </div>

          {/* Parameters */}
          {endpoint.params && endpoint.params.length > 0 && (
            <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
              <div className="px-5 py-3 border-b border-white/8">
                <span className="text-sm font-semibold text-white">
                  {endpoint.method === "POST" ? "Request Body" : "Query Parameters"}
                </span>
              </div>
              <div className="divide-y divide-white/5">
                {endpoint.params.map((param) => (
                  <div
                    key={param.name}
                    className="px-5 py-3.5 flex items-start gap-4"
                  >
                    <div className="min-w-[140px]">
                      <code className="text-sm font-bold text-[#00d4ff] font-mono">
                        {param.name}
                      </code>
                      <div className="flex items-center gap-1.5 mt-1">
                        <span className="text-[10px] text-white/40 font-mono bg-white/5 px-1.5 py-0.5 rounded">
                          {param.type}
                        </span>
                        {param.required ? (
                          <span className="text-[9px] font-bold text-red-400 bg-red-500/10 border border-red-500/20 px-1.5 py-0.5 rounded">
                            required
                          </span>
                        ) : (
                          <span className="text-[9px] text-white/25 bg-white/5 border border-white/8 px-1.5 py-0.5 rounded">
                            optional
                          </span>
                        )}
                      </div>
                    </div>
                    <p className="text-sm text-white/55 leading-relaxed">
                      {param.desc}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Response */}
          <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
            <div className="px-5 py-3 border-b border-white/8 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className="text-sm font-semibold text-white">
                  Response
                </span>
                <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-green-500/15 text-green-400 border border-green-500/25">
                  200 OK
                </span>
              </div>
              <span className="text-xs text-white/30 font-mono">
                application/json
              </span>
            </div>
            <div className="p-4 overflow-x-auto max-h-[480px] overflow-y-auto">
              <JsonDisplay data={endpoint.response} />
            </div>
          </div>

          {/* Error codes */}
          <div className="bg-[#070f1f] rounded-xl border border-white/8 overflow-hidden">
            <div className="px-5 py-3 border-b border-white/8">
              <span className="text-sm font-semibold text-white">
                Error Codes
              </span>
            </div>
            <div className="divide-y divide-white/5">
              {[
                { code: "400", color: "#f97316", label: "Bad Request", desc: "Invalid parameters or malformed request body" },
                { code: "401", color: "#ef4444", label: "Unauthorized", desc: "Missing or invalid API key" },
                { code: "403", color: "#ef4444", label: "Forbidden", desc: "API key does not have access to this endpoint" },
                { code: "429", color: "#f59e0b", label: "Rate Limited", desc: "You have exceeded your plan's rate limit" },
                { code: "500", color: "#a78bfa", label: "Server Error", desc: "Internal error — contact support@orionlabs.io" },
              ].map((e) => (
                <div key={e.code} className="px-5 py-3 flex items-center gap-4">
                  <code
                    className="text-sm font-bold font-mono w-12 shrink-0"
                    style={{ color: e.color }}
                  >
                    {e.code}
                  </code>
                  <span className="text-sm font-medium text-white/70 w-28 shrink-0">
                    {e.label}
                  </span>
                  <span className="text-sm text-white/40">{e.desc}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function JsonDisplay({ data }: { data: unknown }) {
  const json = JSON.stringify(data, null, 2);
  // Simple syntax highlighting
  const highlighted = json
    .replace(/("[\w_]+"):/g, '<span style="color:#00d4ff">$1</span>:')
    .replace(/: (".*?")/g, ': <span style="color:#a5d6a7">$1</span>')
    .replace(/: (true|false)/g, ': <span style="color:#ffb74d">$1</span>')
    .replace(/: (\d+\.?\d*)/g, ': <span style="color:#ce93d8">$1</span>')
    .replace(/: (null)/g, ': <span style="color:#ef9a9a">$1</span>');

  return (
    <pre
      className="code-block text-white/75"
      dangerouslySetInnerHTML={{ __html: highlighted }}
    />
  );
}
