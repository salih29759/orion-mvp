import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        page:    "var(--bg-page)",
        surface: "var(--bg-surface)",
        sidebar: "var(--bg-sidebar)",
        border:  "var(--border)",
        "border-strong": "var(--border-strong)",
        primary:   "var(--text-primary)",
        secondary: "var(--text-secondary)",
        muted:     "var(--text-muted)",
        accent:    "var(--accent)",
        band: {
          minimal:  "var(--minimal)",
          minor:    "var(--minor)",
          moderate: "var(--moderate)",
          major:    "var(--major)",
          extreme:  "var(--extreme)",
        },
        peril: {
          heat:     "var(--heat)",
          rain:     "var(--rain)",
          wind:     "var(--wind)",
          drought:  "var(--drought)",
          wildfire: "var(--wildfire)",
          flood:    "var(--flood)",
        },
      },
      fontFamily: {
        serif: ["'DM Serif Display'", "Georgia", "serif"],
        sans:  ["'DM Sans'", "system-ui", "sans-serif"],
        mono:  ["'JetBrains Mono'", "'Fira Code'", "monospace"],
      },
      fontSize: {
        "score-xl": ["72px", { lineHeight: "1", fontWeight: "400" }],
        "score-lg": ["48px", { lineHeight: "1", fontWeight: "400" }],
        "score-md": ["36px", { lineHeight: "1", fontWeight: "400" }],
      },
      boxShadow: {
        card:  "0 1px 4px rgba(0,0,0,0.08), 0 0 0 1px var(--border)",
        float: "0 4px 24px rgba(0,0,0,0.12)",
        popup: "0 8px 32px rgba(0,0,0,0.16)",
      },
      animation: {
        pulse: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        shimmer: "shimmer 1.4s infinite linear",
      },
    },
  },
  plugins: [],
};
export default config;
