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
        navy: {
          950: "#050d1a",
          900: "#0a1628",
          800: "#0f2040",
          700: "#132a55",
          600: "#1a3a6e",
        },
        orion: {
          blue: "#1e6fff",
          cyan: "#00d4ff",
          glow: "#0099ff",
        },
        risk: {
          high: "#ef4444",
          "high-bg": "#7f1d1d",
          medium: "#f97316",
          "medium-bg": "#7c2d12",
          low: "#22c55e",
          "low-bg": "#14532d",
        },
      },
      fontFamily: {
        mono: ["'JetBrains Mono'", "'Fira Code'", "monospace"],
      },
      boxShadow: {
        glow: "0 0 20px rgba(30, 111, 255, 0.3)",
        "glow-cyan": "0 0 20px rgba(0, 212, 255, 0.2)",
        card: "0 4px 24px rgba(0, 0, 0, 0.4)",
      },
      animation: {
        pulse: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "fade-in": "fadeIn 0.5s ease-in-out",
      },
      keyframes: {
        fadeIn: {
          "0%": { opacity: "0", transform: "translateY(8px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
      },
    },
  },
  plugins: [],
};
export default config;
