"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

export default function Header() {
  const pathname = usePathname();

  return (
    <header className="h-16 border-b border-white/8 bg-[#0a1628]/95 backdrop-blur-md sticky top-0 z-50 flex items-center px-6 justify-between">
      {/* Logo */}
      <Link href="/" className="flex items-center gap-3 group">
        {/* SVG Logo Mark */}
        <div className="relative w-9 h-9">
          <svg viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg" className="w-9 h-9">
            {/* Outer ring */}
            <circle cx="18" cy="18" r="17" stroke="url(#ring-grad)" strokeWidth="1.5" />
            {/* Inner circle */}
            <circle cx="18" cy="18" r="8" fill="url(#center-grad)" />
            {/* Cross hairs */}
            <line x1="18" y1="2" x2="18" y2="10" stroke="#00d4ff" strokeWidth="1.5" strokeLinecap="round" />
            <line x1="18" y1="26" x2="18" y2="34" stroke="#00d4ff" strokeWidth="1.5" strokeLinecap="round" />
            <line x1="2" y1="18" x2="10" y2="18" stroke="#00d4ff" strokeWidth="1.5" strokeLinecap="round" />
            <line x1="26" y1="18" x2="34" y2="18" stroke="#00d4ff" strokeWidth="1.5" strokeLinecap="round" />
            {/* Star dot */}
            <circle cx="18" cy="18" r="3" fill="#ffffff" />
            <defs>
              <linearGradient id="ring-grad" x1="0" y1="0" x2="36" y2="36">
                <stop offset="0%" stopColor="#1e6fff" />
                <stop offset="100%" stopColor="#00d4ff" />
              </linearGradient>
              <radialGradient id="center-grad" cx="50%" cy="50%">
                <stop offset="0%" stopColor="#1e6fff" stopOpacity="0.4" />
                <stop offset="100%" stopColor="#00d4ff" stopOpacity="0.1" />
              </radialGradient>
            </defs>
          </svg>
          {/* Glow */}
          <div className="absolute inset-0 rounded-full bg-blue-500/10 blur-md group-hover:bg-cyan-400/20 transition-all" />
        </div>
        <div>
          <span className="text-xl font-bold tracking-tight text-white">
            ORION
          </span>
          <span className="text-xl font-light tracking-tight text-[#00d4ff] ml-1">
            LABS
          </span>
          <div className="text-[10px] text-white/40 font-medium tracking-widest uppercase leading-none mt-0.5">
            Climate Risk Intelligence
          </div>
        </div>
      </Link>

      {/* Center nav */}
      <nav className="hidden md:flex items-center gap-1">
        {[
          { label: "Dashboard", href: "/" },
          { label: "API Docs", href: "/api-docs" },
        ].map((item) => (
          <Link
            key={item.href}
            href={item.href}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              pathname === item.href
                ? "bg-blue-500/20 text-[#00d4ff] border border-blue-500/30"
                : "text-white/60 hover:text-white hover:bg-white/5"
            }`}
          >
            {item.label}
          </Link>
        ))}
      </nav>

      {/* Right side */}
      <div className="flex items-center gap-4">
        {/* Live indicator */}
        <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-full bg-green-500/10 border border-green-500/20">
          <div className="w-2 h-2 rounded-full bg-green-400 pulse-dot" />
          <span className="text-xs text-green-400 font-medium">LIVE</span>
        </div>

        {/* Alert badge */}
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-red-500/10 border border-red-500/20">
          <svg className="w-3.5 h-3.5 text-red-400" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
          </svg>
          <span className="text-xs text-red-400 font-semibold">3 HIGH</span>
        </div>

        {/* User */}
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center text-xs font-bold text-white">
            RL
          </div>
          <div className="hidden sm:block text-right">
            <div className="text-xs font-medium text-white">Risk Analyst</div>
            <div className="text-[10px] text-white/40">Pro Plan</div>
          </div>
        </div>
      </div>
    </header>
  );
}
