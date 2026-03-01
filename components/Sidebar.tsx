"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
  {
    icon: (
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
      </svg>
    ),
    label: "Dashboard",
    href: "/",
  },
  {
    icon: (
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
      </svg>
    ),
    label: "Risk Map",
    href: "/",
    badge: null,
  },
  {
    icon: (
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
      </svg>
    ),
    label: "Alerts",
    href: "/",
    badge: "6",
  },
  {
    icon: (
      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
      </svg>
    ),
    label: "API Docs",
    href: "/api-docs",
    badge: null,
  },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-16 md:w-56 border-r border-white/8 bg-[#0a1628]/80 flex flex-col py-4 shrink-0">
      <nav className="flex flex-col gap-1 px-2">
        {navItems.map((item) => {
          const isActive = pathname === item.href && item.href !== "/" || (item.href === "/" && pathname === "/");
          return (
            <Link
              key={item.label}
              href={item.href}
              className={`flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all group shimmer ${
                isActive
                  ? "bg-blue-500/15 text-[#00d4ff] border border-blue-500/25"
                  : "text-white/50 hover:text-white hover:bg-white/5"
              }`}
            >
              <span className={isActive ? "text-[#00d4ff]" : "text-white/40 group-hover:text-white/80 transition-colors"}>
                {item.icon}
              </span>
              <span className="hidden md:block text-sm font-medium">{item.label}</span>
              {item.badge && (
                <span className="hidden md:flex ml-auto text-[10px] font-bold bg-red-500/20 text-red-400 border border-red-500/30 px-1.5 py-0.5 rounded-full">
                  {item.badge}
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      {/* Bottom section */}
      <div className="mt-auto px-2">
        <div className="border-t border-white/8 pt-4 pb-2">
          <div className="hidden md:block px-3 py-3 rounded-lg bg-blue-500/5 border border-blue-500/10">
            <div className="text-[10px] text-white/40 uppercase tracking-widest mb-2">Data Coverage</div>
            <div className="text-sm font-bold text-white">81 Provinces</div>
            <div className="text-[11px] text-white/40 mt-0.5">Last sync: 2 min ago</div>
            <div className="mt-2 h-1 bg-white/10 rounded-full overflow-hidden">
              <div className="h-full w-[82%] bg-gradient-to-r from-blue-500 to-cyan-400 rounded-full" />
            </div>
          </div>
        </div>
      </div>
    </aside>
  );
}
