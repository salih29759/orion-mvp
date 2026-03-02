"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Building2,
  Globe2,
  BarChart3,
  Bell,
  Code2,
} from "lucide-react";
import { useNotifications } from "@/hooks/useApi";
import { useGlobalStore } from "@/lib/store";

interface NavItem {
  label:    string;
  href:     string;
  icon:     React.ReactNode;
  badge?:   string | number;
  disabled?: boolean;
}

const NAV_SECTIONS = [
  {
    heading: "Analysis",
    items: [
      { label: "Portfolio",     href: "/portfolio",   icon: <LayoutDashboard size={16} /> },
      { label: "Assets",        href: "/assets",      icon: <Building2 size={16} /> },
      { label: "Geostrategy",   href: "/geostrategy", icon: <Globe2 size={16} /> },
    ] as NavItem[],
  },
  {
    heading: "Risk",
    items: [
      { label: "Scenario Analysis", href: "/scenario",      icon: <BarChart3 size={16} />, badge: "BETA" },
      { label: "Notifications",     href: "/notifications", icon: <Bell size={16} /> },
    ] as NavItem[],
  },
  {
    heading: "Developer",
    items: [
      { label: "API Docs", href: "/api-docs", icon: <Code2 size={16} />, badge: "Soon", disabled: true },
    ] as NavItem[],
  },
];

export function Sidebar() {
  const pathname = usePathname();
  const { selectedPortfolioId } = useGlobalStore();
  const { data: notifs } = useNotifications(selectedPortfolioId ?? undefined);
  const unread = notifs?.filter((n) => !n.acknowledged_at).length ?? 0;

  const isActive = (href: string) =>
    href === "/" ? pathname === "/" : pathname.startsWith(href);

  return (
    <aside
      className="w-[220px] shrink-0 flex flex-col h-screen sticky top-0"
      style={{ backgroundColor: "var(--bg-sidebar)" }}
    >
      {/* Logo */}
      <div className="px-5 py-5 border-b" style={{ borderColor: "rgba(255,255,255,0.08)" }}>
        <div className="flex items-center gap-2">
          <span className="font-serif text-[22px] leading-none" style={{ color: "var(--text-sidebar)" }}>
            ORION
          </span>
          <span
            className="text-[9px] font-bold uppercase tracking-widest px-1.5 py-0.5 rounded"
            style={{ backgroundColor: "var(--accent)", color: "#fff" }}
          >
            LABS
          </span>
        </div>
        <p
          className="text-[10px] uppercase tracking-widest mt-1"
          style={{ color: "var(--text-sidebar-muted)" }}
        >
          Climate Risk Intelligence
        </p>
      </div>

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto py-4 px-3 space-y-5">
        {NAV_SECTIONS.map((section) => (
          <div key={section.heading}>
            <p
              className="text-[10px] uppercase tracking-widest px-2 mb-1.5"
              style={{ color: "var(--text-sidebar-muted)" }}
            >
              {section.heading}
            </p>
            <div className="space-y-0.5">
              {section.items.map((item) => {
                const active = isActive(item.href);
                const notifBadge = item.href === "/notifications" && unread > 0 ? unread : undefined;
                return (
                  <Link
                    key={item.href}
                    href={item.disabled ? "#" : item.href}
                    onClick={item.disabled ? (e) => e.preventDefault() : undefined}
                    className={`flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-sm transition-all group ${
                      item.disabled ? "opacity-40 cursor-not-allowed" : ""
                    }`}
                    style={{
                      color:           active ? "#fff" : "var(--text-sidebar-muted)",
                      backgroundColor: active ? "rgba(255,255,255,0.08)" : "transparent",
                      borderLeft:      active ? "2px solid #fff" : "2px solid transparent",
                    }}
                  >
                    <span>{item.icon}</span>
                    <span className="flex-1 font-medium">{item.label}</span>
                    {notifBadge !== undefined && (
                      <span
                        className="text-[10px] font-bold px-1.5 py-0.5 rounded-full"
                        style={{ backgroundColor: "var(--extreme)", color: "#fff" }}
                      >
                        {notifBadge}
                      </span>
                    )}
                    {item.badge && notifBadge === undefined && (
                      <span
                        className="text-[9px] font-bold uppercase px-1.5 py-0.5 rounded"
                        style={{ backgroundColor: "rgba(255,255,255,0.1)", color: "var(--text-sidebar-muted)" }}
                      >
                        {item.badge}
                      </span>
                    )}
                  </Link>
                );
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* Data coverage */}
      <div className="px-4 py-4 border-t" style={{ borderColor: "rgba(255,255,255,0.08)" }}>
        <div
          className="rounded-lg p-3 text-[11px] space-y-1.5"
          style={{ backgroundColor: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)" }}
        >
          <p className="text-[10px] uppercase tracking-widest" style={{ color: "var(--text-sidebar-muted)" }}>
            Data Coverage
          </p>
          <p className="font-semibold" style={{ color: "var(--text-sidebar)" }}>Turkey · 81 İl</p>
          <p style={{ color: "var(--text-sidebar-muted)" }}>ERA5 · v1_baseline</p>
          <div className="h-1 rounded-full overflow-hidden" style={{ backgroundColor: "rgba(255,255,255,0.1)" }}>
            <div className="h-full rounded-full" style={{ width: "82%", backgroundColor: "var(--accent)" }} />
          </div>
        </div>

        {/* User */}
        <div className="flex items-center gap-2.5 mt-3">
          <div
            className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold"
            style={{ backgroundColor: "var(--accent)", color: "#fff" }}
          >
            SD
          </div>
          <div>
            <p className="text-xs font-medium" style={{ color: "var(--text-sidebar)" }}>Salih Durmus</p>
            <p className="text-[10px]" style={{ color: "var(--text-sidebar-muted)" }}>Pro Plan</p>
          </div>
        </div>
      </div>
    </aside>
  );
}

export default Sidebar;
