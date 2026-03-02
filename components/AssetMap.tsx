"use client";

import { useEffect, useRef } from "react";
import type { BandKey } from "@/types";

const BAND_COLOR_HEX: Record<BandKey, string> = {
  minimal:  "#8BBF8B",
  minor:    "#C8C84A",
  moderate: "#E8903A",
  major:    "#D44A2A",
  extreme:  "#AA1A1A",
};

interface AssetMapProps {
  lat:    number;
  lon:    number;
  name:   string;
  score?: number | null;
  band?:  BandKey | null;
}

export function AssetMap({ lat, lon, name, score, band }: AssetMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapRef = useRef<any>(null);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    let cancelled = false;

    import("mapbox-gl").then((mapboxgl) => {
      if (cancelled || !containerRef.current) return;

      // inject Mapbox CSS once
      if (!document.getElementById("mapbox-css")) {
        const link = document.createElement("link");
        link.id = "mapbox-css";
        link.rel = "stylesheet";
        link.href = "https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css";
        document.head.appendChild(link);
      }

      mapboxgl.default.accessToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN ?? "";

      const map = new mapboxgl.default.Map({
        container: containerRef.current!,
        style: "mapbox://styles/mapbox/satellite-streets-v12",
        center: [lon, lat],
        zoom: 10,
        attributionControl: false,
      });

      mapRef.current = map;

      map.addControl(new mapboxgl.default.NavigationControl());
      map.addControl(new mapboxgl.default.AttributionControl({ compact: true }), "bottom-right");

      map.on("load", () => {
        if (cancelled) return;

        // Custom HTML marker — colored circle with score inside
        const markerColor = band ? BAND_COLOR_HEX[band] : "#1B3A4B";
        const el = document.createElement("div");
        el.style.cssText = `
          width: 40px; height: 40px;
          border-radius: 50%;
          background: ${markerColor};
          border: 3px solid #fff;
          display: flex; align-items: center; justify-content: center;
          color: #fff;
          font-family: 'JetBrains Mono', monospace;
          font-size: 12px;
          font-weight: 700;
          box-shadow: 0 2px 8px rgba(0,0,0,0.3);
          cursor: pointer;
        `;
        el.textContent = score != null ? String(Math.round(score)) : "—";

        const popup = new mapboxgl.default.Popup({ offset: 28, closeButton: false })
          .setHTML(`
            <div style="font-family:'DM Sans',sans-serif;padding:4px;min-width:160px;">
              <div style="font-weight:600;font-size:13px;color:#111;">${name}</div>
              <div style="font-size:11px;color:#9A9590;margin-top:2px;">${lat.toFixed(4)}, ${lon.toFixed(4)}</div>
              ${score != null ? `<div style="font-weight:700;font-size:16px;color:${markerColor};margin-top:6px;">${Math.round(score)}/100</div>` : ""}
            </div>
          `);

        new mapboxgl.default.Marker({ element: el })
          .setLngLat([lon, lat])
          .setPopup(popup)
          .addTo(map);

        // TODO Phase 2: add wildfire points layer
        // Source: /api/firms/nearby?lat=&lon=&radius_km=50
        // Layer: circle layer, orange dots, radius by fire intensity
      });
    });

    return () => {
      cancelled = true;
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
    };
  }, [lat, lon, name, score, band]);

  return (
    <div className="relative w-full h-full" style={{ minHeight: 280 }}>
      <div ref={containerRef} className="w-full h-full" style={{ minHeight: 280 }} />
    </div>
  );
}
