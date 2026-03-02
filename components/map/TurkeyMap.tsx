"use client";

import { useEffect, useRef, useState } from "react";
import { RISK_BY_NAME, type ProvinceRisk } from "@/lib/turkeyRiskData";

interface TurkeyMapProps {
  onProvinceClick?: (province: ProvinceRisk) => void;
}

export function TurkeyMap({ onProvinceClick }: TurkeyMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapRef       = useRef<any>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    let cancelled = false;

    import("mapbox-gl").then(async (mapboxgl) => {
      if (cancelled || !containerRef.current) return;

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
        center: [35.2, 38.9],
        zoom: 5.2,
        minZoom: 4,
        attributionControl: false,
      });

      mapRef.current = map;
      map.addControl(new mapboxgl.default.NavigationControl(), "top-right");
      map.addControl(new mapboxgl.default.AttributionControl({ compact: true }), "bottom-right");

      map.on("load", async () => {
        if (cancelled) return;

        // Fetch Turkey provinces GeoJSON
        let geoJson: GeoJSON.FeatureCollection | null = null;
        try {
          const res = await fetch(
            "https://raw.githubusercontent.com/alpers/Turkey-Maps-GeoJSON/master/tr-cities-utf8.json"
          );
          if (res.ok) geoJson = await res.json();
        } catch {
          // CDN failed — skip choropleth, just show markers below
        }

        if (geoJson && !cancelled) {
          // Attach risk scores to features
          const features = geoJson.features.map((f) => {
            const props = f.properties ?? {};
            const name  = props.name ?? props.NAME ?? props.il ?? "";
            const pData = RISK_BY_NAME[name] ?? RISK_BY_NAME[name.toLowerCase()];
            return {
              ...f,
              properties: {
                ...props,
                risk_score: pData?.risk ?? 0,
                province_name: pData?.nameEn ?? name,
              },
            };
          });

          map.addSource("provinces", {
            type: "geojson",
            data: { ...geoJson, features },
          });

          // Choropleth fill
          map.addLayer({
            id: "province-fill",
            type: "fill",
            source: "provinces",
            paint: {
              "fill-color": [
                "interpolate", ["linear"],
                ["get", "risk_score"],
                0,   "#8BBF8B",
                25,  "#C8C84A",
                50,  "#E8903A",
                75,  "#D44A2A",
                100, "#AA1A1A",
              ],
              "fill-opacity": 0.70,
            },
          });

          // Province outlines
          map.addLayer({
            id: "province-outline",
            type: "line",
            source: "provinces",
            paint: {
              "line-color": "#FFFFFF",
              "line-width": 0.8,
              "line-opacity": 0.6,
            },
          });

          // Hover highlight layer
          map.addLayer({
            id: "province-hover",
            type: "line",
            source: "provinces",
            paint: {
              "line-color": "#FFFFFF",
              "line-width": 2.5,
              "line-opacity": 0,
            },
          });

          // Hover interactions
          let hoveredId: string | number | null = null;

          map.on("mousemove", "province-fill", (e) => {
            if (e.features && e.features.length > 0) {
              map.getCanvas().style.cursor = "pointer";
              const f = e.features[0];
              if (hoveredId !== null) {
                map.setFeatureState({ source: "provinces", id: hoveredId }, { hover: false });
              }
              hoveredId = f.id ?? null;
              if (hoveredId !== null) {
                map.setFeatureState({ source: "provinces", id: hoveredId }, { hover: true });
              }
            }
          });

          map.on("mouseleave", "province-fill", () => {
            map.getCanvas().style.cursor = "";
            if (hoveredId !== null) {
              map.setFeatureState({ source: "provinces", id: hoveredId }, { hover: false });
            }
            hoveredId = null;
          });

          // Click
          map.on("click", "province-fill", (e) => {
            if (!e.features?.length) return;
            const props = e.features[0].properties ?? {};
            const name  = props.province_name ?? props.name ?? "";
            const pData = RISK_BY_NAME[name] ?? RISK_BY_NAME[name.toLowerCase()];
            if (pData && onProvinceClick) onProvinceClick(pData);
          });
        }

        setReady(true);
      });
    });

    return () => {
      cancelled = true;
      if (mapRef.current) { mapRef.current.remove(); mapRef.current = null; }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="relative w-full" style={{ height: 520 }}>
      <div ref={containerRef} className="w-full h-full" />

      {/* Legend */}
      <div
        className="absolute bottom-8 left-4 rounded-lg p-3 shadow-popup text-xs"
        style={{ backgroundColor: "rgba(255,255,255,0.92)", border: "1px solid var(--border)" }}
      >
        <p className="font-bold uppercase tracking-widest mb-2" style={{ color: "var(--text-secondary)" }}>Risk Score</p>
        <div className="h-2 w-36 rounded-full mb-1" style={{
          background: "linear-gradient(to right, #8BBF8B, #C8C84A, #E8903A, #D44A2A, #AA1A1A)"
        }} />
        <div className="flex justify-between w-36" style={{ color: "var(--text-muted)" }}>
          <span>0</span><span>25</span><span>50</span><span>75</span><span>100</span>
        </div>
        <p className="mt-2" style={{ color: "var(--text-muted)" }}>Based on ERA5 climate data</p>
      </div>

      {!ready && (
        <div className="absolute inset-0 flex items-center justify-center" style={{ backgroundColor: "rgba(245,243,238,0.6)" }}>
          <div className="text-sm" style={{ color: "var(--text-secondary)" }}>Loading map…</div>
        </div>
      )}
    </div>
  );
}
