"use client";

import { useEffect, useRef } from "react";

interface AssetMapProps {
  lat: number;
  lon: number;
  name: string;
}

export function AssetMap({ lat, lon, name }: AssetMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapRef = useRef<any>(null);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    let cancelled = false;

    import("maplibre-gl").then((maplibre) => {
      if (cancelled || !containerRef.current) return;

      // inject MapLibre CSS once
      if (!document.getElementById("maplibre-css")) {
        const link = document.createElement("link");
        link.id = "maplibre-css";
        link.rel = "stylesheet";
        link.href = "https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.css";
        document.head.appendChild(link);
      }

      const map = new maplibre.Map({
        container: containerRef.current,
        style: {
          version: 8,
          sources: {
            osm: {
              type: "raster",
              tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
              tileSize: 256,
              attribution: "© OpenStreetMap contributors",
            },
          },
          layers: [{ id: "osm", type: "raster", source: "osm" }],
        },
        center: [lon, lat],
        zoom: 10,
        attributionControl: false,
      });

      mapRef.current = map;

      map.addControl(new maplibre.NavigationControl());
      map.addControl(new maplibre.AttributionControl({ compact: true }), "bottom-right");

      map.on("load", () => {
        new maplibre.Marker({ color: "#1e6fff" })
          .setLngLat([lon, lat])
          .setPopup(
            new maplibre.Popup({ offset: 25 }).setHTML(
              `<div style="color:#0a1628;font-weight:600;font-size:13px;">${name}</div>
               <div style="color:#555;font-size:11px;margin-top:2px;">${lat.toFixed(4)}, ${lon.toFixed(4)}</div>`
            )
          )
          .addTo(map);
      });
    });

    return () => {
      cancelled = true;
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
    };
  }, [lat, lon, name]);

  return (
    <div className="relative w-full h-full" style={{ minHeight: 260 }}>
      <div ref={containerRef} className="w-full h-full" style={{ minHeight: 260 }} />
      <div className="absolute bottom-2 left-2 bg-black/60 text-white/70 text-[10px] font-mono px-2 py-1 rounded pointer-events-none z-10">
        {lat.toFixed(4)}, {lon.toFixed(4)}
      </div>
    </div>
  );
}
