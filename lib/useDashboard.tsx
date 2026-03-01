"use client";

import { createContext, useContext, useEffect, useState, ReactNode } from "react";
import { Province, Alert, fetchProvinces, fetchAlerts } from "./api";

// ---------------------------------------------------------------------------
// Context shape
// ---------------------------------------------------------------------------

interface DashboardState {
  provinces: Province[];
  alerts: Alert[];
  asOfDate: string | null;
  dataSource: string | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

const DashboardContext = createContext<DashboardState>({
  provinces: [],
  alerts: [],
  asOfDate: null,
  dataSource: null,
  loading: true,
  error: null,
  refetch: () => {},
});

// ---------------------------------------------------------------------------
// Provider — fetches both endpoints once, shares via context
// ---------------------------------------------------------------------------

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [provinces, setProvinces] = useState<Province[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [asOfDate, setAsOfDate] = useState<string | null>(null);
  const [dataSource, setDataSource] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    Promise.all([fetchProvinces(), fetchAlerts()])
      .then(([p, a]) => {
        if (cancelled) return;
        setProvinces(p.data);
        setAlerts(a.data);
        setAsOfDate(p.as_of_date ?? null);
        setDataSource(p.data_source ?? a.data_source ?? null);
        setLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load data");
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [tick]);

  const refetch = () => setTick((t) => t + 1);

  return (
    <DashboardContext.Provider
      value={{ provinces, alerts, asOfDate, dataSource, loading, error, refetch }}
    >
      {children}
    </DashboardContext.Provider>
  );
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useDashboard() {
  return useContext(DashboardContext);
}
