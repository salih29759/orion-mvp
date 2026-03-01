export type RiskLevel = "HIGH" | "MEDIUM" | "LOW";
export type RiskType = "FLOOD" | "DROUGHT" | "WILDFIRE" | "EARTHQUAKE";

export interface ProvinceRisk {
  id: string;
  name: string;
  nameEn: string;
  floodScore: number;
  droughtScore: number;
  overallScore: number;
  riskLevel: RiskLevel;
  trend: "up" | "down" | "stable";
  trendPct: number;
  lat: number;
  lng: number;
  population: number;
  insuredAssets: number; // in million USD
}

export interface Alert {
  id: string;
  provinceId: string;
  provinceName: string;
  riskType: RiskType;
  level: RiskLevel;
  message: string;
  issuedAt: string;
  expiresAt: string;
  affectedPolicies: number;
  estimatedLoss: number; // in million USD
}

export const provinces: ProvinceRisk[] = [
  {
    id: "IST",
    name: "İstanbul",
    nameEn: "Istanbul",
    floodScore: 82,
    droughtScore: 38,
    overallScore: 87,
    riskLevel: "HIGH",
    trend: "up",
    trendPct: 4.2,
    lat: 41.0082,
    lng: 28.9784,
    population: 15840900,
    insuredAssets: 4820,
  },
  {
    id: "ANK",
    name: "Ankara",
    nameEn: "Ankara",
    floodScore: 45,
    droughtScore: 68,
    overallScore: 61,
    riskLevel: "MEDIUM",
    trend: "stable",
    trendPct: 0.8,
    lat: 39.9334,
    lng: 32.8597,
    population: 5782285,
    insuredAssets: 2150,
  },
  {
    id: "IZM",
    name: "İzmir",
    nameEn: "Izmir",
    floodScore: 71,
    droughtScore: 55,
    overallScore: 78,
    riskLevel: "HIGH",
    trend: "up",
    trendPct: 6.1,
    lat: 38.4189,
    lng: 27.1287,
    population: 4394694,
    insuredAssets: 1640,
  },
  {
    id: "ANT",
    name: "Antalya",
    nameEn: "Antalya",
    floodScore: 58,
    droughtScore: 72,
    overallScore: 76,
    riskLevel: "HIGH",
    trend: "up",
    trendPct: 8.3,
    lat: 36.8969,
    lng: 30.7133,
    population: 2696249,
    insuredAssets: 980,
  },
  {
    id: "BUR",
    name: "Bursa",
    nameEn: "Bursa",
    floodScore: 64,
    droughtScore: 42,
    overallScore: 59,
    riskLevel: "MEDIUM",
    trend: "down",
    trendPct: 1.5,
    lat: 40.1885,
    lng: 29.0610,
    population: 3194720,
    insuredAssets: 1120,
  },
  {
    id: "ADA",
    name: "Adana",
    nameEn: "Adana",
    floodScore: 77,
    droughtScore: 63,
    overallScore: 81,
    riskLevel: "HIGH",
    trend: "up",
    trendPct: 5.7,
    lat: 37.0000,
    lng: 35.3213,
    population: 2258718,
    insuredAssets: 720,
  },
  {
    id: "KON",
    name: "Konya",
    nameEn: "Konya",
    floodScore: 28,
    droughtScore: 79,
    overallScore: 54,
    riskLevel: "MEDIUM",
    trend: "up",
    trendPct: 3.4,
    lat: 37.8746,
    lng: 32.4932,
    population: 2276720,
    insuredAssets: 590,
  },
  {
    id: "GAZ",
    name: "Gaziantep",
    nameEn: "Gaziantep",
    floodScore: 52,
    droughtScore: 61,
    overallScore: 63,
    riskLevel: "MEDIUM",
    trend: "stable",
    trendPct: 1.1,
    lat: 37.0662,
    lng: 37.3833,
    population: 2154051,
    insuredAssets: 480,
  },
  {
    id: "KAY",
    name: "Kayseri",
    nameEn: "Kayseri",
    floodScore: 31,
    droughtScore: 56,
    overallScore: 42,
    riskLevel: "LOW",
    trend: "stable",
    trendPct: 0.3,
    lat: 38.7312,
    lng: 35.4787,
    population: 1441523,
    insuredAssets: 340,
  },
  {
    id: "TRA",
    name: "Trabzon",
    nameEn: "Trabzon",
    floodScore: 88,
    droughtScore: 18,
    overallScore: 91,
    riskLevel: "HIGH",
    trend: "up",
    trendPct: 11.2,
    lat: 41.0015,
    lng: 39.7178,
    population: 811902,
    insuredAssets: 260,
  },
];

export const alerts: Alert[] = [
  {
    id: "ALT-001",
    provinceId: "TRA",
    provinceName: "Trabzon",
    riskType: "FLOOD",
    level: "HIGH",
    message:
      "Extreme flood risk detected. Heavy rainfall forecast (180mm/48h). Immediate policy review recommended.",
    issuedAt: "2024-01-15T06:00:00Z",
    expiresAt: "2024-01-17T06:00:00Z",
    affectedPolicies: 1842,
    estimatedLoss: 47.3,
  },
  {
    id: "ALT-002",
    provinceId: "IST",
    provinceName: "İstanbul",
    riskType: "FLOOD",
    level: "HIGH",
    message:
      "Urban flood alert. Storm surge risk in coastal districts. High-value portfolio exposure elevated.",
    issuedAt: "2024-01-15T08:30:00Z",
    expiresAt: "2024-01-16T20:00:00Z",
    affectedPolicies: 12450,
    estimatedLoss: 218.6,
  },
  {
    id: "ALT-003",
    provinceId: "ADA",
    provinceName: "Adana",
    riskType: "FLOOD",
    level: "HIGH",
    message:
      "River basin overflow risk. Seyhan River at 94% capacity. Agricultural portfolio at risk.",
    issuedAt: "2024-01-14T14:00:00Z",
    expiresAt: "2024-01-16T14:00:00Z",
    affectedPolicies: 3210,
    estimatedLoss: 31.8,
  },
  {
    id: "ALT-004",
    provinceId: "ANT",
    provinceName: "Antalya",
    riskType: "DROUGHT",
    level: "MEDIUM",
    message:
      "Prolonged drought conditions. 60-day rainfall deficit at 72%. Tourism sector exposure increasing.",
    issuedAt: "2024-01-13T10:00:00Z",
    expiresAt: "2024-01-28T10:00:00Z",
    affectedPolicies: 5680,
    estimatedLoss: 28.4,
  },
  {
    id: "ALT-005",
    provinceId: "KON",
    provinceName: "Konya",
    riskType: "DROUGHT",
    level: "MEDIUM",
    message:
      "Moderate drought stress in agricultural zones. Crop insurance claims expected to rise 15-20%.",
    issuedAt: "2024-01-12T09:00:00Z",
    expiresAt: "2024-01-26T09:00:00Z",
    affectedPolicies: 2890,
    estimatedLoss: 14.2,
  },
  {
    id: "ALT-006",
    provinceId: "GAZ",
    provinceName: "Gaziantep",
    riskType: "DROUGHT",
    level: "MEDIUM",
    message:
      "Soil moisture levels at 6-month low. Pistachio and olive crop risk elevated for Q1.",
    issuedAt: "2024-01-11T11:00:00Z",
    expiresAt: "2024-01-25T11:00:00Z",
    affectedPolicies: 1640,
    estimatedLoss: 9.7,
  },
];

export const getRiskColor = (score: number): string => {
  if (score >= 75) return "#ef4444";
  if (score >= 50) return "#f97316";
  return "#22c55e";
};

export const getRiskBgColor = (level: RiskLevel): string => {
  switch (level) {
    case "HIGH":
      return "rgba(239, 68, 68, 0.12)";
    case "MEDIUM":
      return "rgba(249, 115, 22, 0.12)";
    case "LOW":
      return "rgba(34, 197, 94, 0.12)";
  }
};

export const getRiskTextColor = (level: RiskLevel): string => {
  switch (level) {
    case "HIGH":
      return "#ef4444";
    case "MEDIUM":
      return "#f97316";
    case "LOW":
      return "#22c55e";
  }
};

export const getTopRiskProvinces = (count = 5): ProvinceRisk[] => {
  return [...provinces]
    .sort((a, b) => b.overallScore - a.overallScore)
    .slice(0, count);
};

export const getSummaryStats = () => {
  const high = provinces.filter((p) => p.riskLevel === "HIGH").length;
  const medium = provinces.filter((p) => p.riskLevel === "MEDIUM").length;
  const low = provinces.filter((p) => p.riskLevel === "LOW").length;
  const totalPolicies = alerts.reduce((s, a) => s + a.affectedPolicies, 0);
  const totalExposure = alerts.reduce((s, a) => s + a.estimatedLoss, 0);
  return { high, medium, low, totalPolicies, totalExposure };
};
