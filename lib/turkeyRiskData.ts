// Mock risk scores for Turkish provinces — realistic for climate risk context
// Eastern Anatolia: high drought, Black Sea: high rain, Aegean: heat+drought

export interface ProvinceRisk {
  name:     string;    // as it appears in GeoJSON properties
  nameEn:   string;
  risk:     number;    // 0-100 all-hazards
  heat:     number;
  rain:     number;
  drought:  number;
  lat:      number;
  lon:      number;
  pop_change: number;  // % YoY
  gdp_change: number;  // % YoY
}

export const PROVINCE_RISK: ProvinceRisk[] = [
  { name: "İstanbul",    nameEn: "Istanbul",     risk: 52, heat: 61, rain: 48, drought: 42, lat: 41.0082, lon: 28.9784, pop_change: +1.2, gdp_change: -0.8 },
  { name: "Ankara",      nameEn: "Ankara",       risk: 44, heat: 55, rain: 30, drought: 52, lat: 39.9334, lon: 32.8597, pop_change: +0.4, gdp_change: -0.5 },
  { name: "İzmir",       nameEn: "Izmir",        risk: 64, heat: 72, rain: 38, drought: 66, lat: 38.4192, lon: 27.1287, pop_change: +0.8, gdp_change: -1.1 },
  { name: "Bursa",       nameEn: "Bursa",        risk: 48, heat: 58, rain: 44, drought: 40, lat: 40.1885, lon: 29.0610, pop_change: +0.6, gdp_change: -0.6 },
  { name: "Antalya",     nameEn: "Antalya",      risk: 62, heat: 78, rain: 32, drought: 62, lat: 36.8969, lon: 30.7133, pop_change: +1.5, gdp_change: -1.4 },
  { name: "Adana",       nameEn: "Adana",        risk: 68, heat: 80, rain: 45, drought: 65, lat: 37.0000, lon: 35.3213, pop_change: -0.2, gdp_change: -1.6 },
  { name: "Konya",       nameEn: "Konya",        risk: 58, heat: 62, rain: 24, drought: 70, lat: 37.8667, lon: 32.4833, pop_change: -0.3, gdp_change: -0.9 },
  { name: "Gaziantep",   nameEn: "Gaziantep",    risk: 70, heat: 82, rain: 28, drought: 72, lat: 37.0662, lon: 37.3833, pop_change: +1.0, gdp_change: -1.8 },
  { name: "Mersin",      nameEn: "Mersin",       risk: 66, heat: 75, rain: 40, drought: 68, lat: 36.8000, lon: 34.6333, pop_change: +0.3, gdp_change: -1.2 },
  { name: "Diyarbakır",  nameEn: "Diyarbakir",   risk: 72, heat: 85, rain: 22, drought: 78, lat: 37.9144, lon: 40.2306, pop_change: -1.5, gdp_change: -2.1 },
  { name: "Şanlıurfa",   nameEn: "Sanliurfa",    risk: 76, heat: 88, rain: 20, drought: 80, lat: 37.1591, lon: 38.7969, pop_change: +0.8, gdp_change: -2.4 },
  { name: "Kocaeli",     nameEn: "Kocaeli",      risk: 50, heat: 58, rain: 50, drought: 38, lat: 40.8533, lon: 29.8815, pop_change: +1.8, gdp_change: -0.7 },
  { name: "Hatay",       nameEn: "Hatay",        risk: 65, heat: 74, rain: 38, drought: 60, lat: 36.4018, lon: 36.3498, pop_change: -0.8, gdp_change: -1.5 },
  { name: "Kayseri",     nameEn: "Kayseri",      risk: 55, heat: 60, rain: 28, drought: 62, lat: 38.7312, lon: 35.4787, pop_change: +0.2, gdp_change: -0.8 },
  { name: "Trabzon",     nameEn: "Trabzon",      risk: 56, heat: 38, rain: 80, drought: 18, lat: 41.0015, lon: 39.7178, pop_change: -0.5, gdp_change: -0.9 },
  { name: "Rize",        nameEn: "Rize",         risk: 62, heat: 40, rain: 85, drought: 15, lat: 41.0201, lon: 40.5234, pop_change: -0.8, gdp_change: -1.0 },
  { name: "Erzurum",     nameEn: "Erzurum",      risk: 68, heat: 48, rain: 35, drought: 72, lat: 39.9000, lon: 41.2700, pop_change: -2.1, gdp_change: -1.8 },
  { name: "Van",         nameEn: "Van",          risk: 70, heat: 52, rain: 30, drought: 76, lat: 38.4891, lon: 43.4089, pop_change: -1.2, gdp_change: -1.9 },
  { name: "Ağrı",        nameEn: "Agri",         risk: 74, heat: 50, rain: 28, drought: 80, lat: 39.7191, lon: 43.0503, pop_change: -2.8, gdp_change: -2.5 },
  { name: "Muğla",       nameEn: "Mugla",        risk: 60, heat: 70, rain: 35, drought: 60, lat: 37.2153, lon: 28.3636, pop_change: +1.2, gdp_change: -1.2 },
  { name: "Manisa",      nameEn: "Manisa",       risk: 58, heat: 68, rain: 32, drought: 58, lat: 38.6191, lon: 27.4290, pop_change: +0.3, gdp_change: -0.9 },
  { name: "Balıkesir",   nameEn: "Balikesir",    risk: 46, heat: 55, rain: 42, drought: 40, lat: 39.6484, lon: 27.8826, pop_change: +0.1, gdp_change: -0.6 },
  { name: "Eskişehir",   nameEn: "Eskisehir",    risk: 42, heat: 50, rain: 30, drought: 45, lat: 39.7767, lon: 30.5206, pop_change: +0.5, gdp_change: -0.5 },
  { name: "Sakarya",     nameEn: "Sakarya",      risk: 48, heat: 55, rain: 52, drought: 32, lat: 40.6940, lon: 30.4358, pop_change: +1.1, gdp_change: -0.7 },
  { name: "Samsun",      nameEn: "Samsun",       risk: 50, heat: 42, rain: 70, drought: 22, lat: 41.2867, lon: 36.3300, pop_change: -0.2, gdp_change: -0.8 },
  { name: "Malatya",     nameEn: "Malatya",      risk: 62, heat: 70, rain: 25, drought: 68, lat: 38.3552, lon: 38.3095, pop_change: -0.9, gdp_change: -1.4 },
  { name: "Tekirdağ",    nameEn: "Tekirdag",     risk: 44, heat: 52, rain: 44, drought: 36, lat: 40.9781, lon: 27.5117, pop_change: +1.6, gdp_change: -0.6 },
  { name: "Denizli",     nameEn: "Denizli",      risk: 56, heat: 65, rain: 34, drought: 58, lat: 37.7765, lon: 29.0864, pop_change: +0.4, gdp_change: -0.9 },
  { name: "Kırıkkale",   nameEn: "Kirikkale",    risk: 44, heat: 52, rain: 28, drought: 48, lat: 39.8468, lon: 33.5153, pop_change: -1.0, gdp_change: -0.7 },
  { name: "Mardin",      nameEn: "Mardin",       risk: 72, heat: 84, rain: 22, drought: 76, lat: 37.3212, lon: 40.7245, pop_change: -1.4, gdp_change: -2.0 },
];

// Build lookup by name (for GeoJSON join)
export const RISK_BY_NAME: Record<string, ProvinceRisk> = {};
for (const p of PROVINCE_RISK) {
  RISK_BY_NAME[p.name]   = p;
  RISK_BY_NAME[p.nameEn] = p;
  RISK_BY_NAME[p.name.toLowerCase()]   = p;
  RISK_BY_NAME[p.nameEn.toLowerCase()] = p;
}
