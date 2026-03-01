import { NextResponse } from "next/server";
import { provinces, alerts } from "@/data/mockData";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const riskLevel = searchParams.get("risk_level");
  const minScore = searchParams.get("min_score");

  let filtered = [...provinces];

  if (riskLevel) {
    filtered = filtered.filter((p) => p.riskLevel === riskLevel.toUpperCase());
  }

  if (minScore) {
    filtered = filtered.filter((p) => p.overallScore >= parseInt(minScore));
  }

  return NextResponse.json({
    status: "success",
    generated_at: new Date().toISOString(),
    model_version: "orion-climate-v2.1",
    data: filtered,
    alerts: alerts,
    pagination: {
      total: filtered.length,
      returned: filtered.length,
    },
  });
}
