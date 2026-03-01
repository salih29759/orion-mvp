import { NextRequest, NextResponse } from "next/server";

const BACKEND_URL = process.env.ORION_BACKEND_URL;
const BACKEND_KEY = process.env.ORION_BACKEND_API_KEY;

function buildBackendUrl(pathSegments: string[], req: NextRequest): string {
  if (!BACKEND_URL) {
    throw new Error("Missing ORION_BACKEND_URL");
  }
  const base = BACKEND_URL.endsWith("/") ? BACKEND_URL.slice(0, -1) : BACKEND_URL;
  const pathname = pathSegments.join("/");
  const url = new URL(`${base}/${pathname}`);

  req.nextUrl.searchParams.forEach((v, k) => url.searchParams.append(k, v));
  return url.toString();
}

async function proxy(req: NextRequest, pathSegments: string[]) {
  if (!BACKEND_KEY || !BACKEND_URL) {
    return NextResponse.json(
      { status: "error", message: "Backend proxy env vars are not configured." },
      { status: 500 },
    );
  }

  const target = buildBackendUrl(pathSegments, req);
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  const body = hasBody ? await req.text() : undefined;

  const upstream = await fetch(target, {
    method: req.method,
    headers: {
      Authorization: `Bearer ${BACKEND_KEY}`,
      "Content-Type": "application/json",
    },
    body,
    cache: "no-store",
  });

  const text = await upstream.text();
  return new NextResponse(text, {
    status: upstream.status,
    headers: {
      "Content-Type": upstream.headers.get("Content-Type") || "application/json",
    },
  });
}

export async function GET(
  req: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const { path } = await context.params;
  return proxy(req, path);
}

export async function POST(
  req: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  const { path } = await context.params;
  return proxy(req, path);
}
