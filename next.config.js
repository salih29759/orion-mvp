/** @type {import('next').NextConfig} */
const nextConfig = {
  // MapLibre GL JS uses ESM; transpilePackages ensures webpack handles it correctly.
  transpilePackages: ["mapbox-gl"],
};

module.exports = nextConfig;
