import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

function demoApiFromEnv() {
  const file = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", ".env");
  let fromFile = "";
  try {
    const text = fs.readFileSync(file, "utf8");
    for (const line of text.split("\n")) {
      const match = line.match(/^\s*DEMO_API_URL\s*=\s*(.*?)\s*$/);
      if (match) fromFile = match[1].replace(/^["']|["']$/g, "");
    }
  } catch {
    fromFile = "";
  }
  return (fromFile || process.env.NEXT_PUBLIC_DEMO_API || "").trim();
}

const demoApi = demoApiFromEnv();

/** @type {import('next').NextConfig} */
const nextConfig = {
  env: demoApi ? { NEXT_PUBLIC_DEMO_API: demoApi } : {},
};

export default nextConfig;
