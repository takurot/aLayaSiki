import { defineConfig } from "@playwright/test";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
export default defineConfig({
  testDir: "./e2e",
  use: {
    baseURL: "http://127.0.0.1:4173",
    headless: true,
    ...(process.env.PLAYWRIGHT_CHANNEL
      ? { channel: process.env.PLAYWRIGHT_CHANNEL }
      : {}),
  },
  webServer: [
    {
      command: "cargo run -p alayasiki-api --manifest-path ../Cargo.toml",
      url: "http://127.0.0.1:18080/health",
      timeout: 180000,
      env: {
        ALAYASIKI_BIND: "127.0.0.1:18080",
        ALAYASIKI_DATA_DIR: mkdtempSync(join(tmpdir(), "alayasiki-ui-e2e-")),
        ALAYASIKI_JWT_SECRET: "ui-e2e-only-secret-at-least-32-bytes",
        ALAYASIKI_JWT_ISSUER: "ui-e2e",
        ALAYASIKI_JWT_AUDIENCE: "ui-e2e",
      },
    },
    {
      command: "npm run dev -- --host 127.0.0.1 --port 4173 --strictPort",
      url: "http://127.0.0.1:4173",
      env: { ALAYASIKI_API_TARGET: "http://127.0.0.1:18080" },
    },
  ],
});
