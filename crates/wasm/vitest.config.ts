import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  plugins: [wasm()],
  publicDir: "../core/data",
  test: {
    browser: {
      enabled: true,
      provider: "playwright",
      headless: true,
      instances: [{ browser: "chromium" }],
    },
  },
});
