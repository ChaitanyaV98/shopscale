import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.recommended,
  { ignores: ["node_modules", "loadtest/results"] },
  // k6 scripts run in k6's runtime, which provides these globals.
  { files: ["loadtest/**/*.js"], languageOptions: { globals: { __ENV: "readonly", __VU: "readonly", __ITER: "readonly" } } },
);
