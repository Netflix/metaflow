module.exports = {
  parser: "@typescript-eslint/parser", // add the TypeScript parser
  parserOptions: {
    tsconfigRootDir: __dirname,
    project: ["./tsconfig.json"],
    extraFileExtensions: [".svelte"],
  },
  env: {
    browser: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
  ],
  plugins: [
    "svelte3",
    "@typescript-eslint", // add the TypeScript plugin
  ],
  ignorePatterns: ["**/*.js"],
  overrides: [
    {
      files: ["*.svelte"],
      processor: "svelte3/svelte3",
    },
  ],
  rules: {
    "@typescript-eslint/no-unsafe-member-access": 0,
    "@typescript-eslint/no-explicit-any": 0,
    "@typescript-eslint/no-unsafe-argument": 0,
    "@typescript-eslint/no-unsafe-call": 0,
  },
  settings: {
    "svelte3/typescript": () => require("typescript"),
  },
};
