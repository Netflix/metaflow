import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [svelte()],
  build: {
    assetsDir: "public",
    outDir: "../card_modules",
    emptyOutDir: false,
    minify: true,
    lib: {
      entry: resolve(__dirname, "src/main.ts"),
      name: "Outerbounds Cards",
      // the proper extensions will be added
      fileName: "main",
      formats: ["umd"],
    },
    rollupOptions: {
      output: {
        assetFileNames: (assetInfo) => {
          if (assetInfo.name == "style.css") return "bundle.css";
          return assetInfo.name;
        },
        chunkFileNames: "[name].[ext]",
        entryFileNames: "[name].js",
      },
    },
  },
});
