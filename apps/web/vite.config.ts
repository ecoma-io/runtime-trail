import tailwindcss from "@tailwindcss/vite";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

// No path aliases (ADR 0004): the web app is one module until the
// investigation surface needs more, and Archkeep watches its imports.
export default defineConfig({
  plugins: [vue(), tailwindcss()],
});
