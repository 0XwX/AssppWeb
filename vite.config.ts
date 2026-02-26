import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: 'frontend/dist',
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8787',
      '/wisp': { target: 'ws://localhost:8787', ws: true },
    },
  },
});
