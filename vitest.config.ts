import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    projects: [
      {
        plugins: [react()],
        test: {
          name: 'frontend',
          include: ['frontend/tests/**/*.test.{ts,tsx}'],
          environment: 'jsdom',
          globals: true,
          testTimeout: 15000,
          setupFiles: ['./frontend/tests/setup.ts'],
        },
      },
      {
        test: {
          name: 'workers',
          include: ['src/**/*.test.ts'],
          environment: 'node',
        },
      },
    ],
  },
});
