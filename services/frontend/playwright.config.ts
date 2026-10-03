import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  outputDir: '../../.cache/frontend-e2e-results',
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: [
    ['list'],
    ['html', { outputFolder: '../../.cache/frontend-e2e-report', open: 'never' }],
  ],
  use: {
    baseURL: 'http://127.0.0.1:5173',
    browserName: 'chromium',
    ...(process.platform === 'win32' ? { channel: 'msedge' } : {}),
    viewport: { width: 1440, height: 960 },
    locale: 'id-ID',
    timezoneId: 'Asia/Jakarta',
    reducedMotion: 'reduce',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: [{
    command: 'npm run dev -- --host 127.0.0.1 --strictPort',
    url: 'http://127.0.0.1:5173',
    timeout: 30_000,
    reuseExistingServer: !process.env.CI,
    env: { VITE_APP_MODE: 'demo', VITE_ENABLE_DEMO: 'true' },
  }, {
    command: 'npm run build && npm run preview -- --host 127.0.0.1 --strictPort',
    url: 'http://127.0.0.1:4173',
    timeout: 60_000,
    reuseExistingServer: false,
    env: { VITE_APP_MODE: 'gateway', VITE_ENABLE_DEMO: 'false' },
  }],
});
