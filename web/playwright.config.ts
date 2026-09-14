import { defineConfig, devices } from '@playwright/test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = fileURLToPath(new URL('../', import.meta.url));
const executablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH;

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: 'http://127.0.0.1:18080',
    permissions: ['clipboard-read', 'clipboard-write'],
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        ...(executablePath ? { launchOptions: { executablePath } } : {}),
      },
    },
  ],
  webServer: {
    command: 'cargo run --quiet --bin biubin',
    cwd: repositoryRoot,
    url: 'http://127.0.0.1:18080/readyz',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      ...process.env,
      BIUBIN_BIND_HOST: '127.0.0.1',
      BIUBIN_ADVERTISE_HOST: '127.0.0.1',
      BIUBIN_HTTP_PORT: '18080',
      BIUBIN_GRPC_H2C_PORT: '0',
      BIUBIN_GRPC_TLS_PORT: '0',
      BIUBIN_TCP_PORT: '0',
      BIUBIN_UDP_PORT: '0',
      BIUBIN_THRIFT_PORT: '0',
      BIUBIN_MQTT_ENABLED: 'true',
      BIUBIN_MQTT_TCP_PORT: '0',
      BIUBIN_MQTT_AUTH_TCP_PORT: '0',
      BIUBIN_MQTT_V5_PORT: '0',
      BIUBIN_MQTT_TLS_PORT: '0',
      BIUBIN_MQTT_TLS_BACKEND_PORT: '0',
      BIUBIN_MQTT_WS_PORT: '18083',
      BIUBIN_MQTT_TLS_ENABLED: 'false',
    },
  },
});
