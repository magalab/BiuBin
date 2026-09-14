import { expect, test } from '@playwright/test';

test.describe.configure({ mode: 'serial' });

test('loads the homepage and its control plane', async ({ page }) => {
  await page.goto('/');

  await expect(page).toHaveTitle(/biubin/);
  await expect(page.getByRole('heading', { name: 'Every protocol, one process' })).toBeVisible();
  await expect(page.getByText('ready', { exact: true })).toBeVisible();
  await expect(page.getByRole('link', { name: 'OpenAPI docs ↗' })).toBeVisible();

  await page.getByRole('button', { name: 'copy curl' }).first().click();
  await expect(page.getByText('copied', { exact: true })).toBeVisible();
});

test('calls an HTTP fixture from the homepage', async ({ page }) => {
  await page.goto('/');

  const panel = page.locator('article').filter({
    has: page.getByRole('heading', { name: 'Echo a request' }),
  });
  await panel.getByLabel('Path').fill('/anything/e2e?source=playwright');
  await panel.getByRole('button', { name: 'call endpoint' }).click();

  await expect(panel.locator('pre')).toContainText('200 OK');
  await expect(panel.locator('pre')).toContainText('playwright');
});

test('round-trips a WebSocket frame', async ({ page }) => {
  await page.goto('/');

  const panel = page.locator('article').filter({
    has: page.getByRole('heading', { name: 'Frame echo' }),
  });
  await panel.getByRole('button', { name: 'connect' }).click();
  await expect(panel.getByText('connected', { exact: true })).toBeVisible();
  await panel.getByLabel('Message').fill('browser websocket smoke');
  await panel.getByRole('button', { name: 'send' }).click();

  await expect(panel.locator('pre')).toContainText('browser websocket smoke');
});

test('receives Server-Sent Events', async ({ page }) => {
  await page.goto('/');

  const panel = page.locator('article').filter({
    has: page.getByRole('heading', { name: 'Watch a ticker' }),
  });
  await panel.getByRole('button', { name: 'start' }).click();
  await expect(panel.getByText('connected', { exact: true })).toBeVisible();
  await expect(panel.locator('pre')).toContainText('sequence', { timeout: 10_000 });
});

test('publishes and receives MQTT over WebSocket', async ({ page }) => {
  await page.goto('/');

  const panel = page.locator('article').filter({
    has: page.getByRole('heading', { name: 'Publish / subscribe' }),
  });
  await panel.getByRole('button', { name: 'connect' }).click();
  await expect(panel.getByText('connected', { exact: true })).toBeVisible({ timeout: 10_000 });
  await panel.getByLabel('Payload').fill('browser mqtt smoke');
  await panel.getByRole('button', { name: 'publish' }).click();

  await expect(panel.locator('pre')).toContainText('browser mqtt smoke', { timeout: 10_000 });
});

test('renders grouped OpenAPI documentation', async ({ page }) => {
  await page.goto('/openapi');

  await expect(page.getByText('Request and response basics').first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText('Request echo').first()).toBeVisible();
  await expect(page.getByText('Streaming and bytes').first()).toBeVisible();
});
