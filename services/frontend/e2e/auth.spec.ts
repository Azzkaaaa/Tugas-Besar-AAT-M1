import { expect, test } from '@playwright/test';

const production = 'http://127.0.0.1:4173';
const session = (full = false, ttl = 60_000) => ({
  user: { id: 'oidc-user-test', name: 'Pengguna uji Auth', role: full ? 'FIELD_TEAM' : 'MEDIA', mode: 'oidc', scopes: ['hazards:read:summary', 'notifications:read', ...(full ? ['hazards:read:full'] : [])] },
  csrf_token: 'csrf-browser-test', expires_at: new Date(Date.now() + ttl).toISOString(),
});

// These intercept the bridge contract. Real IdP acceptance requires compose.auth.yaml.
test('sesi OIDC memakai profil server dan logout POST dengan CSRF', async ({ page }) => {
  let loggedOut = false;
  let logoutCsrf = '';
  await page.addInitScript(() => sessionStorage.setItem('bnpb.demo.role', 'BNPB_INTERNAL'));
  await page.route('**/auth/session', route => route.fulfill({ status: loggedOut ? 401 : 200, json: loggedOut ? {} : session() }));
  await page.route('**/auth/logout', route => {
    expect(route.request().method()).toBe('POST');
    logoutCsrf = route.request().headers()['x-csrf-token'];
    loggedOut = true;
    return route.fulfill({ json: { logout_url: `${production}/#/login` } });
  });
  await page.goto(`${production}/#/dashboard`);
  await expect(page.getByText('Pengguna uji Auth', { exact: true })).toBeVisible();
  await expect(page.getByText('Ringkasan publik', { exact: true })).toBeVisible();
  await expect(page.getByText('Akun simulasi', { exact: true })).toHaveCount(0);
  await expect(page.getByText('Data simulasi.', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: /^Lihat detail / }).first().click();
  await expect(page.getByRole('dialog').getByText('Koordinat', { exact: true })).toHaveCount(0);
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'Keluar', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Masuk dengan Zitadel' })).toBeVisible();
  expect(logoutCsrf).toBe('csrf-browser-test');
  await expect(page.getByRole('button', { name: /Pratinjau sebagai/ })).toHaveCount(0);
});

test('cookie akses kedaluwarsa dipulihkan sekali sebelum dashboard dibuka', async ({ page }) => {
  let refreshed = false;
  let refreshCount = 0;
  await page.route('**/auth/session', route => route.fulfill({ status: refreshed ? 200 : 401, json: refreshed ? session(true) : { refresh_required: true, csrf_token: 'expired-csrf' } }));
  await page.route('**/auth/refresh', route => {
    expect(route.request().method()).toBe('POST');
    expect(route.request().headers()['x-csrf-token']).toBe('expired-csrf');
    refreshCount++;
    refreshed = true;
    return route.fulfill({ json: session(true) });
  });
  await page.goto(`${production}/#/dashboard`);
  await expect(page.getByText('Akses lengkap', { exact: true })).toBeVisible();
  expect(refreshCount).toBe(1);
  await page.reload();
  await expect(page.getByText('Akses lengkap', { exact: true })).toBeVisible();
  expect(refreshCount).toBe(1);
});

test('refresh yang ditolak menghapus sesi tanpa membuka akses demo', async ({ page }) => {
  await page.clock.install();
  await page.route('**/auth/session', route => route.fulfill({ json: session(true, 7000) }));
  await page.route('**/auth/refresh', route => route.fulfill({ status: 401, json: { detail: 'Sesi kedaluwarsa.' } }));
  await page.goto(`${production}/#/dashboard`);
  await expect(page.getByText('Akses lengkap', { exact: true })).toBeVisible();
  await page.clock.fastForward(8000);
  await expect(page.getByRole('button', { name: 'Masuk dengan Zitadel' })).toBeVisible();
  await expect(page.getByRole('button', { name: /Pratinjau sebagai/ })).toHaveCount(0);
  await expect(page.getByRole('table')).toHaveCount(0);
});

test('layanan Auth gagal menampilkan retry tanpa menganggap pengguna sudah login', async ({ page }) => {
  let unavailable = true;
  await page.route('**/auth/session', route => route.fulfill({ status: unavailable ? 503 : 401, json: { detail: unavailable ? 'Auth belum tersedia.' : 'Login diperlukan.' } }));
  await page.goto(`${production}/#/dashboard`);
  await expect(page.getByRole('alert')).toContainText('Auth belum tersedia.');
  await expect(page.getByRole('table')).toHaveCount(0);
  unavailable = false;
  await page.getByRole('button', { name: 'Periksa sesi lagi' }).click();
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page).toHaveURL(/#\/login$/);
});
