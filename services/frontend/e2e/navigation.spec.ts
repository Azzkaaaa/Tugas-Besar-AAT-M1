import { expect, test } from '@playwright/test';

test('pagination, notifikasi, dan sesi pratinjau berjalan bersama', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/');
  await page.getByRole('button', { name: 'Pratinjau sebagai Tim Lapangan, akun lapangan-demo' }).click();
  const rows = page.getByRole('row').filter({ has: page.getByRole('button', { name: /^Lihat detail / }) });
  await expect(rows).toHaveCount(8);
  const firstPage = await rows.allTextContents();
  await page.getByRole('button', { name: 'Halaman berikutnya' }).click();
  await expect(page.getByText('8 kejadian di halaman 2', { exact: true })).toBeVisible();
  await expect.poll(() => rows.allTextContents()).not.toEqual(firstPage);
  await page.getByRole('button', { name: 'Halaman sebelumnya' }).click();
  await expect(page.getByText('8 kejadian di halaman 1', { exact: true })).toBeVisible();

  await page.getByLabel('Tingkat bahaya', { exact: true }).selectOption('AWAS');
  await expect.poll(async () => {
    const values = await rows.allTextContents();
    return values.length > 0 && values.every(text => text.includes('Awas'));
  }).toBe(true);
  await page.getByLabel('Cari di halaman ini', { exact: true }).fill('lokasi tidak ada');
  await expect(page.getByRole('heading', { name: 'Lokasi tidak ditemukan' })).toBeVisible();
  await page.getByRole('button', { name: 'Reset filter' }).click();
  await expect(rows).toHaveCount(8);

  await page.getByRole('button', { name: /^Notifikasi/ }).click();
  await expect(page.getByRole('heading', { name: 'Notifikasi', exact: true })).toBeVisible();
  const notification = page.getByRole('button', { name: /^Buka notifikasi / }).first();
  await notification.click();
  await expect(page.getByRole('dialog', { name: 'Detail kejadian' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog')).toHaveCount(0);
  await expect(notification).toBeFocused();
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Notifikasi', exact: true })).toBeVisible();
  expect(errors).toEqual([]);
});

test('event baru menawarkan refresh tanpa menggeser halaman yang sedang dibaca', async ({ page }) => {
  await page.clock.install();
  await page.goto('/');
  await page.getByRole('button', { name: 'Pratinjau sebagai Media, akun media-demo' }).click();
  await expect(page.getByRole('button', { name: /^Lihat detail / }).first()).toBeVisible();
  const firstBefore = await page.getByRole('row').nth(1).textContent();
  await page.clock.fastForward(16_000);
  await expect(page.getByRole('button', { name: /Ada pembaruan baru/ })).toBeVisible();
  expect(await page.getByRole('row').nth(1).textContent()).toBe(firstBefore);
  await page.getByRole('button', { name: /Ada pembaruan baru/ }).click();
  await expect.poll(() => page.getByRole('row').nth(1).textContent()).not.toBe(firstBefore);
});

test('build produksi menolak identitas demo meskipun disisipkan ke penyimpanan browser', async ({ page }) => {
  await page.route('**/auth/session', route => route.fulfill({ status: 401, json: { detail: 'Login diperlukan.' } }));
  await page.route('**/auth/login', route => route.fulfill({ contentType: 'text/html', body: '<h1>Halaman penyedia login</h1>' }));
  await page.addInitScript(() => sessionStorage.setItem('bnpb.demo.role', 'BNPB_INTERNAL'));
  await page.goto('http://127.0.0.1:4173/#/dashboard');
  await expect(page.getByRole('heading', { name: 'Dashboard Kebencanaan' })).toBeVisible();
  await expect(page.getByRole('button', { name: /Pratinjau sebagai/ })).toHaveCount(0);
  await expect(page.getByRole('heading', { name: 'Pratinjau tanpa Auth' })).toHaveCount(0);
  await expect(page).toHaveURL(/#\/login$/);
  await page.getByRole('button', { name: 'Masuk dengan Zitadel' }).click();
  await expect(page).toHaveURL('http://127.0.0.1:4173/auth/login');
  await expect(page.getByRole('heading', { name: 'Halaman penyedia login' })).toBeVisible();
});
