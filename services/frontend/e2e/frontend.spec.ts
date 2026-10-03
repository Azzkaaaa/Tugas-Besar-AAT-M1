import { expect, test, type Page } from '@playwright/test';

const accounts = [
  { name: 'Media', username: 'media-demo', full: false },
  { name: 'Tim Lapangan', username: 'lapangan-demo', full: true },
  { name: 'BNPB Pusat', username: 'bnpb-demo', full: true },
];

async function enterDemo(page: Page, account = accounts[1]) {
  await page.goto('/');
  await page.getByRole('button', { name: `Pratinjau sebagai ${account.name}, akun ${account.username}` }).click();
  await expect(page.getByRole('heading', { name: 'Situasi kebencanaan', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Lihat detail / }).first()).toBeVisible();
}

async function expectNoHorizontalOverflow(page: Page) {
  await expect.poll(() => page.evaluate(() => {
    const root = document.documentElement;
    return Math.max(root.scrollWidth, document.body.scrollWidth) - root.clientWidth;
  })).toBeLessThanOrEqual(1);
}

test('login organisasi yang belum tersedia tidak membuat sesi palsu', async ({ page }, testInfo) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Selamat datang.' })).toBeVisible();
  await expect(page.getByText('Mode demo', { exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath('login-desktop.png'), fullPage: true });

  await page.getByRole('button', { name: 'Masuk dengan organisasi' }).click();
  await expect(page.getByRole('alert')).toBeVisible();
  await expect(page.getByRole('alert')).not.toBeEmpty();
  await expect(page.getByRole('heading', { name: 'Selamat datang.' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Situasi kebencanaan' })).toHaveCount(0);
});

for (const account of accounts) {
  test(`pratinjau ${account.name} menampilkan detail sesuai peran dan dapat keluar`, async ({ page }, testInfo) => {
    await enterDemo(page, account);
    if (account.name === 'Tim Lapangan') {
      await page.screenshot({ path: testInfo.outputPath('dashboard-desktop.png'), fullPage: true });
    }

    await page.getByRole('button', { name: /^Lihat detail / }).first().click();
    const detail = page.getByRole('dialog', { name: 'Detail kejadian' });
    await expect(detail).toBeVisible();
    if (account.full) {
      await expect(detail.getByText('Koordinat', { exact: true })).toBeVisible();
      await expect(detail.getByText('Data sumber', { exact: true })).toBeVisible();
    } else {
      await expect(detail.getByText('Koordinat', { exact: true })).toHaveCount(0);
      await expect(detail.getByText('Data sumber', { exact: true })).toHaveCount(0);
    }
    await detail.getByRole('button', { name: 'Tutup detail' }).click();
    await expect(detail).toHaveCount(0);

    await page.getByRole('button', { name: 'Keluar', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'Selamat datang.' })).toBeVisible();
    await page.reload();
    await expect(page.getByRole('heading', { name: 'Selamat datang.' })).toBeVisible();
  });
}

test('filter sumber hanya menampilkan kejadian dari sumber terpilih', async ({ page }) => {
  await enterDemo(page);
  await page.getByLabel('Sumber', { exact: true }).selectOption('BMKG');
  const rows = page.getByRole('row').filter({ has: page.getByRole('button', { name: /^Lihat detail / }) });
  // Wait for the new response, rather than asserting the previous snapshot.
  await expect.poll(async () => {
    const texts = await rows.allTextContents();
    return texts.length > 0 && texts.every(text => text.includes('BMKG') && !text.includes('PVMBG'));
  }).toBe(true);

  await page.getByLabel('Sumber', { exact: true }).selectOption('PVMBG');
  await expect.poll(async () => {
    const texts = await rows.allTextContents();
    return texts.length > 0 && texts.every(text => text.includes('PVMBG') && !text.includes('BMKG'));
  }).toBe(true);
});

test('dashboard menangani data terlambat, kosong, gagal, dan kembali normal', async ({ page }) => {
  await enterDemo(page);
  const scenarios = page.getByLabel('Skenario demo', { exact: true });

  await scenarios.selectOption('stale');
  await expect(page.getByText('Terlambat', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Lihat detail / }).first()).toBeVisible();

  await scenarios.selectOption('empty');
  await expect(page.getByRole('heading', { name: /belum ada kejadian|tidak ada kejadian/i })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Lihat detail / })).toHaveCount(0);

  await scenarios.selectOption('error');
  await expect(page.getByRole('alert').first()).toBeVisible();
  await expect(page.getByRole('button', { name: 'Coba lagi', exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Coba lagi', exact: true }).click();
  // The selected error scenario remains active until the user chooses normal.
  await expect(page.getByRole('alert').first()).toBeVisible();

  await scenarios.selectOption('normal');
  await expect(page.getByRole('button', { name: /^Lihat detail / }).first()).toBeVisible();
  await expect(page.getByRole('button', { name: 'Coba lagi', exact: true })).toHaveCount(0);
});

test('login dan dashboard dapat digunakan pada layar ponsel tanpa overflow', async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Selamat datang.' })).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: testInfo.outputPath('login-mobile.png'), fullPage: true });

  await page.getByRole('button', { name: 'Pratinjau sebagai Media, akun media-demo' }).click();
  await expect(page.getByRole('heading', { name: 'Situasi kebencanaan' })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Lihat detail / }).first()).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: testInfo.outputPath('dashboard-mobile.png'), fullPage: true });
  await expect(page.getByRole('complementary', { name: 'Menu utama' })).not.toBeVisible();
  await page.getByRole('button', { name: 'Buka menu' }).click();
  await page.getByRole('button', { name: /^Notifikasi/ }).click();
  await expect(page.getByRole('heading', { name: 'Pusat notifikasi', exact: true })).toBeVisible();
  await expect(page.getByRole('complementary', { name: 'Menu utama' })).not.toBeVisible();
});
