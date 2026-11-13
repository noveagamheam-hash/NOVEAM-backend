const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { performance } = require('node:perf_hooks');
const { chromium } = require(process.env.NOVEAM_PLAYWRIGHT || 'playwright');

const root = path.resolve('public_html');
const origin = 'http://127.0.0.1:8000';
const types = { '.js': 'application/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json' };

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.NOVEAM_CHROME });
  try {
    const page = await browser.newPage({ serviceWorkers: 'block', viewport: { width: 375, height: 812 } });
    await page.addInitScript(() => {
      const tokenKey = 'noveagamheamApiToken';
      if (!localStorage.getItem(tokenKey) && !sessionStorage.getItem(tokenKey)) localStorage.setItem(tokenKey, 'test');
      localStorage.setItem('noveamAppearance', 'dark');
      window.SpeechSynthesisUtterance = class {
        constructor(text) { this.text = text; }
      };
      Object.defineProperty(window, 'speechSynthesis', { value: {
        getVoices: () => [{ lang: 'en-US', localService: true }],
        speak: utterance => { window.testNarration = utterance.text; },
        cancel: () => { window.testNarration = null; }
      } });
    });
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route(origin + '/**', route => {
      const file = path.resolve(root, '.' + new URL(route.request().url()).pathname);
      if (!file.startsWith(root + path.sep) || !fs.existsSync(file)) return route.fulfill({ status: 404 });
      return route.fulfill({ contentType: types[path.extname(file)] || 'text/html', body: fs.readFileSync(file) });
    });
    await page.route('https://fonts.googleapis.com/**', route => route.abort());
    let accepted = false;
    const digest = 'a'.repeat(64);
    let verified = false;
    const state = { step: 0, wizard_step: null, revision: 0, display_name: 'New Member',
      interests: [], preferences: {}, completed: false };
    await page.route('**/api/**', route => {
      const request = route.request();
      const pathname = new URL(request.url()).pathname;
      const send = (code, json) => route.fulfill({ status: code, json });
      if (pathname === '/api/auth/register') return send(200, { token: 'registration-token', requires_mfa_setup: true });
      if (pathname === '/api/v1/policies/current') return send(200, { version: '1.6', digest, terms: 'Test terms',
        sections: Array.from({ length: 60 }, (_, index) => ({ id: index + 1, chapter: 'Rules', title: 'Rule', text: 'Test rule' })) });
      if (pathname === '/api/v1/policies/status') return send(200, { accepted, version: '1.6', digest, onboarding_completed: false });
      if (pathname === '/api/v1/policies/accept') {
        assert.deepEqual(request.postDataJSON(), { version: '1.6', digest, accepted: true });
        assert.equal(request.headers().authorization, 'Bearer registration-token');
        accepted = true;
        return send(200, { accepted, digest, next: '/visitor/onboarding.html' });
      }
      if (pathname === '/api/auth/2fa/status') return send(200, { enabled: verified, recovery_remaining: 1 });
      if (pathname === '/api/auth/2fa/setup') return send(200, { secret: 'TESTSETUPKEY', qr_code: '/icons/icon-192.png' });
      if (pathname === '/api/auth/2fa/enable') {
        if (request.postDataJSON().code !== '123456') return send(400, { detail: 'Invalid verification code' });
        verified = true;
        return send(200, { token: 'verified', recovery_codes: ['TEST-RECOVERY'] });
      }
      if (pathname === '/api/onboarding/me' && request.method() === 'PATCH') {
        const draft = request.postDataJSON();
        if (draft.wizard_step >= 5) assert.equal(request.headers().authorization, 'Bearer verified');
        Object.assign(state, draft, { wizard_step: Math.max(state.wizard_step, draft.wizard_step),
          revision: state.revision + 1, preferences: { ...state.preferences, ...draft.preferences } });
      }
      if (pathname === '/api/onboarding/complete') {
        assert.equal(verified, true);
        assert.equal(state.wizard_step, 6);
        state.completed = true;
      }
      if (pathname.startsWith('/api/onboarding/')) return send(200, state);
      return send(200, { id: 1 });
    });
    await page.route(origin + '/feed.html', route => route.fulfill({ contentType: 'text/html', body: '<h1>News Feed</h1>' }));
    const heading = name => page.getByRole('heading', { name, exact: true }).waitFor();
    const next = () => page.locator('#next').click();
    await page.goto(origin + '/register.html');
    const fields = { displayName: 'New Member', email: 'new@example.test', phone: '+919999999999',
      password: 'test-password', confirmPassword: 'test-password' };
    for (const [id, value] of Object.entries(fields)) await page.locator('#' + id).fill(value);
    await page.getByRole('button', { name: 'Create Account' }).click();
    await heading('Registration is Successful!');
    await page.getByRole('button', { name: 'OK', exact: true }).click();
    await heading('Welcome to NOVEAM Pre-Alpha!');
    await page.getByRole('button', { name: 'OK, Continue' }).click();
    await page.waitForURL(origin + '/rules.html?from=signin');
    await page.getByText('Step 1 of 7 · Rules & Privacy').waitFor();
    await page.locator('#agree').check();
    await page.getByRole('button', { name: 'Agree and continue' }).click();
    await heading('Profile setup');
    assert.equal(await page.locator('html').getAttribute('data-noveam-theme'), 'dark');
    await page.waitForFunction(() => document.querySelector('.nv-brand img').naturalWidth > 0);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    if (!process.env.NOVEAM_BASELINE) {
      const theme = page.getByLabel('Appearance', { exact: true });
      for (const value of ['light', 'system', 'dark']) {
        await theme.selectOption(value);
        assert.equal(await page.evaluate(() => localStorage.getItem('noveamAppearance')), value);
        if (value !== 'system') assert.equal(await page.locator('html').getAttribute('data-noveam-theme'), value);
      }
      await page.getByRole('button', { name: 'Read step aloud' }).click();
      assert.match(await page.evaluate(() => window.testNarration), /^Profile setup\. Use a name/);
      await page.getByRole('button', { name: 'Stop reading' }).click();
      assert.equal(await page.evaluate(() => window.testNarration), null);
    }
    await next();
    await heading('Language & accessibility');
    await page.locator('#preferred-language').selectOption('te');
    await page.locator('#large-text').check();
    if (!process.env.NOVEAM_BASELINE) {
      const shell = page.locator('.onboarding-shell');
      assert.equal(await shell.getAttribute('data-large-text'), 'true');
      await page.locator('#reduced-motion').check();
      assert.equal(await shell.getAttribute('data-reduced-motion'), 'true');
      await page.locator('#reduced-motion').uncheck();
    }
    await next();
    await heading('Interests & preferences');
    await page.goto(origin + '/onboarding.html');
    await heading('Interests & preferences');
    if (process.env.NOVEAM_BENCHMARK) {
      const durations = [];
      for (const width of [320, 375, 768, 1440]) {
        await page.setViewportSize({ width, height: 812 });
        for (let sample = 0; sample < 5; sample += 1) {
          const start = performance.now();
          await page.goto(origin + (sample % 2 === 0 ? '/onboarding.html' : '/visitor/onboarding.html'),
            { waitUntil: 'commit' });
          await heading('Interests & preferences');
          durations.push(performance.now() - start);
          assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
        }
      }
      durations.sort((a, b) => a - b);
      console.log(`Navigation start to visible resumed step (n=20): p50=${durations[9].toFixed(1)} ms, p95=${durations[18].toFixed(1)} ms, max=${durations[19].toFixed(1)} ms`);
      await page.setViewportSize({ width: 375, height: 812 });
    }
    await page.getByText('Learning', { exact: true }).last().click();
    await next();
    await heading('Security & MFA');
    const code = page.getByLabel('Current authenticator code');
    await code.fill('000000');
    await page.getByRole('button', { name: 'Verify and activate' }).click();
    await page.getByText('Invalid verification code', { exact: true }).waitFor();
    await code.fill('123456');
    await page.getByRole('button', { name: 'Verify and activate' }).click();
    await page.getByText('TEST-RECOVERY', { exact: true }).waitFor();
    await page.getByRole('button', { name: 'Continue to NOVEAM' }).click();
    await heading('Notifications');
    await page.locator('#notifications').uncheck();
    await next();
    await heading('Review & finish');
    await next();
    await heading('You’re ready to go!');
    await page.getByRole('button', { name: 'Go to Feed' }).waitFor();
    await page.waitForURL(origin + '/feed.html');
    assert.deepEqual(state.preferences, { language: 'te', large_text: true, notifications: false });
    assert.deepEqual(errors, []);
    console.log('PASS registration dialogs, ordered onboarding, resume, MFA retry, preferences, finish dialog and Feed');
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
