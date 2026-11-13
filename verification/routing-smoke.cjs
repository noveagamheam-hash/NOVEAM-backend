const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync('public_html/js/page-router.js', 'utf8');

(async () => {
  for (const [policy, completed, expected] of [
    [{ accepted: true, onboarding_completed: true }, false, '/feed.html'],
    [{ accepted: true, onboarding_completed: false }, false, '/visitor/onboarding.html'],
    [{ accepted: false }, false, '/rules.html?from=signin'],
    [null, false, '/visitor/onboarding.html'],
    [null, true, '/feed.html'],
  ]) {
    const destinations = [];
    const window = {
      NOVEAM_PAGE_ROUTES: { login: 'login.html' },
      NoveamAPI: { json: async path => path.endsWith("/api/onboarding/me") ? { completed } : policy },
      noveamApiUrl: path => 'https://staging.example.test' + path,
    };
    vm.runInNewContext(source, {
      window,
      URL,
      location: { origin: 'https://staging.example.test', replace: path => destinations.push(path) },
      localStorage: { getItem: () => 'test-token' },
      sessionStorage: { getItem: () => null },
    });
    await window.noveamVisitor.routeAfterLogin(policy);
    assert.deepEqual(destinations, [expected]);
  }
  console.log('PASS Rules acceptance, pending and completed onboarding, and policy-disabled registration routing');
})().catch(error => { console.error(error); process.exitCode = 1; });
