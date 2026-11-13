const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const origin = 'https://noveam.test';
const handlers = {};
const calls = [];
let offline = false;
vm.runInNewContext(fs.readFileSync('public_html/sw.js', 'utf8'), {
  URL, Response, self: { location: { origin }, addEventListener: (name, fn) => { handlers[name] = fn; } },
  fetch: async (request, options) => {
    calls.push([new URL(request.url).pathname, options.cache]);
    if (offline) throw new Error('Offline');
    return { status: 200 };
  },
  caches: { match: async path => path === '/offline.html' ? { offlinePage: true } : undefined }
});
function workerFetch(path, mode = 'navigate', authorized = false) {
  let response;
  handlers.fetch({
    request: { method: 'GET', url: origin + path, mode, headers: { has: () => authorized } },
    respondWith(value) { response = value; }
  });
  return response;
}
(async () => {
  for (const path of ['/onboarding.html', '/visitor/onboarding.html', '/visitor/onboarding.js',
    '/visitor/onboarding.css', '/js/security-setup.js', '/NOVEAM-navigation.js', '/js/onboarding-adapter.js']) {
    assert.equal((await workerFetch(path)).status, 200);
    assert.deepEqual(calls.at(-1), [path, 'no-store']);
  }
  assert.equal(workerFetch('/api/onboarding/me'), undefined);
  assert.equal(workerFetch('/onboarding.html', 'navigate', true), undefined);
  offline = true;
  assert.equal((await workerFetch('/onboarding.html')).offlinePage, true);
  assert.equal((await workerFetch('/visitor/onboarding.html')).offlinePage, true);
  assert.equal((await workerFetch('/visitor/onboarding.js', 'cors')).type, 'error');
  assert.match(fs.readFileSync('public_html/feed.html', 'utf8'), /<a class="navlink" href="\/onboarding.html">/);
  assert.match(fs.readFileSync('public_html/NOVEAM-navigation.js', 'utf8'), /\["\/onboarding.html", "Onboarding"\]/);
  console.log('PASS service worker cache and offline routing, and Onboarding menu placement');
})().catch(error => { console.error(error); process.exitCode = 1; });
