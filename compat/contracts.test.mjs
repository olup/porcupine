import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn, execFileSync } from 'node:child_process';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { gunzipSync } from 'node:zlib';
import { once } from 'node:events';
import { chromium } from 'playwright';
import { PostHog } from 'posthog-node';
import { browserProfile, cookielessProfile, nodeProfile } from './profiles.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');
const matrix = JSON.parse(await readFile(join(here, 'matrix.json'), 'utf8'));
const installed = async name => JSON.parse(await readFile(join(here, 'node_modules', name, 'package.json'), 'utf8')).version;
const versions = { 'posthog-js': await installed('posthog-js'), 'posthog-node': await installed('posthog-node'), playwright: await installed('playwright') };
const report = { schema_version: 1, mode: process.env.PORCUPINE_COMPAT_CANARY ? 'canary' : 'pinned', versions, profile: 'cookieless-product-and-identified-diagnostics', configuration: { browser_cookieless: cookielessProfile, browser_identified_diagnostic: browserProfile, node_identified_diagnostic: nodeProfile, browser_batch_diagnostic: { ...browserProfile, request_batching: true, disable_compression: false, request_queue_config: { flush_interval_ms: 50 } } }, results: [], unsupported: matrix.unsupported };
let directory, child, api, origin, browser, context, page, site, project, otherProject, proxy, proxyURL;
const admin = randomBytes(32).toString('hex');
const blocked = [], unexpected = [], wires = [], browserWires = [], browserFailures = [];
let failAcknowledgement = false;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; };
const close = server => server ? new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }) : Promise.resolve();
const decodeWire = wire => JSON.parse((wire.headers['content-encoding'] === 'gzip' || wire.body[0] === 0x1f && wire.body[1] === 0x8b ? gunzipSync(wire.body) : wire.body).toString());
const safeFetch = async (url, options) => {
  if (new URL(url).origin !== proxyURL) { blocked.push('node external request'); throw new Error('SDK request outside local capture origin'); }
  return fetch(url, options);
};
async function management(path, { method = 'GET', body, token = admin } = {}) {
  const response = await fetch(api + path, { method, headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  assert.equal(response.status, method === 'POST' ? 201 : 200, 'management HTTP status');
  return response.json();
}
const events = async (id = project.project.id) => (await management(`/api/v1/projects/${id}/events?limit=1000`)).events;
async function event(name, count = 1, projectID = project.project.id) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const found = (await events(projectID)).filter(e => e.event === name);
    if (found.length >= count) { assert.equal(found.length, count, 'event count'); return found; }
    await delay(50);
  }
  throw new Error(`persisted event missing: ${name}`);
}
function contract(id, run) {
  assert.ok(matrix.cases.some(item => item.id === id), `test ${id} is declared in matrix`);
  test(id, { timeout: 20000 }, async () => {
    try { await run(); report.results.push({ id, status: 'passed' }); }
    catch (error) { report.results.push({ id, status: 'failed' }); throw error; }
  });
}
before(async () => {
  assert.ok(matrix.cases.some(item => item.required), 'matrix has required contracts');
  const goTests = execFileSync('go', ['test', '-list', '.', './internal/porcupine'], { cwd: root, encoding: 'utf8' }).split(/\r?\n/);
  for (const item of matrix.go_contracts) assert.ok(goTests.includes(item.test), `Go contract exists: ${item.test}`);
  for (const fixture of matrix.fixtures) {
    const recorded = JSON.parse(await readFile(join(here, fixture), 'utf8'));
    assert.ok(recorded.provenance.sdk && recorded.provenance.version, 'fixture has SDK provenance');
    assert.ok(recorded.payload, 'fixture has a payload');
  }
  directory = await mkdtemp(join(tmpdir(), 'porcupine-compat-'));
  const binary = join(directory, 'porcupine');
  execFileSync('go', ['build', '-o', binary, './cmd/porcupine'], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, CGO_ENABLED: '0' } });
  site = createServer(async (request, response) => {
    if (request.url === '/') { response.setHeader('content-type', 'text/html'); response.end('<!doctype html><title>Porcupine SDK contract</title><script src="/sdk.js"></script>'); }
    else if (request.url === '/sdk.js') { response.setHeader('content-type', 'text/javascript'); response.end(await readFile(join(here, 'node_modules/posthog-js/dist/array.no-external.js'))); }
    else if (request.url === '/favicon.ico') { response.writeHead(204).end(); }
    else { unexpected.push('page route'); response.writeHead(404).end(); }
  });
  origin = await listen(site);
  const portReservation = createServer();
  api = await listen(portReservation);
  await close(portReservation);
  child = spawn(binary, [], { cwd: directory, env: { ...process.env, PORCUPINE_ADDR: new URL(api).host, PORCUPINE_DB: join(directory, 'events.db'), PORCUPINE_BOOTSTRAP_TOKEN: admin, PORCUPINE_ALLOWED_ORIGINS: origin }, stdio: 'ignore' });
  for (let attempt = 0; attempt < 200; attempt++) {
    try { if ((await fetch(api + '/readyz')).ok) break; } catch {}
    if (attempt === 199) throw new Error('Go server did not become ready');
    await delay(50);
  }
  project = await management('/api/v1/projects', { method: 'POST', body: { name: 'SDK contracts' } });
  otherProject = await management('/api/v1/projects', { method: 'POST', body: { name: 'Other project' } });
  proxy = createServer(async (request, response) => {
    const url = new URL(request.url, api);
    if (url.pathname !== '/batch/' || request.method !== 'POST') { unexpected.push('node route'); response.writeHead(404).end(); return; }
    const chunks = []; for await (const chunk of request) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    wires.push({ path: url.pathname, headers: request.headers, body });
    const upstream = await fetch(api + request.url, { method: request.method, headers: { 'content-type': request.headers['content-type'] || 'application/json', ...(request.headers['content-encoding'] ? { 'content-encoding': request.headers['content-encoding'] } : {}) }, body });
    const text = await upstream.text();
    if (failAcknowledgement && upstream.ok) { failAcknowledgement = false; response.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"injected lost acknowledgement"}'); return; }
    response.writeHead(upstream.status, { 'content-type': 'application/json' }).end(text);
  });
  proxyURL = await listen(proxy);
  browser = await chromium.launch({ headless: true });
  report.browser = { name: "chromium", version: browser.version() };
  context = await browser.newContext({ serviceWorkers: 'block' });
  await context.route('**/*', async route => {
    const url = new URL(route.request().url());
    if (![api, origin].includes(url.origin)) { blocked.push('browser external request'); await route.abort(); return; }
    if (url.origin === api && !['/i/v0/e/', '/e/', '/capture/'].includes(url.pathname)) { unexpected.push(`browser ${url.pathname}`); await route.abort(); return; }
    if (url.origin === api && route.request().method() === 'POST') browserWires.push({ path: url.pathname, headers: route.request().headers(), body: route.request().postDataBuffer(), query: url.searchParams.has('compression') ? { compression: url.searchParams.get('compression') } : {} });
    await route.continue();
  });
  page = await context.newPage();
  page.on('requestfailed', request => browserFailures.push(new URL(request.url()).pathname));
  await page.goto(origin);
  await page.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token: project.ingest_token, profile: browserProfile });
}, { timeout: 60000 });

after(async () => {
  await context?.close(); await browser?.close();
  await close(proxy); await close(site);
  if (child && child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'exit'); }
  if (directory) await rm(directory, { recursive: true, force: true });
  for (const item of matrix.cases.filter(item => item.required)) if (!report.results.some(result => result.id === item.id)) report.results.push({ id: item.id, status: 'missing' });
  report.passed = report.results.every(result => result.status === 'passed') && blocked.length === 0 && unexpected.length === 0 && browserFailures.length === 0;
  report.network = { external_requests: blocked.length, unexpected_routes: unexpected.length, node_requests: wires.length, browser_requests: browserWires.length, browser_failed_requests: browserFailures.length, node_transport: wires.map(wire => ({ path: wire.path, encoding: wire.headers['content-encoding'] || 'identity', content_type: wire.headers['content-type'] })), browser_transport: browserWires.map(wire => ({ path: wire.path, encoding: wire.headers['content-encoding'] || wire.query.compression || (wire.body[0] === 0x1f && wire.body[1] === 0x8b ? 'gzip-magic' : 'identity'), content_type: wire.headers['content-type'] })) };
  if (process.env.PORCUPINE_UPDATE_FIXTURES === '1') await writeFixtures();
  await mkdir(join(here, 'artifacts'), { recursive: true });
  await writeFile(join(here, 'artifacts/report.json'), JSON.stringify(report, null, 2) + '\n');
  assert.equal(report.passed, true, 'all required matrix cases must pass');
});

contract('node.capture-flush', async () => {
  const client = new PostHog(project.ingest_token, { ...nodeProfile, host: proxyURL, fetch: safeFetch });
  try {
    client.capture({ distinctId: 'node-user-é', event: 'node custom', timestamp: new Date('2025-02-03T04:05:06.789Z'), properties: { nested: { unicode: '東京 🦔', list: [1, true, null] }, number: 42 } });
    await client.flush();
    const [saved] = await event('node custom');
    assert.equal(saved.distinct_id, 'node-user-é');
    assert.equal(saved.timestamp, '2025-02-03T04:05:06.789Z');
    assert.deepEqual(saved.properties.nested, { unicode: '東京 🦔', list: [1, true, null] });
    assert.equal(saved.properties.number, 42);
    assert.equal(saved.properties.$lib, 'posthog-node');
    assert.equal(saved.properties.$lib_version, versions['posthog-node']);
    assert.ok(saved.uuid);
  } finally { await client.shutdown(); }
});
contract('node.shutdown-batch', async () => {
  const client = new PostHog(project.ingest_token, { ...nodeProfile, host: proxyURL, fetch: safeFetch });
  client.capture({ distinctId: 'shutdown-user', event: 'shutdown a' });
  client.capture({ distinctId: 'shutdown-user', event: 'shutdown b' });
  await client.shutdown();
  await event('shutdown a'); await event('shutdown b');
  assert.ok(wires.some(wire => decodeWire(wire).batch.some(item => item.event === 'shutdown a') && decodeWire(wire).batch.some(item => item.event === 'shutdown b')));
});
contract('node.retry-stable-uuid', async () => {
  const client = new PostHog(project.ingest_token, { ...nodeProfile, host: proxyURL, fetch: safeFetch });
  const start = wires.length;
  failAcknowledgement = true;
  try {
    client.capture({ distinctId: 'retry-user', event: 'retry after commit', properties: { answer: 42 } });
    await client.flush();
    const attempts = wires.slice(start).map(decodeWire);
    assert.equal(attempts.length, 2, 'SDK retries the injected HTTP 503');
    const first = attempts[0].batch.find(item => item.event === 'retry after commit');
    const second = attempts[1].batch.find(item => item.event === 'retry after commit');
    assert.ok(first.uuid); assert.equal(first.uuid, second.uuid);
    const [saved] = await event('retry after commit');
    assert.equal(saved.uuid, first.uuid);
    assert.equal(saved.properties.answer, 42);
  } finally { await client.shutdown(); }
});
contract('browser.capture-cors', async () => {
  const responsePromise = page.waitForResponse(response => new URL(response.url()).origin === api && response.request().method() === 'POST');
  const result = await page.evaluate(() => {
    const distinct = window.posthog.get_distinct_id();
    const session = window.posthog.get_session_id();
    if (!posthog.__loaded || posthog.has_opted_out_capturing()) throw new Error(JSON.stringify({loaded:posthog.__loaded,optout:posthog.has_opted_out_capturing()}));
    const captured = window.posthog.capture('browser custom', { nested: { unicode: 'café 東京 🦔', list: [false, 3, null] }, number: 7 }, { timestamp: new Date('2025-03-04T05:06:07.123Z'), send_instantly: true });
    if (!captured) throw new Error('SDK capture returned undefined; loaded=' + posthog.__loaded + '; botFilter=' + posthog.config.opt_out_useragent_filter);
    return { distinct, session: captured.properties.$session_id, uuid: captured.uuid };
  });
  const response = await responsePromise;
  assert.equal(response.status(), 200);
  assert.equal(await response.headerValue('access-control-allow-origin'), origin);
  assert.equal(await response.finished(), null, 'Chromium completes SDK fetch response');
  assert.equal(response.request().failure(), null);
  const [saved] = await event('browser custom');
  assert.equal(new URL(api).origin === origin, false, 'capture uses a different browser origin');
  assert.equal(saved.uuid, result.uuid);
  assert.equal(saved.distinct_id, result.distinct);
  assert.equal(saved.timestamp, '2025-03-04T05:06:07.123Z');
  assert.equal(saved.properties.$session_id, result.session);
  assert.deepEqual(saved.properties.nested, { unicode: 'café 東京 🦔', list: [false, 3, null] });
  assert.equal(saved.properties.number, 7);
  assert.equal(saved.properties.$lib, 'web');
  assert.equal(saved.properties.$lib_version, versions['posthog-js']);
});
contract('browser.batch-compression', async () => {
  const start = browserWires.length;
  await page.evaluate(({ host, token, profile }) => {
    const batched = window.posthog.init(token, { ...profile, api_host: host, request_batching: true, disable_compression: false, request_queue_config: { flush_interval_ms: 50 } }, 'batched');
    batched.capture('browser batch a', { order: 1 });
    batched.capture('browser batch b', { order: 2 });
  }, { host: api, token: project.ingest_token, profile: browserProfile });
  await event('browser batch a'); await event('browser batch b');
  assert.ok(browserWires.slice(start).map(decodeWire).some(payload => payload.batch.some(item => item.event === 'browser batch a') && payload.batch.some(item => item.event === 'browser batch b')), 'both events share a real SDK batch');
  assert.ok(browserWires.slice(start).some(wire => wire.query.compression === 'gzip-js' || wire.headers['content-encoding'] === 'gzip' || wire.body[0] === 0x1f && wire.body[1] === 0x8b), 'real browser SDK used gzip transport');
});
contract('browser.persistence-reset', async () => {
  const first = await page.evaluate(() => ({ distinct: posthog.get_distinct_id(), session: posthog.get_session_id() }));
  await page.reload();
  await page.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token: project.ingest_token, profile: browserProfile });
  const reloaded = await page.evaluate(() => ({ distinct: posthog.get_distinct_id(), session: posthog.get_session_id() }));
  assert.deepEqual(reloaded, first);
  const reset = await page.evaluate(() => { posthog.reset(); const distinct = posthog.get_distinct_id(); const session = posthog.get_session_id(); posthog.capture('browser after reset', { reset: true }, { send_instantly: true }); return { distinct, session }; });
  assert.notEqual(reset.distinct, first.distinct); assert.notEqual(reset.session, first.session);
  const [saved] = await event('browser after reset');
  assert.equal(saved.distinct_id, reset.distinct); assert.equal(saved.properties.$session_id, reset.session);
});
contract('browser.cookieless-no-storage', async () => {
  const userAgent = 'Porcupine compatibility browser cookieless contract';
  const freshContext = async ua => {
    const isolated = await browser.newContext({ serviceWorkers: 'block', userAgent: ua });
    await isolated.route('**/*', async route => {
      const request = route.request();
      const url = new URL(request.url());
      if (![api, origin].includes(url.origin)) { blocked.push('cookieless external request'); await route.abort(); return; }
      if (url.origin === api && !['/i/v0/e/', '/e/', '/capture/'].includes(url.pathname)) { unexpected.push(`cookieless ${url.pathname}`); await route.abort(); return; }
      if (url.origin === api && request.method() === 'POST') browserWires.push({ path: url.pathname, headers: request.headers(), body: request.postDataBuffer(), query: {} });
      await route.continue();
    });
    isolated.on('requestfailed', request => browserFailures.push(new URL(request.url()).pathname));
    return isolated;
  };
  const assertNoStorage = async isolated => {
    const state = await isolated.storageState();
    assert.deepEqual(state.cookies, [], 'cookieless SDK writes no cookies');
    assert.ok(state.origins.every(item => item.localStorage.length === 0), 'cookieless SDK writes no localStorage');
    for (const tab of isolated.pages()) assert.equal(await tab.evaluate(() => sessionStorage.length), 0, 'cookieless SDK writes no sessionStorage');
  };
  const init = async (tab, token) => {
    await tab.goto(origin);
    await tab.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token, profile: cookielessProfile });
  };
  const capture = async (tab, name, projectID = project.project.id) => {
    const responsePromise = tab.waitForResponse(response => new URL(response.url()).origin === api && response.request().method() === 'POST');
    const clientID = await tab.evaluate(name => {
      const id = window.posthog.get_distinct_id();
      window.posthog.capture(name, { nested: { unicode: 'café 東京 🦔' }, $ip: '198.51.100.41', ip: '198.51.100.42', $session_id: 'discard-session', $window_id: 'discard-window', $device_id: 'discard-device', $user_id: 'discard-user' }, { send_instantly: true });
      return id;
    }, name);
    assert.equal(clientID, '$posthog_cookieless', 'SDK never receives a durable visitor ID');
    const response = await responsePromise;
    assert.equal(response.status(), 200, 'cookieless capture succeeds');
    assert.equal(await response.headerValue('access-control-allow-origin'), origin);
    assert.equal(await response.finished(), null, 'Chromium completes the CORS fetch');
    assert.equal(response.request().failure(), null);
    assert.deepEqual(await response.json(), { status: 1, accepted: 1, duplicates: 0 });
    const sent = decodeWire({ headers: response.request().headers(), body: response.request().postDataBuffer() }).batch;
    assert.equal(sent.length, 1);
    assert.equal(sent[0].properties.distinct_id, '$posthog_cookieless', 'the actual SDK request carries the sentinel');
    const [saved] = await event(name, 1, projectID);
    assert.match(saved.distinct_id, /^cookieless_[a-f0-9]+$/);
    assert.equal(saved.properties.distinct_id, saved.distinct_id);
    assert.equal(saved.properties.cookieless, true);
    assert.match(saved.properties.identity_day, /^\d{4}-\d{2}-\d{2}$/);
    assert.deepEqual(saved.properties.nested, { unicode: 'café 東京 🦔' });
    for (const key of ['$ip', 'ip', '$session_id', '$window_id', '$device_id', '$user_id']) assert.equal(key in saved.properties, false, `${key} is removed before persistence`);
    for (const value of ['$posthog_cookieless', '198.51.100.41', '198.51.100.42', 'discard-session', 'discard-window', 'discard-device', 'discard-user']) assert.equal(JSON.stringify(saved).includes(value), false, 'stored event contains no raw IP or reserved correlation value');
    return saved;
  };
  const contexts = [];
  try {
    const isolated = await freshContext(userAgent); contexts.push(isolated);
    const tab = await isolated.newPage();
    await init(tab, project.ingest_token);
    const first = await capture(tab, 'cookieless first');
    const second = await capture(tab, 'cookieless second');
    assert.equal(first.distinct_id, second.distinct_id, 'events share the server daily pseudonym');
    await assertNoStorage(isolated);
    await init(tab, project.ingest_token);
    const reloaded = await capture(tab, 'cookieless reload');
    assert.equal(first.distinct_id, reloaded.distinct_id, 'reload needs no persisted browser ID');
    await assertNoStorage(isolated);
    const fresh = await freshContext(userAgent); contexts.push(fresh);
    const freshTab = await fresh.newPage();
    await init(freshTab, project.ingest_token);
    const freshEvent = await capture(freshTab, 'cookieless fresh context');
    assert.equal(first.distinct_id, freshEvent.distinct_id, 'same project, address and user agent share the receipt-day pseudonym');
    await assertNoStorage(fresh);
    const different = await freshContext(userAgent + ' different'); contexts.push(different);
    const differentTab = await different.newPage();
    await init(differentTab, project.ingest_token);
    const differentEvent = await capture(differentTab, 'cookieless different user agent');
    assert.notEqual(first.distinct_id, differentEvent.distinct_id);
    await assertNoStorage(different);
    assert.deepEqual(await events(otherProject.project.id), [], 'other project starts empty');
    await init(freshTab, otherProject.ingest_token);
    const other = await capture(freshTab, 'cookieless other project', otherProject.project.id);
    assert.notEqual(first.distinct_id, other.distinct_id, 'pseudonyms are project scoped');
    await assertNoStorage(fresh);
  } finally { for (const isolated of contexts) await isolated.close(); }
});
contract('isolation-and-network', async () => {
  const otherEvents = await events(otherProject.project.id);
  assert.equal(otherEvents.length, 1);
  assert.equal(otherEvents[0].event, 'cookieless other project');
  assert.equal(otherEvents[0].project_id, otherProject.project.id);
  assert.deepEqual(blocked, []); assert.deepEqual(unexpected, []); assert.deepEqual(browserFailures, []);
  for (const saved of await events()) {
    assert.equal(saved.project_id, project.project.id);
    assert.equal(JSON.stringify(saved).includes(project.ingest_token), false, 'credential is absent from persisted event');
  }
  if (!process.env.PORCUPINE_COMPAT_CANARY) assert.deepEqual(versions, matrix.supported_versions, 'pinned run uses declared package versions');
  const ids = matrix.cases.map(item => item.id);
  assert.equal(new Set(ids).size, ids.length, 'matrix IDs are unique');
});

async function writeFixtures() {
  const replacements = new Map();
  const normalizedUUID = original => {
    if (!replacements.has(original)) replacements.set(original, '01900000-0000-7000-8000-' + String(replacements.size + 1).padStart(12, '0'));
    return replacements.get(original);
  };
  const normalize = value => {
    if (Array.isArray(value)) return value.map(normalize);
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, ['api_key', 'token'].includes(key) ? 'FIXTURE_INGEST_TOKEN' : ['$host', '$session_entry_host'].includes(key) ? 'browser.example' : key === '$insert_id' ? 'fixture-insert-id' : key === '$time' ? 1738555506.789 : normalize(item)]));
    if (typeof value === 'string') return value.replaceAll(project.ingest_token, 'FIXTURE_INGEST_TOKEN').replaceAll(origin, 'http://browser.example').replaceAll(api, 'http://ingest.example').replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, normalizedUUID).replace(/20\d\d-\d\d-\d\dT[0-9:.]+Z/g, '2025-02-03T04:05:06.789Z');
    return value;
  };
  await mkdir(join(here, 'fixtures'), { recursive: true });
  for (const [name, wire, version] of [['node-batch', wires[0], versions['posthog-node']], ['browser-capture', browserWires[0], versions['posthog-js']], ['browser-batch-gzip', browserWires.find(wire => wire.body[0] === 0x1f && wire.body[1] === 0x8b), versions['posthog-js']]]) {
    if (!wire) continue;
    const payload = decodeWire(wire);
    await writeFile(join(here, 'fixtures', name + '.json'), JSON.stringify({ provenance: { sdk: name.startsWith('node') ? 'posthog-node' : 'posthog-js', version, profile: name === 'browser-batch-gzip' ? 'analytics-only-gzip-batch' : 'analytics-only', source: 'Observed real SDK HTTP request to temporary Go server', normalization: 'Credentials, UUIDs, timestamps, local addresses replaced; remaining SDK properties retained', path: wire.path, content_type: wire.headers['content-type'], content_encoding: wire.headers['content-encoding'] || 'identity', body_format: wire.body[0] === 0x1f && wire.body[1] === 0x8b ? 'gzip' : 'json' }, payload: normalize(payload) }, null, 2) + '\n');
  }
}
