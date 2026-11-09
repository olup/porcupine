// Local-only real SDK -> Go/SQLite -> ClickHouse -> project-scoped SQL proof.
// Run after npm ci: node compat/identify-clickhouse.mjs
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn, execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { once } from 'node:events';
import { chromium } from 'playwright';
import { PostHog } from 'posthog-node';
import { identifiedBrowserProfile, nodeProfile } from './profiles.mjs';

const here = dirname(fileURLToPath(import.meta.url)), root = resolve(here, '..');
const image = 'clickhouse/clickhouse-server:26.8.36.3';
const suffix = randomBytes(6).toString('hex'), container = 'porcupine-sdk-identify-' + suffix;
const bootstrap = randomBytes(32).toString('hex'), password = randomBytes(32).toString('hex');
const environment = { ...process.env, CLICKHOUSE_USER: 'porcupine_admin', CLICKHOUSE_PASSWORD: password, CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: '1' };
const docker = args => execFileSync('docker', args, { cwd: root, env: environment, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return 'http://127.0.0.1:' + server.address().port; };
const close = server => server ? new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }) : Promise.resolve();
let directory, app, browser, site, api, origin, project, created = false, phase = 'local setup';
const failures = [], external = [], transport = [];
const report = { passed: false, production_connected: false, profile: identifiedBrowserProfile, clickhouse_image: image };
async function request(path, body, token = bootstrap) {
  const response = await fetch(api + path, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000) });
  const payload = await response.json();
  if (response.status !== (path === '/api/v1/projects' ? 201 : 200)) report.failed_http = { status: response.status, detail: payload.detail || payload.error };
  assert.equal(response.status, path === '/api/v1/projects' ? 201 : 200, phase);
  return payload;
}
try {
  directory = await mkdtemp(join(tmpdir(), 'porcupine-sdk-identify-'));
  const binary = join(directory, 'porcupine');
  execFileSync('go', ['build', '-o', binary, './cmd/porcupine'], { cwd: root, env: { ...process.env, CGO_ENABLED: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  docker(['run', '-d', '--name', container, '--label', 'porcupine.integration=true', '--memory', '2g', '--cpus', '2', '-p', '127.0.0.1::8123', '-e', 'CLICKHOUSE_USER', '-e', 'CLICKHOUSE_PASSWORD', '-e', 'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT', image]);
  created = true;
  const clickhouse = 'http://127.0.0.1:' + docker(['port', container, '8123/tcp']).split(':').at(-1);
  phase = 'ClickHouse readiness';
  for (let attempt = 0; ; attempt++) {
    try {
      const response = await fetch(clickhouse, { method: 'POST', headers: { 'X-ClickHouse-User': environment.CLICKHOUSE_USER, 'X-ClickHouse-Key': password }, body: 'SELECT 1', signal: AbortSignal.timeout(2000) });
      if (response.ok && (await response.text()).trim() === '1') break;
    } catch {}
    if (attempt === 149) throw new Error('readiness');
    await delay(200);
  }
  site = createServer(async (req, res) => {
    if (req.url === '/sdk.js') { res.setHeader('content-type', 'text/javascript'); res.end(await readFile(join(here, 'node_modules/posthog-js/dist/array.no-external.js'))); }
    else if (req.url === '/') { res.setHeader('content-type', 'text/html'); res.end('<!doctype html><script src="/sdk.js"></script>'); }
    else if (req.url === '/favicon.ico') res.writeHead(204).end();
    else res.writeHead(404).end();
  });
  origin = await listen(site);
  const reservation = createServer(); api = await listen(reservation); await close(reservation);
  app = spawn(binary, [], { cwd: directory, env: { ...process.env, PORCUPINE_ADDR: new URL(api).host, PORCUPINE_DB: join(directory, 'events.db'), PORCUPINE_BOOTSTRAP_TOKEN: bootstrap, PORCUPINE_ALLOWED_ORIGINS: origin, PORCUPINE_OAUTH_ISSUER: '', PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS: '', PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS: '', PORCUPINE_CLICKHOUSE_URL: clickhouse, PORCUPINE_CLICKHOUSE_DATABASE: 'sdk_identify_' + suffix, PORCUPINE_CLICKHOUSE_USER: environment.CLICKHOUSE_USER, PORCUPINE_CLICKHOUSE_PASSWORD: password }, stdio: 'ignore' });
  phase = 'Go readiness';
  for (let attempt = 0; ; attempt++) {
    try { if ((await fetch(api + '/readyz')).ok) break; } catch {}
    if (attempt === 149) throw new Error('readiness');
    await delay(100);
  }
  project = await request('/api/v1/projects', { name: 'Real SDK identity demonstration' });
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ serviceWorkers: 'block' });
  context.on('requestfailed', req => failures.push(new URL(req.url()).pathname));
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (![api, origin].includes(url.origin)) { external.push('external request'); await route.abort(); return; }
    if (url.origin === api && req.method() === 'POST') transport.push(JSON.parse(req.postData()));
    await route.continue();
  });
  const page = await context.newPage(); await page.goto(origin);
  await page.evaluate(({ host, token, profile }) => posthog.init(token, { ...profile, api_host: host }), { host: api, token: project.ingest_token, profile: identifiedBrowserProfile });
  const send = async (name, action) => {
    const response = page.waitForResponse(response => new URL(response.url()).origin === api && response.request().method() === 'POST' && JSON.parse(response.request().postData()).batch.some(item => item.event === name));
    await page.evaluate(action);
    const completed = await response;
    assert.equal(completed.status(), 200);
    assert.equal(await completed.headerValue('access-control-allow-origin'), origin);
    assert.equal(await completed.finished(), null);
    const body = await completed.json();
    assert.equal(body.identity_ignored, 0);
  };
  phase = 'browser identify';
  const ids = { a: await page.evaluate(() => posthog.get_distinct_id()) };
  await send('sdk before A', () => posthog.capture('sdk before A', {}, { send_instantly: true }));
  await send('$identify', () => posthog.identify('sdk-account-a', { plan: 'starter' }, { first_source: 'first' }));
  await send('sdk after A', () => posthog.capture('sdk after A', {}, { send_instantly: true }));
  await send('$set', () => posthog.identify('sdk-account-a', { plan: 'team' }, { first_source: 'ignored' }));
  ids.b = await page.evaluate(() => { posthog.reset(); return posthog.get_distinct_id(); });
  await send('sdk before B', () => posthog.capture('sdk before B', {}, { send_instantly: true }));
  await send('$identify', () => posthog.identify('sdk-account-b', { plan: 'second' }));
  await send('sdk after B', () => posthog.capture('sdk after B', {}, { send_instantly: true }));
  // A fresh context provides a genuinely anonymous cookie for mixed batching.
  const batched = await browser.newContext({ serviceWorkers: 'block' });
  batched.on('requestfailed', req => failures.push(new URL(req.url()).pathname));
  await batched.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (![api, origin].includes(url.origin)) { external.push('external request'); await route.abort(); return; }
    if (url.origin === api && req.method() === 'POST') transport.push(JSON.parse(req.postData()));
    await route.continue();
  });
  const batchPage = await batched.newPage(); await batchPage.goto(origin);
  const batchAnonymous = await batchPage.evaluate(({ host, token, profile }) => {
    posthog.init(token, { ...profile, api_host: host, request_batching: true, request_queue_config: { flush_interval_ms: 250 } });
    const anonymous = posthog.get_distinct_id();
    posthog.capture('sdk batch before');
    posthog.identify('sdk-batch-account', { plan: 'initial' });
    posthog.identify('sdk-batch-account', { plan: 'final' });
    posthog.capture('sdk batch after');
    return anonymous;
  }, { host: api, token: project.ingest_token, profile: identifiedBrowserProfile });
  phase = 'Node target-only identify';
  const node = new PostHog(project.ingest_token, { ...nodeProfile, host: api, fetch: async (url, options) => { assert.equal(new URL(url).origin, api); return fetch(url, options); } });
  node.identify({ distinctId: 'sdk-backend-account', properties: { plan: 'backend' } });
  node.capture({ distinctId: 'sdk-backend-account', event: 'sdk backend after' });
  await node.flush(); await node.shutdown();
  phase = 'SQL identity projection';
  const sql = "SELECT event,distinct_id,person_id,JSONExtractString(person_properties,'plan') AS plan,JSONExtractString(person_properties,'first_source') AS first_source FROM events ORDER BY event LIMIT 100";
  let result;
  for (let attempt = 0; ; attempt++) {
    result = await request('/api/v1/projects/' + project.project.id + '/query', { sql });
    report.last_query = { rows: result.result.data, delivery: result.delivery };
    if (result.result.data.length === 7 && result.delivery.pending === 0 && result.result.data.every(row => row.plan)) break;
    if (attempt === 149) throw new Error('projection deadline');
    await delay(100);
  }
  const rows = Object.fromEntries(result.result.data.map(row => [row.event, row]));
  for (const name of ['sdk before A', 'sdk after A']) { assert.equal(rows[name].person_id, 'sdk-account-a'); assert.equal(rows[name].plan, 'team'); assert.equal(rows[name].first_source, 'first'); }
  for (const name of ['sdk before B', 'sdk after B']) { assert.equal(rows[name].person_id, 'sdk-account-b'); assert.equal(rows[name].plan, 'second'); }
  for (const name of ['sdk batch before', 'sdk batch after']) { assert.equal(rows[name].person_id, 'sdk-batch-account'); assert.equal(rows[name].plan, 'final'); }
  assert.equal(rows['sdk before A'].distinct_id, ids.a); assert.equal(rows['sdk before B'].distinct_id, ids.b); assert.equal(rows['sdk batch before'].distinct_id, batchAnonymous);
  assert.equal(rows['sdk backend after'].person_id, 'sdk-backend-account'); assert.equal(rows['sdk backend after'].plan, 'backend');
  assert.ok(result.result.data.every(row => !row.event.startsWith('$')));
  assert.ok(transport.some(payload => payload.batch.map(event => event.event).join(',') === 'sdk batch before,$set,sdk batch after'));
  assert.deepEqual(external, []); assert.deepEqual(failures, []);
  report.passed = true; report.sql = sql; report.rows = result.result.data; report.delivery = result.delivery;
  delete report.last_query;
  report.versions = { 'posthog-js': JSON.parse(await readFile(join(here, 'node_modules/posthog-js/package.json'))).version, 'posthog-node': JSON.parse(await readFile(join(here, 'node_modules/posthog-node/package.json'))).version, chromium: browser.version() };
  report.observed_browser_requests = transport.length;
  report.external_requests = external.length;
} catch (error) {
  report.failed_check = phase;
  if (error.code === 'ERR_ASSERTION' && ['string', 'number', 'boolean'].includes(typeof error.actual)) report.failed_assertion = { actual: error.actual, expected: error.expected };
  if (directory) {
    try { report.identity_state = JSON.parse(execFileSync('python3', [join(here, 'sqlite-identities.py'), join(directory, 'events.db'), project.project.id], { encoding: 'utf8' })); } catch {}
  }
} finally {
  await browser?.close(); await close(site);
  if (app && app.exitCode === null) { app.kill('SIGTERM'); await once(app, 'exit'); }
  if (created) docker(['rm', '-fv', container]);
  if (directory) await rm(directory, { recursive: true, force: true });
  await mkdir(join(here, 'artifacts'), { recursive: true });
  await writeFile(join(here, 'artifacts/identify-clickhouse.json'), JSON.stringify(report, null, 2) + '\n');
}
console.log(JSON.stringify(report, null, 2));
process.exitCode = report.passed ? 0 : 1;
