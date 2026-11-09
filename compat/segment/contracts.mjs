// Real pinned Segment SDK over HTTP, against a temporary Go/SQLite service.
import assert from 'node:assert/strict';
import { Analytics } from '@segment/analytics-node';
import { createServer } from 'node:http';
import { spawn, execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { once } from 'node:events';

const here = dirname(fileURLToPath(import.meta.url)), root = resolve(here, '../..');
const token = randomBytes(32).toString('hex'), blocked = [], requests = [];
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return 'http://127.0.0.1:' + server.address().port; };
const close = server => server ? new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }) : Promise.resolve();
let directory, app, proxy, api, proxyURL, sdk, phase = 'setup';
const report = { passed: false, sdk: '@segment/analytics-node', version: '3.1.0', production_connected: false };
const originalFetch = globalThis.fetch;
globalThis.fetch = (url, options) => {
  if (![api, proxyURL].includes(new URL(url).origin)) { blocked.push('external request'); throw new Error('request outside local test origins'); }
  return originalFetch(url, options);
};
async function management(path, body) {
  const response = await fetch(api + path, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(5000) });
  assert.equal(response.status, body === undefined ? 200 : 201);
  return response.json();
}
try {
  directory = await mkdtemp(join(tmpdir(), 'porcupine-segment-sdk-'));
  const binary = join(directory, 'porcupine');
  execFileSync('go', ['build', '-o', binary, './cmd/porcupine'], { cwd: root, env: { ...process.env, CGO_ENABLED: '0' }, stdio: ['ignore', 'pipe', 'pipe'] });
  const reservation = createServer(); api = await listen(reservation); await close(reservation);
  app = spawn(binary, [], { cwd: directory, env: { ...process.env, PORCUPINE_ADDR: new URL(api).host, PORCUPINE_DB: join(directory, 'events.db'), PORCUPINE_BOOTSTRAP_TOKEN: token, PORCUPINE_OAUTH_ISSUER: '', PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS: '', PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS: '', PORCUPINE_CLICKHOUSE_URL: '' }, stdio: 'ignore' });
  phase = 'Go readiness';
  for (let attempt = 0; ; attempt++) { try { if ((await fetch(api + '/readyz')).ok) break; } catch {} if (attempt === 99) throw new Error('readiness'); await delay(50); }
  const project = await management('/api/v1/projects', { name: 'Segment SDK contract' });
  const key = await management('/api/v1/access-keys', { name: 'Segment source', permissions: ['events:write'], projects: [project.project.id], source_id: 'segment-sdk' });
  proxy = createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    requests.push({ path: req.url, headers: req.headers, payload: JSON.parse(body.toString()) });
    const upstream = await fetch(api + req.url, { method: 'POST', headers: { 'content-type': req.headers['content-type'], authorization: req.headers.authorization }, body });
    res.writeHead(upstream.status, { 'content-type': 'application/json' }).end(await upstream.text());
  });
  proxyURL = await listen(proxy);
  phase = 'official SDK HTTP transport';
  sdk = new Analytics({ writeKey: key.token, host: proxyURL, flushAt: 50, flushInterval: 60000, maxRetries: 0, httpRequestTimeout: 3000 });
  const timestamp = new Date('2025-03-04T05:06:07.123Z');
  sdk.identify({ messageId: 'segment-sdk-anon', anonymousId: 'anonymous-device', traits: { campaign: 'docs' }, timestamp });
  sdk.track({ messageId: 'segment-sdk-before', anonymousId: 'anonymous-device', event: 'Before Login', properties: { token: 'business-token', nested: { unicode: '東京 🦔', values: [1, true, null] } }, context: { locale: 'fr-FR' }, timestamp });
  sdk.identify({ messageId: 'segment-sdk-login', anonymousId: 'anonymous-device', userId: 'account-7', traits: { plan: 'pro' }, timestamp });
  sdk.page({ messageId: 'segment-sdk-page', userId: 'account-7', name: 'Pricing', category: 'Docs', properties: { url: 'https://example.test/pricing', title: 'Pricing', name: 'business-name' }, timestamp });
  sdk.track({ messageId: 'segment-sdk-after', userId: 'account-7', event: 'After Login', properties: { revenue: 42.25 }, integrations: { Vendor: { apiKey: 'synthetic-vendor-secret' } }, timestamp });
  await sdk.flush({ timeout: 10000 });
  assert.equal(requests.length, 1);
  const wire = requests[0];
  assert.equal(wire.path, '/v1/batch');
  assert.equal(Buffer.from(wire.headers.authorization.slice(6), 'base64').toString(), key.token + ':');
  assert.equal(wire.payload.writeKey, key.token);
  assert.deepEqual(wire.payload.batch.map(event => event.type), ['identify', 'track', 'identify', 'page', 'track']);
  const response = await fetch(api + wire.path, { method: 'POST', headers: { 'content-type': 'application/json', authorization: wire.headers.authorization }, body: JSON.stringify(wire.payload) });
  assert.equal(response.status, 200);
  const replay = await response.json();
  assert.equal(replay.accepted, 0); assert.equal(replay.duplicates, 5);
  phase = 'durable canonical events and identity';
  const events = (await management('/api/v1/projects/' + project.project.id + '/events')).events;
  assert.equal(events.length, 3, 'controls stay hidden from the ordinary event API');
  const before = events.find(event => event.message_id === 'segment-sdk-before');
  const page = events.find(event => event.message_id === 'segment-sdk-page');
  assert.equal(before.distinct_id, 'anonymous-device'); assert.equal(before.event, 'Before Login');
  assert.equal(before.properties.token, 'business-token');
  assert.deepEqual(before.properties.nested, { unicode: '東京 🦔', values: [1, true, null] });
  assert.equal(before.context.locale, 'fr-FR');
  assert.equal(page.event, '$pageview'); assert.equal(page.properties.name, 'business-name');
  assert.equal(page.context.page.name, 'Pricing'); assert.equal(page.context.page.category, 'Docs');
  assert.equal(page.context.page.url, 'https://example.test/pricing');
  for (const event of events) { assert.equal(event.source_id, 'segment-sdk'); assert.equal(event.timestamp, timestamp.toISOString()); assert.equal(JSON.stringify(event).includes('synthetic-vendor-secret'), false); assert.equal(JSON.stringify(event).includes(key.token), false); }
  const snapshot = JSON.parse(execFileSync('python3', [join(root, 'compat/sqlite-identities.py'), join(directory, 'events.db'), project.project.id], { encoding: 'utf8' }));
  const person = snapshot.identity_profiles.find(row => row.identified_id === 'account-7');
  assert.deepEqual(person.traits, { campaign: 'docs', plan: 'pro' });
  assert.equal(snapshot.identity_links.find(row => row.anonymous_id === 'anonymous-device').identified_id, 'account-7');
  assert.deepEqual(blocked, []);
  report.passed = true; report.accepted_messages = 5; report.visible_events = events.length; report.retry_duplicates = replay.duplicates; report.external_requests = blocked.length;
  const sanitized = JSON.parse(JSON.stringify(wire.payload)); sanitized.writeKey = 'FIXTURE_WRITE_KEY';
  report.observed_payload = sanitized;
} catch {
  report.failed_check = phase;
} finally {
  if (sdk) await sdk.closeAndFlush({ timeout: 3000 }).catch(() => {});
  await close(proxy);
  if (app && app.exitCode === null) { app.kill('SIGTERM'); await once(app, 'exit'); }
  if (directory) await rm(directory, { recursive: true, force: true });
  globalThis.fetch = originalFetch;
  await mkdir(join(here, '../artifacts'), { recursive: true });
  await writeFile(join(here, '../artifacts/segment-sdk.json'), JSON.stringify(report, null, 2) + '\n');
}
console.log(JSON.stringify({ passed: report.passed, sdk: report.sdk, version: report.version, accepted_messages: report.accepted_messages, visible_events: report.visible_events, retry_duplicates: report.retry_duplicates, failed_check: report.failed_check }, null, 2));
process.exitCode = report.passed ? 0 : 1;
