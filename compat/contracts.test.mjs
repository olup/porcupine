import { serverEnv } from './server-env.mjs';
import { identityProbe } from './identity-probe.mjs';
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
import { browserBeaconProfile, browserProfile, cookielessProfile, identifiedBrowserProfile, nodeProfile } from './profiles.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '..');
const matrix = JSON.parse(await readFile(join(here, 'matrix.json'), 'utf8'));
const installed = async name => JSON.parse(await readFile(join(here, 'node_modules', name, 'package.json'), 'utf8')).version;
const versions = { 'posthog-js': await installed('posthog-js'), 'posthog-node': await installed('posthog-node'), playwright: await installed('playwright') };
const report = { schema_version: 1, mode: process.env.PORCUPINE_COMPAT_CANARY ? 'canary' : 'pinned', versions, profile: 'cookieless-default-and-opt-in-identified', configuration: { browser_cookieless: cookielessProfile, browser_identified: identifiedBrowserProfile, browser_local_id_diagnostic: browserProfile, browser_beacon_diagnostic: browserBeaconProfile, node: nodeProfile, browser_batch_diagnostic: { ...browserProfile, request_batching: true, disable_compression: false, request_queue_config: { flush_interval_ms: 250 } } }, results: [], unsupported: matrix.unsupported };
let probe, directory, child, api, origin, browser, context, page, site, project, otherProject, proxy, proxyURL;
const admin = randomBytes(32).toString('hex');
const blocked = [], unexpected = [], wires = [], browserWires = [], browserFailures = [];
const identifiedProjects = [];
let failAcknowledgement = false;
let failBrowserAcknowledgement = false;
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; };
const close = server => server ? new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }) : Promise.resolve();
const decodeWire = wire => {
  if (wire.query?.compression === 'base64') {
    const form = new URLSearchParams(wire.body.toString());
    return JSON.parse(Buffer.from(form.get('data'), 'base64').toString());
  }
  return JSON.parse((wire.headers['content-encoding'] === 'gzip' || wire.body[0] === 0x1f && wire.body[1] === 0x8b ? gunzipSync(wire.body) : wire.body).toString());
};
const safeFetch = async (url, options) => {
  if (new URL(url).origin !== proxyURL) { blocked.push('node external request'); throw new Error('SDK request outside local capture origin'); }
  return fetch(url, options);
};
async function management(path, { method = 'GET', body, token = admin } = {}) {
  const response = await fetch(api + path, { method, headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  assert.equal(response.status, method === 'POST' && !path.endsWith('/query') ? 201 : 200, 'management HTTP status');
  return response.json();
}
const events = async (id = project.project.id) => {
  const result = await management(`/api/v1/projects/${id}/query`, { method: 'POST', body: { sql: 'SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,source_id,message_id,event_type,context FROM events ORDER BY id LIMIT 1000' } });
  return result.result.data.map(row => ({ ...row, id: Number(row.id), properties: JSON.parse(row.properties), context: JSON.parse(row.context) }));
};
async function event(name, count = 1, projectID = project.project.id) {
  for (let attempt = 0; attempt < 100; attempt++) {
    const found = (await events(projectID)).filter(e => e.event === name);
    if (found.length >= count) { assert.equal(found.length, count, 'event count'); return found; }
    await delay(50);
  }
  throw new Error(`persisted event missing: ${name}`);
}
function assertTimestamp(saved, payload, sent) {
  assert.equal(typeof payload.sent_at, 'string', 'SDK sends sent_at at the batch root');
  assert.equal(sent.sent_at, undefined);
  assert.equal(saved.context.client_timestamp, sent.timestamp);
  assert.equal(saved.context.sent_at, payload.sent_at);
  const clientDelta = Date.parse(sent.timestamp) - Date.parse(payload.sent_at);
  const storedDelta = Date.parse(saved.timestamp) - Date.parse(saved.received_at);
  assert.equal(storedDelta, clientDelta, 'stored timestamp keeps the gap between capture and send');
}
const identities = id => {
  const target = [project, otherProject, ...identifiedProjects].find(item => item?.project.id === id);
  const controls = new Map();
  for (const wire of [...wires, ...browserWires]) {
    const payload = decodeWire(wire);
    for (const item of payload.batch || [payload]) {
      if ((payload.api_key || item.api_key || item.properties?.token) === target.ingest_token && ['$identify', '$set'].includes(item.event)) controls.set(item.uuid, { uuid: item.uuid, event: item.event });
    }
  }
  return probe(id, [...controls.values()]);
};
async function sdkSend(tab, name, operation, argument) {
  const pending = tab.waitForResponse(response => new URL(response.url()).origin === api && response.request().method() === 'POST' && decodeWire({ headers: response.request().headers(), body: response.request().postDataBuffer() }).batch.some(item => item.event === name));
  const value = await tab.evaluate(operation, argument);
  const response = await pending;
  assert.equal(response.status(), 200);
  assert.equal(await response.headerValue('access-control-allow-origin'), '*');
  assert.equal(await response.finished(), null);
  assert.equal(response.request().failure(), null);
  const payload = decodeWire({ headers: response.request().headers(), body: response.request().postDataBuffer() });
  assert.deepEqual(await response.json(), { status: 1, durability: 'durable', accepted: payload.batch.length, duplicates: 0, identity_ignored: 0 });
  return { value, payload, sent: payload.batch.find(item => item.event === name) };
}
const sdkCapture = (tab, name) => sdkSend(tab, name, name => ({ distinct: posthog.get_distinct_id(), uuid: posthog.capture(name, { synthetic: true }, { send_instantly: true }).uuid }), name);
async function routeBrowser(route) {
  const request = route.request();
  const url = new URL(request.url());
  if (![api, origin].includes(url.origin)) { blocked.push('browser external request'); await route.abort(); return; }
  if (url.origin === api && !['/i/v0/e/', '/e/', '/capture/'].includes(url.pathname)) { unexpected.push(`browser ${url.pathname}`); await route.abort(); return; }
  if (url.origin === api && request.method() === 'POST') browserWires.push({ path: url.pathname, headers: request.headers(), body: request.postDataBuffer(), query: Object.fromEntries(url.searchParams) });
  await route.continue();
}
async function identifiedBrowser(run, batched = false) {
  const scoped = await management('/api/v1/projects', { method: 'POST', body: { name: 'Identified SDK contract' } });
  identifiedProjects.push(scoped);
  const isolated = await browser.newContext({ serviceWorkers: 'block' });
  const observed = [];
  isolated.on('requestfailed', request => browserFailures.push(new URL(request.url()).pathname));
  await isolated.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    if (![api, origin].includes(url.origin)) { blocked.push('identified external request'); await route.abort(); return; }
    if (url.origin === api && !['/i/v0/e/', '/e/', '/capture/'].includes(url.pathname)) { unexpected.push(`identified ${url.pathname}`); await route.abort(); return; }
    if (url.origin === api && request.method() === 'POST') {
      const wire = { path: url.pathname, headers: request.headers(), body: request.postDataBuffer(), query: {} };
      observed.push(wire); browserWires.push(wire);
    }
    await route.continue();
  });
  const tab = await isolated.newPage();
  const profile = batched ? { ...identifiedBrowserProfile, request_batching: true, request_queue_config: { flush_interval_ms: 250 } } : identifiedBrowserProfile;
  const init = async () => {
    await tab.goto(origin);
    await tab.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token: scoped.ingest_token, profile });
  };
  try { await init(); await run({ tab, isolated, scoped, observed, init }); }
  finally { await isolated.close(); }
}
function contract(id, run) {
  assert.ok(matrix.cases.some(item => item.id === id), `test ${id} is declared in matrix`);
  test(id, { timeout: 20000 }, async t => {
    try { await run(t); report.results.push({ id, status: 'passed' }); }
    catch (error) { report.results.push({ id, status: 'failed' }); throw error; }
  });
}
before(async () => {
  assert.ok(matrix.cases.some(item => item.required), 'matrix has required contracts');
  const goTests = execFileSync('go', ['test', '-list', '.', './internal/porcupine'], { cwd: root, encoding: 'utf8', env: { ...process.env, CGO_ENABLED: '1' } }).split(/\r?\n/);
  for (const item of matrix.go_contracts) assert.ok(goTests.includes(item.test), `Go contract exists: ${item.test}`);
  for (const fixture of matrix.fixtures) {
    const recorded = JSON.parse(await readFile(join(here, fixture), 'utf8'));
    assert.ok(recorded.provenance.sdk && recorded.provenance.version, 'fixture has SDK provenance');
    assert.ok(recorded.payload, 'fixture has a payload');
  }
  directory = await mkdtemp(join(tmpdir(), 'porcupine-compat-'));
  probe = identityProbe(root, directory);
  const binary = join(directory, 'porcupine');
  execFileSync('go', ['build', '-o', binary, './cmd/porcupine'], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, CGO_ENABLED: '1' } });
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
  child = spawn(binary, [], { cwd: directory, env: serverEnv({ PORCUPINE_ADDR: new URL(api).host, PORCUPINE_DB: join(directory, 'events.db'), PORCUPINE_BOOTSTRAP_TOKEN: admin }), stdio: 'ignore' });
  for (let attempt = 0; attempt < 200; attempt++) {
    try { if ((await fetch(api + '/readyz')).ok) break; } catch {}
    if (attempt === 199) throw new Error('Go server did not become ready');
    await delay(50);
  }
  project = await management('/api/v1/projects', { method: 'POST', body: { name: 'SDK contracts' } });
  otherProject = await management('/api/v1/projects', { method: 'POST', body: { name: 'Other project' } });
  proxy = createServer(async (request, response) => {
    const url = new URL(request.url, api);
    const fromBrowser = request.headers.origin === origin;
    if (request.method === 'GET' && url.pathname === '/') {
      response.writeHead(200, { 'content-type': 'text/html' }).end('<!doctype html><title>SDK contract destination</title>');
      return;
    }
    if (request.method === 'GET' && url.pathname === '/favicon.ico') { response.writeHead(204).end(); return; }
    const validPath = fromBrowser ? ['/i/v0/e/', '/e/', '/capture/', '/batch/'].includes(url.pathname) : url.pathname === '/batch/';
    if (request.method !== 'POST' || !validPath) { unexpected.push('proxy route'); response.writeHead(404).end(); return; }
    try {
      const chunks = []; for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks);
      const wire = { path: url.pathname, headers: request.headers, body, query: Object.fromEntries(url.searchParams) };
      (fromBrowser ? browserWires : wires).push(wire);
      const headers = { 'content-type': request.headers['content-type'] || 'application/json' };
      if (request.headers['content-encoding']) headers['content-encoding'] = request.headers['content-encoding'];
      if (fromBrowser) headers.origin = request.headers.origin;
      const upstream = await fetch(api + request.url, { method: request.method, headers, body });
      const text = await upstream.text();
      wire.status = upstream.status;
      const responseHeaders = { 'content-type': 'application/json' };
      for (const name of ['access-control-allow-origin', 'access-control-expose-headers']) {
        if (upstream.headers.has(name)) responseHeaders[name] = upstream.headers.get(name);
      }
      if (upstream.ok && (fromBrowser ? failBrowserAcknowledgement : failAcknowledgement)) {
        if (fromBrowser) {
          failBrowserAcknowledgement = false;
          responseHeaders['retry-after'] = '2';
        } else failAcknowledgement = false;
        response.writeHead(503, responseHeaders).end('{"error":"injected lost acknowledgement"}');
        return;
      }
      response.writeHead(upstream.status, responseHeaders).end(text);
    } catch {
      unexpected.push('proxy forwarding failure');
      response.writeHead(502).end();
    }
  });
  proxyURL = await listen(proxy);
  browser = await chromium.launch({ headless: true });
  report.browser = { name: "chromium", version: browser.version() };
  context = await browser.newContext({ serviceWorkers: 'block' });
  await context.route('**/*', routeBrowser);
  page = await context.newPage();
  page.on('requestfailed', request => browserFailures.push(new URL(request.url()).pathname));
  await page.goto(origin);
  await page.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token: project.ingest_token, profile: browserProfile });
}, { timeout: 240000 }); // Includes compiling both the Go tests and the native binary in a fresh checkout.

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
    const payload = decodeWire(wires[0]);
    assert.equal(payload.batch[0].timestamp, '2025-02-03T04:05:06.789Z');
    assertTimestamp(saved, payload, payload.batch[0]);
    assert.deepEqual(saved.properties.nested, { unicode: '東京 🦔', list: [1, true, null] });
    assert.equal(saved.properties.number, 42);
    assert.equal(saved.properties.$lib, 'posthog-node');
    assert.equal(saved.properties.$lib_version, versions['posthog-node']);
    assert.ok(saved.uuid);
  } finally { await client.shutdown(); }
});
contract('node.clock-skew', async t => {
  const name = 'node clock skew';
  const start = wires.length;
  const clientTime = new Date(Date.now() + 2 * 60 * 60 * 1000);
  const client = new PostHog(project.ingest_token, { ...nodeProfile, host: proxyURL, fetch: safeFetch });
  t.mock.timers.enable({ apis: ['Date'], now: clientTime });
  try {
    client.capture({ distinctId: 'clock-user', event: name });
    await client.flush();
  } finally {
    t.mock.timers.reset();
    await client.shutdown();
  }
  const wire = wires[start];
  assert.equal(wire.query.sent_at, undefined);
  assert.equal(wire.query._, undefined);
  const payload = decodeWire(wire);
  const sent = payload.batch.find(item => item.event === name);
  const [saved] = await event(name);
  assert.equal(sent.timestamp, clientTime.toISOString());
  assert.equal(payload.sent_at, clientTime.toISOString());
  assertTimestamp(saved, payload, sent);
  assert.ok(Math.abs(Date.parse(saved.timestamp) - Date.parse(saved.received_at)) < 3000);
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
  assert.equal(await response.headerValue('access-control-allow-origin'), '*');
  assert.equal(await response.finished(), null, 'Chromium completes SDK fetch response');
  assert.equal(response.request().failure(), null);
  const [saved] = await event('browser custom');
  assert.equal(new URL(api).origin === origin, false, 'capture uses a different browser origin');
  assert.equal(saved.uuid, result.uuid);
  assert.equal(saved.distinct_id, result.distinct);
  const request = response.request();
  const payload = decodeWire({ headers: request.headers(), body: request.postDataBuffer() });
  const sent = payload.batch.find(item => item.uuid === result.uuid);
  assert.equal(sent.timestamp, '2025-03-04T05:06:07.123Z');
  assertTimestamp(saved, payload, sent);
  assert.equal(saved.properties.$session_id, result.session);
  assert.deepEqual(saved.properties.nested, { unicode: 'café 東京 🦔', list: [false, 3, null] });
  assert.equal(saved.properties.number, 7);
  assert.equal(saved.properties.$lib, 'web');
  assert.equal(saved.properties.$lib_version, versions['posthog-js']);
});
contract('browser.batch-compression', async () => {
  const start = browserWires.length;
  await page.evaluate(({ host, token, profile }) => {
    const batched = window.posthog.init(token, { ...profile, api_host: host, request_batching: true, disable_compression: false, request_queue_config: { flush_interval_ms: 250 } }, 'batched');
    batched.capture('browser batch a', { order: 1 });
    batched.capture('browser batch b', { order: 2 });
  }, { host: api, token: project.ingest_token, profile: browserProfile });
  await event('browser batch a'); await event('browser batch b');
  assert.ok(browserWires.slice(start).map(decodeWire).some(payload => payload.batch.some(item => item.event === 'browser batch a') && payload.batch.some(item => item.event === 'browser batch b')), 'both events share a real SDK batch');
  assert.ok(browserWires.slice(start).some(wire => wire.query.compression === 'gzip-js' || wire.headers['content-encoding'] === 'gzip' || wire.body[0] === 0x1f && wire.body[1] === 0x8b), 'real browser SDK used gzip transport');
});
contract('browser.pageleave-beacon', async () => {
  const isolated = await browser.newContext({ serviceWorkers: 'block' });
  await isolated.route('**/*', async route => {
    if (![origin, proxyURL].includes(new URL(route.request().url()).origin)) {
      blocked.push('beacon external request');
      await route.abort();
      return;
    }
    await route.continue();
  });
  isolated.on('requestfailed', request => browserFailures.push(new URL(request.url()).pathname));
  const pageleaveUUIDs = [];
  const firstWire = browserWires.length;
  try {
    for (const phase of ['navigation', 'close']) {
      const tab = await isolated.newPage();
      await tab.goto(origin);
      if (phase === 'navigation') {
        // The virtual clock covers this context. Pause the periodic batch timer for both pages.
        const now = new Date();
        await tab.clock.install({ time: now });
        await tab.clock.pauseAt(new Date(now.getTime() + 1000));
      }
      const names = ['a', 'b'].map(suffix => `beacon ${phase} ${suffix}`);
      const unicode = 'café 東京 🦔';
      const start = browserWires.length;
      const queued = await tab.evaluate(({ host, token, profile, names, phase, unicode }) => {
        window.posthog.init(token, { ...profile, api_host: host });
        return names.map(name => window.posthog.capture(name, { phase, unicode }).uuid);
      }, { host: proxyURL, token: project.ingest_token, profile: browserBeaconProfile, names, phase, unicode });
      assert.equal(browserWires.length, start, 'ordinary captures are queued until pagehide');
      if (phase === 'navigation') {
        await tab.goto(proxyURL);
        assert.notEqual(new URL(tab.url()).origin, origin, 'navigation leaves the SDK page origin');
        await tab.close();
      } else {
        const closed = tab.waitForEvent('close');
        await tab.close({ runBeforeUnload: true });
        await closed;
      }
      // Native unload beacons can bypass Playwright's request interception.
      for (let attempt = 0; attempt < 100 && browserWires[start]?.status === undefined; attempt++) await delay(50);
      const wires = browserWires.slice(start);
      assert.equal(wires.length, 1, `${phase} sends one SDK request`);
      const [wire] = wires;
      assert.equal(wire.status, 200, `${phase} beacon acknowledged by Porcupine`);
      assert.equal(wire.headers['sec-fetch-mode'], 'no-cors');
      assert.equal(wire.headers['content-type'], 'application/x-www-form-urlencoded');
      assert.equal(wire.query.compression, 'base64');
      assert.match(wire.body.toString(), /^data=[^&]+$/);
      const { batch } = decodeWire(wire);
      assert.deepEqual(batch.map(item => item.event), [...names, '$pageleave']);
      const pageleave = batch.at(-1);
      pageleaveUUIDs.push(pageleave.uuid);
      for (const [index, name] of names.entries()) {
        assert.equal(batch[index].uuid, queued[index]);
        const [saved] = await event(name);
        assert.equal(saved.uuid, queued[index]);
        assert.equal(saved.properties.unicode, unicode);
      }
      const saved = await event('$pageleave', pageleaveUUIDs.length);
      assert.deepEqual(saved.map(item => item.uuid), pageleaveUUIDs);
      assert.equal(saved.at(-1).properties.$current_url, origin + '/');
    }
    assert.equal(browserWires.length - firstWire, 2, 'one beacon per page lifecycle');
  } finally { await isolated.close(); }
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
async function initClockBrowser(tab) {
  await tab.evaluate(({ host, token, profile }) => window.posthog.init(token, { ...profile, api_host: host }), { host: api, token: project.ingest_token, profile: cookielessProfile });
}
contract('browser.clock-skew', async () => {
  for (const hours of [2, -3]) {
    const tab = await context.newPage();
    try {
      await tab.goto(origin);
      const clientTime = new Date(Date.now() + hours * 60 * 60 * 1000);
      await tab.clock.setFixedTime(clientTime);
      await initClockBrowser(tab);
      const name = `browser clock skew ${hours}`;
      const start = browserWires.length;
      const { sent, payload } = await sdkCapture(tab, name);
      assert.equal(browserWires[start].query.sent_at, undefined);
      assert.equal(browserWires[start].query._, undefined);
      const [saved] = await event(name);
      assert.equal(sent.timestamp, clientTime.toISOString());
      assert.equal(payload.sent_at, clientTime.toISOString());
      assert.match(saved.distinct_id, /^cookieless_/);
      assertTimestamp(saved, payload, sent);
      assert.ok(Math.abs(Date.parse(saved.timestamp) - Date.parse(saved.received_at)) < 3000);
    } finally { await tab.close(); }
  }
});
contract('browser.offline-queue', async () => {
  const name = 'browser offline queue';
  const interval = 10 * 60 * 1000;
  const isolated = await browser.newContext({ serviceWorkers: 'block' });
  await isolated.route('**/*', routeBrowser);
  const tab = await isolated.newPage();
  try {
    await tab.clock.install({ time: new Date() });
    await tab.goto(origin);
    await initClockBrowser(tab);
    await isolated.setOffline(true);
    assert.equal(await tab.evaluate(() => navigator.onLine), false);
    const isCapture = request => new URL(request.url()).origin === api && request.method() === 'POST';
    const failed = tab.waitForEvent('requestfailed', { predicate: isCapture });
    const captured = await tab.evaluate(name => posthog.capture(name).timestamp.toISOString(), name);
    assert.equal((await failed).failure().errorText, 'net::ERR_INTERNET_DISCONNECTED');
    await tab.clock.fastForward(interval);
    const delivered = tab.waitForResponse(response => isCapture(response.request()));
    await isolated.setOffline(false);
    const response = await delivered;
    assert.equal(response.status(), 200);
    assert.equal(new URL(response.url()).searchParams.get('retry_count'), '1');
    const request = response.request();
    const payload = decodeWire({ headers: request.headers(), body: request.postDataBuffer() });
    const sent = payload.batch.find(item => item.event === name);
    const [saved] = await event(name);
    assert.equal(sent.timestamp, captured);
    assert.ok(Math.abs(Date.parse(payload.sent_at) - Date.parse(captured) - interval) < 3000);
    assertTimestamp(saved, payload, sent);
    // Only the browser clock advances. Preserve the queue age on the server's clock.
    assert.ok(Math.abs(Date.parse(saved.received_at) - Date.parse(saved.timestamp) - interval) < 3000);
  } finally { await isolated.close(); }
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
    assert.equal(await response.headerValue('access-control-allow-origin'), '*');
    assert.equal(await response.finished(), null, 'Chromium completes the CORS fetch');
    assert.equal(response.request().failure(), null);
    assert.deepEqual(await response.json(), { status: 1, durability: 'durable', accepted: 1, duplicates: 0, identity_ignored: 0 });
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
contract('browser.identify-before-after', async () => {
  await identifiedBrowser(async ({ tab, isolated, scoped, observed, init }) => {
    const id = scoped.project.id;
    const before = await sdkCapture(tab, 'identified before login');
    const login = await sdkSend(tab, '$identify', () => posthog.identify('browser-account-a', { plan: 'starter', display_name: 'Ada' }, { first_source: 'docs' }));
    assert.equal(login.sent.properties.distinct_id, 'browser-account-a');
    assert.equal(login.sent.properties.$anon_distinct_id, before.value.distinct);
    assert.equal(login.sent.properties.$is_identified, true);
    assert.equal(login.sent.properties.$process_person_profile, true);
    assert.deepEqual(login.sent.$set, { plan: 'starter', display_name: 'Ada' });
    assert.equal(login.sent.$set_once.first_source, 'docs');
    assert.equal(login.sent.properties.$set, undefined, 'initial browser traits are root members');
    const after = await sdkCapture(tab, 'identified after login');
    const [savedBefore] = await event('identified before login', 1, id);
    const [savedAfter] = await event('identified after login', 1, id);
    assert.equal(savedBefore.distinct_id, before.value.distinct, 'capture evidence keeps original anonymous ID');
    assert.equal(savedAfter.distinct_id, 'browser-account-a');
    assert.equal(savedBefore.uuid, before.value.uuid); assert.equal(savedAfter.uuid, after.value.uuid);
    const snapshot = identities(id);
    assert.ok(snapshot.control_events.some(row => row.uuid === login.sent.uuid));
    assert.ok((await events(id)).every(row => !['$identify', '$set'].includes(row.event)), 'public event list hides identity controls');
    const hidden = await fetch(api + `/api/v1/projects/${id}/events/${login.sent.uuid}`, { headers: { authorization: `Bearer ${admin}` } });
    assert.equal(hidden.status, 404, 'public event lookup hides identity controls');
    const link = snapshot.identity_links.find(row => row.anonymous_id === before.value.distinct);
    assert.equal(link.identified_id, 'browser-account-a');
    const person = snapshot.identity_profiles.find(row => row.identified_id === 'browser-account-a');
    assert.equal(person.traits.plan, 'starter'); assert.equal(person.traits.first_source, 'docs');
    const wireCount = observed.length;
    await tab.evaluate(() => { posthog.identify('browser-account-a'); posthog.identify('browser-account-a', { plan: 'starter', display_name: 'Ada' }, { first_source: 'docs' }); });
    await sdkCapture(tab, 'identified after repeated login');
    assert.equal(observed.length, wireCount + 1, 'same ID and identical traits emit no new control');
    assert.deepEqual(identities(id), snapshot, 'ordinary events and suppressed repeats do not change identity state');
    const state = await isolated.storageState();
    const cookie = state.cookies.find(item => item.name.startsWith('ph_') && item.name.endsWith('_posthog'));
    assert.ok(cookie, 'named identified profile stores identity in a cookie');
    assert.equal(JSON.parse(decodeURIComponent(cookie.value)).distinct_id, 'browser-account-a');
    assert.ok(state.origins.every(item => item.localStorage.length === 0), 'cookie profile does not store the persistent ID in localStorage');
    await init();
    const reloaded = await sdkCapture(tab, 'identified after reload');
    assert.equal(reloaded.value.distinct, 'browser-account-a');
    assert.equal(reloaded.sent.properties.$is_identified, true);
    assert.deepEqual(identities(id), snapshot);
  });
});

contract('browser.identify-traits-repeat', async () => {
  await identifiedBrowser(async ({ tab, scoped, observed }) => {
    const id = scoped.project.id;
    await sdkSend(tab, '$identify', () => posthog.identify('traits-account'));
    const initial = await sdkSend(tab, '$set', () => posthog.identify('traits-account', { plan: 'pro', nested: { locale: 'fr' } }, { first_source: 'first' }));
    assert.deepEqual(initial.sent.properties.$set, { plan: 'pro', nested: { locale: 'fr' } });
    assert.deepEqual(initial.sent.properties.$set_once, { first_source: 'first' });
    assert.equal(initial.sent.$set, undefined, 'subsequent traits live in properties on a standalone $set');
    const count = observed.length;
    await tab.evaluate(() => posthog.identify('traits-account', { plan: 'pro', nested: { locale: 'fr' } }, { first_source: 'first' }));
    await sdkCapture(tab, 'traits after repeated update');
    assert.equal(observed.length, count + 1, 'identical trait update is suppressed by the real SDK');
    const update = await sdkSend(tab, '$set', () => posthog.identify('traits-account', { plan: 'team' }, { first_source: 'later' }));
    const snapshot = identities(id);
    const person = snapshot.identity_profiles.find(row => row.identified_id === 'traits-account');
    assert.equal(person.traits.plan, 'team');
    assert.equal(person.traits.first_source, 'first', '$set_once keeps the original value');
    assert.deepEqual(person.traits.nested, { locale: 'fr' });
    const saved = snapshot.control_events.find(item => item.uuid === update.sent.uuid);
    assert.equal(saved.event, '$set');
    assert.equal(person.version, saved.id, 'the profile version points to its durable control receipt');
    const response = await fetch(api + '/e/', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(update.payload) });
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { status: 1, durability: 'durable', accepted: 0, duplicates: 1, identity_ignored: 0 });
    assert.deepEqual(identities(id), snapshot, 'stable-UUID retry does not apply traits again or advance version');
  });
});

contract('browser.identify-reset-switch', async () => {
  await identifiedBrowser(async ({ tab, scoped }) => {
    const id = scoped.project.id;
    const beforeA = await sdkCapture(tab, 'switch before A');
    await sdkSend(tab, '$identify', () => posthog.identify('switch-account-a', { account: 'A' }));
    await sdkCapture(tab, 'switch after A');
    const reset = await tab.evaluate(() => { posthog.reset(); return { distinct: posthog.get_distinct_id(), device: posthog.get_property('$device_id') }; });
    assert.notEqual(reset.distinct, beforeA.value.distinct);
    assert.equal(reset.device, beforeA.sent.properties.$device_id, 'reset keeps device ID while changing anonymous ID');
    const beforeB = await sdkCapture(tab, 'switch before B');
    assert.equal(beforeB.value.distinct, reset.distinct);
    const loginB = await sdkSend(tab, '$identify', () => posthog.identify('switch-account-b', { account: 'B' }));
    assert.equal(loginB.sent.properties.$anon_distinct_id, reset.distinct);
    await sdkCapture(tab, 'switch after B');
    const snapshot = identities(id);
    assert.equal(snapshot.identity_links.length, 2);
    assert.equal(snapshot.identity_links.find(row => row.anonymous_id === beforeA.value.distinct).identified_id, 'switch-account-a');
    assert.equal(snapshot.identity_links.find(row => row.anonymous_id === reset.distinct).identified_id, 'switch-account-b');
    assert.equal(snapshot.identity_profiles.find(row => row.identified_id === 'switch-account-a').traits.account, 'A');
    assert.equal(snapshot.identity_profiles.find(row => row.identified_id === 'switch-account-b').traits.account, 'B');
    for (const [name, expected] of [['switch before A', beforeA.value.distinct], ['switch after A', 'switch-account-a'], ['switch before B', reset.distinct], ['switch after B', 'switch-account-b']]) {
      assert.equal((await event(name, 1, id))[0].distinct_id, expected);
    }
  });
});

contract('browser.identify-batched-controls', async () => {
  await identifiedBrowser(async ({ tab, scoped, observed }) => {
    const id = scoped.project.id;
    const response = await sdkSend(tab, 'identified batch before', () => {
      const anonymous = posthog.get_distinct_id();
      const before = posthog.capture('identified batch before', { order: 1 });
      posthog.identify('batch-account', { plan: 'starter' }, { first_source: 'batch' });
      posthog.identify('batch-account', { plan: 'team' }, { first_source: 'ignored' });
      const after = posthog.capture('identified batch after', { order: 2 });
      return { anonymous, before: before.uuid, after: after.uuid };
    });
    const state = response.value;
    await event('identified batch before', 1, id); await event('identified batch after', 1, id);
    const payloads = observed.map(decodeWire);
    const immediate = payloads.find(payload => payload.batch.some(item => item.event === '$identify'));
    assert.equal(immediate.batch.length, 1, 'identify bypasses the SDK batching queue');
    const wire = observed.find(wire => decodeWire(wire).batch.some(item => item.event === 'identified batch before'));
    const batch = decodeWire(wire).batch;
    assert.deepEqual(batch.map(item => item.event), ['identified batch before', '$set', 'identified batch after']);
    assert.equal(batch[0].properties.distinct_id, state.anonymous);
    assert.equal(batch[2].properties.distinct_id, 'batch-account');
    assert.equal(batch[1].properties.$set.plan, 'team');
    const saved = await events(id);
    const control = identities(id).control_events.find(item => item.event === '$identify');
    assert.ok(control.id < saved.find(item => item.uuid === state.before).id, 'queued pre-login capture arrives after the immediate identify');
    assert.equal(saved.find(item => item.uuid === state.before).distinct_id, state.anonymous);
    const snapshot = identities(id);
    assert.equal(snapshot.identity_links.find(row => row.anonymous_id === state.anonymous).identified_id, 'batch-account');
    const person = snapshot.identity_profiles.find(row => row.identified_id === 'batch-account');
    assert.equal(person.traits.plan, 'team'); assert.equal(person.traits.first_source, 'batch');
    const replay = await fetch(api + wire.path, { method: 'POST', headers: { 'content-type': wire.headers['content-type'] }, body: wire.body });
    assert.equal(replay.status, 200);
    assert.deepEqual(await replay.json(), { status: 1, durability: 'durable', accepted: 0, duplicates: batch.length, identity_ignored: 0 });
    assert.deepEqual(identities(id), snapshot, 'mixed-batch retry leaves durable identity and trait version unchanged');
  }, true);
});

contract('node.identify-target-only', async () => {
  const scoped = await management('/api/v1/projects', { method: 'POST', body: { name: 'Node identify contract' } });
  identifiedProjects.push(scoped);
  const start = wires.length;
  const client = new PostHog(scoped.ingest_token, { ...nodeProfile, host: proxyURL, fetch: safeFetch });
  try {
    client.identify({ distinctId: 'node-identified-account', properties: { plan: 'pro', nested: { locale: 'fr' } } });
    client.capture({ distinctId: 'node-identified-account', event: 'node after identify' });
    client.identify({ distinctId: 'node-identified-account', properties: { $set: { plan: 'team' }, $set_once: { first_source: 'node' } } });
    client.capture({ distinctId: 'node-identified-account', event: 'node after trait update' });
    await client.flush();
    const batches = wires.slice(start).map(decodeWire);
    assert.equal(batches.length, 1);
    assert.deepEqual(batches[0].batch.map(item => item.event), ['$identify', 'node after identify', '$identify', 'node after trait update']);
    const control = batches[0].batch[0];
    assert.equal(control.distinct_id, 'node-identified-account');
    assert.equal(control.properties.$anon_distinct_id, undefined);
    assert.deepEqual(control.properties.$set, { plan: 'pro', nested: { locale: 'fr' } });
    assert.equal(control.$set, undefined, 'Node traits live in event properties');
    const snapshot = identities(scoped.project.id);
    assert.deepEqual(snapshot.identity_links, [], 'target-only Node identify creates no anonymous link');
    const person = snapshot.identity_profiles.find(row => row.identified_id === 'node-identified-account');
    assert.deepEqual(person.traits, { plan: 'team', nested: { locale: 'fr' }, first_source: 'node' });
    assert.equal((await event('node after trait update', 1, scoped.project.id))[0].distinct_id, 'node-identified-account');
  } finally { await client.shutdown(); }
});

contract('porcupine-browser.capture-and-retry', async () => {
  const scoped = await management('/api/v1/projects', { method: 'POST', body: { name: 'Porcupine browser SDK contract' } });
  const projectID = scoped.project.id;
  const isolated = await browser.newContext({ serviceWorkers: 'block' });
  const firstWire = browserWires.length;
  try {
    await isolated.addCookies([{ name: 'fixture_cookie', value: 'keep-off-ingestion', url: origin }]);
    const existingCookies = await isolated.cookies();
    await isolated.route('**/*', async route => {
      const request = route.request();
      const url = new URL(request.url());
      if (url.origin === origin || url.origin === proxyURL && url.pathname === '/batch/' && request.method() === 'POST') {
        await route.continue();
        return;
      }
      blocked.push('unexpected Porcupine browser SDK request');
      await route.abort();
    });
    const tab = await isolated.newPage();
    await tab.goto(origin);
    await tab.addScriptTag({ path: join(root, 'sdk/browser/dist/porcupine.js') });
    failBrowserAcknowledgement = true;
    const response = tab.waitForResponse(result => result.url() === proxyURL + '/batch/' && result.status() === 200);
    const queued = await tab.evaluate(({ apiHost, key }) => {
      window.history.replaceState(null, '', '/?private=fixture-value#fragment');
      window.firstPartyAnalytics = window.Porcupine.init(key, { api_host: apiHost });
      return window.firstPartyAnalytics.capture('porcupine SDK signup', { plan: 'starter' });
    }, { apiHost: proxyURL, key: scoped.ingest_token });
    assert.equal(queued, true);
    assert.deepEqual(await (await response).json(), { status: 1, durability: 'durable', accepted: 0, duplicates: 2, identity_ignored: 0 });

    const observed = browserWires.slice(firstWire);
    assert.equal(observed.length, 2);
    for (const request of observed) {
      assert.equal(request.headers.cookie, undefined, 'eligible browser cookie is omitted');
      assert.equal(request.headers.referer, undefined, 'fetch omits the document referrer');
      assert.equal(request.status, 200, 'the real server accepted both attempts');
    }
    const [first, retried] = observed.map(decodeWire);
    assert.deepEqual(retried.batch, first.batch, 'retry preserves capture time and UUID');
    assert.ok(Date.parse(retried.sent_at) - Date.parse(first.sent_at) >= 2000, 'browser respects Retry-After and refreshes send time');
    assert.deepEqual(first.batch.map(item => item.event), ['$pageview', 'porcupine SDK signup']);
    assert.ok(first.batch.every(item => item.distinct_id === '$posthog_cookieless'));
    assert.equal(JSON.stringify(first).includes('fixture-value'), false);
    assert.equal(JSON.stringify(first).includes('Porcupine SDK contract'), false);
    assert.deepEqual(await isolated.cookies(), existingCookies);
    assert.deepEqual(await tab.evaluate(async () => [localStorage.length, sessionStorage.length, (await indexedDB.databases()).length]), [0, 0, 0]);

    const [pageview] = await event('$pageview', 1, projectID);
    const [signup] = await event('porcupine SDK signup', 1, projectID);
    assert.match(pageview.distinct_id, /^cookieless_[a-f0-9]+$/);
    assert.equal(signup.distinct_id, pageview.distinct_id);
    assert.equal(signup.properties.plan, 'starter');
    assert.equal(signup.uuid, first.batch[1].uuid);
    assertTimestamp(signup, first, first.batch[1]);
    assert.equal((await events(projectID)).length, 2);
    assert.equal(await tab.evaluate(() => {
      window.firstPartyAnalytics.disable();
      return window.firstPartyAnalytics.capture('disabled event');
    }), false);
  } finally {
    failBrowserAcknowledgement = false;
    await isolated.close();
  }
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
  for (const scoped of identifiedProjects) {
    for (const saved of await events(scoped.project.id)) {
      assert.equal(saved.project_id, scoped.project.id);
      assert.equal(JSON.stringify(saved).includes(scoped.ingest_token), false);
    }
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
    if (typeof value === 'string') return value.replaceAll(project.ingest_token, 'FIXTURE_INGEST_TOKEN').replaceAll(origin, 'http://browser.example').replaceAll(api, 'http://ingest.example').replaceAll(proxyURL, 'http://ingest.example').replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, normalizedUUID).replace(/20\d\d-\d\d-\d\dT[0-9:.]+Z/g, '2025-02-03T04:05:06.789Z');
    return value;
  };
  await mkdir(join(here, 'fixtures'), { recursive: true });
  const records = [
    ['node-batch', wires[0], versions['posthog-node']],
    ['browser-capture', browserWires[0], versions['posthog-js']],
    ['browser-batch-gzip', browserWires.find(wire => wire.body[0] === 0x1f && wire.body[1] === 0x8b), versions['posthog-js']],
    ['browser-pageleave-beacon', browserWires.find(wire => wire.query.compression === 'base64' && wire.headers['sec-fetch-mode'] === 'no-cors'), versions['posthog-js']],
  ];
  for (const [name, wire, version] of records) {
    if (!wire) continue;
    const payload = decodeWire(wire);
    const beacon = name === 'browser-pageleave-beacon';
    const gzip = name === 'browser-batch-gzip';
    const provenance = {
      sdk: name.startsWith('node') ? 'posthog-node' : 'posthog-js',
      version,
      profile: beacon ? 'analytics-only-pageleave-beacon' : gzip ? 'analytics-only-gzip-batch' : 'analytics-only',
      source: beacon ? 'Observed Chromium sendBeacon HTTP request on cross-origin navigation' : 'Observed real SDK HTTP request to temporary Go server',
      normalization: 'Credentials, UUIDs, timestamps, local addresses replaced; remaining SDK properties retained',
      path: wire.path,
      query: beacon ? new URLSearchParams(wire.query).toString() : undefined,
      content_type: wire.headers['content-type'],
      content_encoding: wire.headers['content-encoding'] || 'identity',
      body_format: beacon ? 'form-base64' : gzip ? 'gzip' : 'json',
    };
    await writeFile(join(here, 'fixtures', name + '.json'), JSON.stringify({ provenance, payload: normalize(payload) }, null, 2) + '\n');
  }
}
