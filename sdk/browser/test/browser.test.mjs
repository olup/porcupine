import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { setImmediate } from 'node:timers/promises'
import { test } from 'node:test'
import { runInNewContext } from 'node:vm'
import { init, initCloud } from '../dist/index.js'

const key = 'pc_ci_test'
const api_host = 'https://analytics.example.test'
const start = Date.parse('2026-01-01T10:00:00.000Z')
const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

function replaceProperty(t, target, name, descriptor) {
  const original = Object.getOwnPropertyDescriptor(target, name)
  Object.defineProperty(target, name, { configurable: true, ...descriptor })
  t.after(() => {
    if (original) Object.defineProperty(target, name, original)
    else delete target[name]
  })
}

function durable(count, duplicates = 0) {
  return Response.json({
    status: 1,
    durability: 'durable',
    accepted: count - duplicates,
    duplicates,
    identity_ignored: 0,
  })
}

function page(t, respond = call => durable(call.payload.batch.length)) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: start })
  t.mock.method(performance, 'now', () => Date.now() - start)
  t.mock.method(Math, 'random', () => 0)
  const window = new EventTarget()
  const document = Object.assign(new EventTarget(), { visibilityState: 'visible' })
  replaceProperty(t, globalThis, 'window', { value: window })
  replaceProperty(t, globalThis, 'document', { value: document })
  const calls = []
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const call = { url, options, payload: JSON.parse(options.body) }
    calls.push(call)
    return respond(call, calls.length)
  })
  return {
    window, document, calls,
    connect(options = {}) {
      const client = init(key, { api_host, capture_pageview: false, ...options })
      t.after(() => client.disable())
      return client
    },
    async tick(ms) {
      t.mock.timers.tick(ms)
      await setImmediate()
    },
  }
}

test('queues one bare pageview, snapshots explicit properties and sends the cookieless wire format', async t => {
  const { calls, connect, tick, window, document } = page(t)
  const accesses = []
  for (const [object, names] of [
    [globalThis, ['location', 'navigator', 'localStorage', 'sessionStorage']],
    [window, ['location', 'navigator', 'localStorage', 'sessionStorage']],
    [document, ['cookie', 'location', 'referrer', 'title']],
  ]) {
    for (const name of names) {
      replaceProperty(t, object, name, {
        get() {
          accesses.push(name)
          throw new Error(name)
        },
        set() {
          accesses.push(name)
          throw new Error(name)
        },
      })
    }
  }
  const client = connect({ api_host: api_host + '/proxy/', capture_pageview: undefined })
  const properties = { plan: { tier: 'pro' }, token: 'do-not-send', toJSON: () => ({ token: 'do-not-send' }) }
  assert.equal(client.capture('signed_up', properties), true)
  properties.plan.tier = 'changed'
  await tick(999)
  assert.equal(calls.length, 0)
  await tick(1)
  assert.deepEqual(accesses, [])
  assert.equal(calls.length, 1)
  const [{ url, options, payload }] = calls
  assert.equal(url, api_host + '/proxy/batch/')
  assert.equal(options.method, 'POST')
  assert.deepEqual(options.headers, { 'Content-Type': 'text/plain' })
  assert.equal(options.credentials, 'omit')
  assert.equal(options.referrerPolicy, 'no-referrer')
  assert.equal(options.redirect, 'error')
  assert.equal(options.keepalive, true)
  assert.deepEqual(Object.keys(payload).sort(), ['api_key', 'batch', 'sent_at'])
  assert.equal(payload.api_key, key)
  assert.equal(payload.sent_at, new Date(start + 1000).toISOString())
  const [first, second] = payload.batch
  for (const event of [first, second]) {
    assert.match(event.uuid, uuidV4)
    assert.equal(event.timestamp, new Date(start).toISOString())
    assert.equal(event.distinct_id, '$posthog_cookieless')
    assert.deepEqual(Object.keys(event).sort(), ['distinct_id', 'event', 'properties', 'timestamp', 'uuid'])
  }
  assert.equal(first.event, '$pageview')
  assert.deepEqual(first.properties, {})
  assert.equal(second.event, 'signed_up')
  assert.deepEqual(second.properties, { plan: { tier: 'pro' } })
  assert.notEqual(first.uuid, second.uuid)
})

test('Cloud quickstart sends a pageview and the captured event to Porcupine Cloud', async t => {
  const { calls, tick } = page(t)
  const analytics = initCloud('pc_ci_YOUR_KEY')
  t.after(() => analytics.disable())
  assert.equal(analytics.capture('signup_completed', { plan: 'starter' }), true)
  await tick(1000)

  assert.equal(calls.length, 1)
  const [{ url, payload }] = calls
  assert.equal(url, 'https://api.getporcupine.eu/batch/')
  assert.equal(payload.api_key, 'pc_ci_YOUR_KEY')
  assert.deepEqual(payload.batch.map(event => [event.event, event.properties]), [
    ['$pageview', {}],
    ['signup_completed', { plan: 'starter' }],
  ])
})

test('Cloud initialization honors pageview opt-out', async t => {
  const { calls, tick } = page(t)
  const analytics = initCloud(key, { capture_pageview: false })
  t.after(() => analytics.disable())
  await tick(1000)
  assert.equal(calls.length, 0)
  assert.equal(analytics.capture('$pageview', { page: 'pricing' }), true)
  await tick(1000)
  assert.equal(calls.length, 1)
  assert.equal(calls[0].url, 'https://api.getporcupine.eu/batch/')
  assert.deepEqual(calls[0].payload.batch.map(event => [event.event, event.properties]), [
    ['$pageview', { page: 'pricing' }],
  ])
})

test('honors opt-out, rejects invalid input, and sends an explicit view when the page hides', async t => {
  const { calls, connect, document, window, tick } = page(t)
  const client = connect()
  await tick(1000)
  assert.equal(calls.length, 0)
  const circular = {}
  circular.self = circular
  for (const event of ['', '  ', 'é'.repeat(251), '$identify', '$set', '$unknown']) {
    assert.equal(client.capture(event), false)
  }
  assert.equal(client.capture('invalid', circular), false)
  assert.equal(client.capture('invalid', []), false)
  assert.equal(client.capture('$pageview', { page: 'pricing' }), true)
  document.visibilityState = 'hidden'
  document.dispatchEvent(new Event('visibilitychange'))
  window.dispatchEvent(new Event('pagehide'))
  await tick(10)
  assert.equal(calls.length, 1)
  assert.deepEqual(calls[0].payload.batch.map(event => [event.event, event.properties]), [
    ['$pageview', { page: 'pricing' }],
  ])
})

test('requires a public project key and a base URL without leaking bad inputs', () => {
  assert.throws(() => init('pc_secret_test', { api_host }), { message: 'A public pc_ci_ ingestion key is required' })
  assert.throws(() => initCloud('pc_secret_test'), { message: 'A public pc_ci_ ingestion key is required' })
  assert.throws(() => init(key), { message: /^api_host must be an absolute HTTP/ })
  for (const bad of [undefined, '/analytics', 'mailto:secret@example.test', 'https://user:secret@example.test', api_host + '?secret=1', api_host + '#secret']) {
    assert.throws(() => init(key, { api_host: bad }), {
      message: 'api_host must be an absolute HTTP(S) base URL without credentials, a query, or a fragment',
    })
  }
})

test('initializing on a server does not send an automatic pageview', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const fetch = t.mock.method(globalThis, 'fetch', async () => durable(1))
  const client = init(key, { api_host })
  t.after(() => client.disable())
  t.mock.timers.tick(1000)
  await setImmediate()
  assert.equal(fetch.mock.callCount(), 0)
})

test('retries the same events after network failure and 503, honoring exposed Retry-After', async t => {
  const { calls, connect, tick, window } = page(t, (call, attempt) => {
    if (attempt === 1) throw new TypeError('network error')
    if (attempt === 2) return new Response(null, { status: 503, headers: { 'Retry-After': '3' } })
    return durable(call.payload.batch.length, call.payload.batch.length)
  })
  const client = connect()
  client.capture('first')
  await tick(1000)
  client.capture('second')
  window.dispatchEvent(new Event('pagehide'))
  await tick(999)
  assert.equal(calls.length, 1)
  await tick(1)
  assert.equal(calls.length, 2)
  await tick(2999)
  assert.equal(calls.length, 2)
  await tick(1)
  assert.equal(calls.length, 3)
  assert.deepEqual(calls[1].payload.batch, calls[0].payload.batch)
  assert.deepEqual(calls[2].payload.batch, calls[0].payload.batch)
  assert.deepEqual(calls.map(call => call.payload.sent_at), [1000, 2000, 5000].map(ms => new Date(start + ms).toISOString()))
  await tick(1000)
  assert.equal(calls.length, 4)
  assert.deepEqual(calls[3].payload.batch.map(event => event.event), ['second'])
  await tick(10_000)
  assert.equal(calls.length, 4)
})

test('requires a matching acknowledgment and bounds retries, including HTTP 429', async t => {
  const responses = [
    () => new Response(null, { status: 429, headers: { 'Retry-After': new Date(start + 3000).toUTCString() } }),
    () => Response.json({ status: 1, durability: 'buffered', received: 2 }),
    () => Response.json({ status: 1 }),
    () => new Response('not JSON'),
    () => Response.json({ status: 1, durability: 'buffered', received: 1 }),
  ]
  const { calls, connect, tick } = page(t, (_, attempt) => responses[attempt - 1]())
  const client = connect()
  client.capture('retryable')
  await tick(1000)
  await tick(1999)
  assert.equal(calls.length, 1)
  await tick(1)
  await tick(2000)
  await tick(4000)
  assert.equal(calls.length, 4)
  assert.ok(calls.every(call => call.payload.batch[0].uuid === calls[0].payload.batch[0].uuid))
  client.capture('buffered')
  await tick(7999)
  assert.equal(calls.length, 4)
  await tick(1)
  await tick(30_000)
  assert.equal(calls.length, 5)
  assert.equal(calls[4].payload.batch[0].event, 'buffered')
})

test('drops batches refused for quota, suspension or a permanent 4xx', async t => {
  const responses = [
    () => Response.json({ status: 1, accepted: 0, duplicates: 0, identity_ignored: 0, quota_limited: ['events'] }),
    () => Response.json({ status: 1, code: 'ingestion_budget_exceeded' }),
    () => Response.json({ status: 1, code: 'ingestion_suspended' }),
    () => new Response(null, { status: 403 }),
  ]
  const { calls, connect, tick } = page(t, (_, attempt) => responses[attempt - 1]())
  for (let i = 0; i < responses.length; i++) {
    const client = connect()
    client.capture('event-' + i)
    await tick(1000)
    await tick(61_000)
    assert.equal(calls.length, i + 1)
    assert.equal(calls[i].payload.batch[0].event, 'event-' + i)
    client.disable()
  }
})

test('a quota refusal drops the active batch and pauses new captures through page hide', async t => {
  let refuseFirst
  const { calls, connect, tick, window, document } = page(t, (_, attempt) => attempt === 1
    ? new Promise(resolve => { refuseFirst = resolve })
    : durable(1))
  const client = connect()
  client.capture('first')
  await tick(1000)
  client.capture('second')
  refuseFirst(Response.json({ status: 1, code: 'ingestion_budget_exceeded' }))
  await tick(0)
  document.visibilityState = 'hidden'
  document.dispatchEvent(new Event('visibilitychange'))
  window.dispatchEvent(new Event('pagehide'))
  await tick(59_999)
  assert.equal(calls.length, 1)
  await tick(1)
  assert.deepEqual(calls.map(call => call.payload.batch.map(event => event.event)), [['first'], ['second']])
})

test('an empty quota array does not delay later batches', async t => {
  const { calls, connect, tick, window } = page(t, () => Response.json({
    status: 1, durability: 'buffered', received: 1, quota_limited: [],
  }))
  const client = connect()
  client.capture('first')
  await tick(1000)
  client.capture('second')
  window.dispatchEvent(new Event('pagehide'))
  await tick(0)
  assert.deepEqual(calls.map(call => call.payload.batch.map(event => event.event)), [['first'], ['second']])
})

test('a terminal retry still delays the next batch, even on page hide', async t => {
  const { calls, connect, tick, window } = page(t, (call, attempt) => {
    if (attempt === 1) return new Response(null, { status: 408 })
    if (attempt <= 3) return new Response(null, { status: 503 })
    if (attempt === 4) return new Response(null, { status: 503, headers: { 'Retry-After': '10' } })
    return durable(call.payload.batch.length)
  })
  const client = connect()
  client.capture('first')
  await tick(1000)
  client.capture('second')
  await tick(1000)
  await tick(2000)
  await tick(4000)
  assert.equal(calls.length, 4)
  assert.ok(calls.every(call => call.payload.batch[0].uuid === calls[0].payload.batch[0].uuid))
  window.dispatchEvent(new Event('pagehide'))
  await tick(9999)
  assert.equal(calls.length, 4)
  await tick(1)
  assert.deepEqual(calls[4].payload.batch.map(event => event.event), ['second'])
})

test('a long Retry-After allows pending events to expire and keeps the next batch waiting', async t => {
  const { calls, connect, tick, window } = page(t, (call, attempt) => attempt === 1
    ? new Response(null, { status: 429, headers: { 'Retry-After': '300' } })
    : durable(call.payload.batch.length))
  const client = connect()
  client.capture('first')
  await tick(1000)
  for (let i = 0; i < 200; i++) assert.equal(client.capture('expires'), true)
  assert.equal(client.capture('overflow'), false)
  await tick(240_000)
  assert.equal(client.capture('fresh'), true)
  window.dispatchEvent(new Event('pagehide'))
  await tick(59_999)
  assert.equal(calls.length, 1)
  await tick(1)
  assert.deepEqual(calls[1].payload.batch.map(event => event.event), ['fresh'])
})

test('bounds the queue count while a single request is in flight, then drains FIFO', async t => {
  let acknowledgeFirst
  const { calls, connect, tick } = page(t, (call, attempt) => attempt === 1
    ? new Promise(resolve => { acknowledgeFirst = resolve })
    : durable(call.payload.batch.length))
  const client = connect()
  for (let i = 0; i < 200; i++) assert.equal(client.capture('event-' + i), true)
  assert.equal(client.capture('overflow'), false)
  assert.equal(calls.length, 1)
  assert.equal(calls[0].payload.batch.length, 20)
  acknowledgeFirst(durable(20))
  await tick(0)
  assert.equal(calls.length, 10)
  assert.deepEqual(calls.flatMap(call => call.payload.batch.map(event => event.event)), Array.from({ length: 200 }, (_, i) => 'event-' + i))
})

test('bounds UTF-8 batch and queue bytes, including the request envelope', async t => {
  let acknowledgeFirst
  const { calls, connect, tick } = page(t, (call, attempt) => attempt === 1
    ? new Promise(resolve => { acknowledgeFirst = resolve })
    : durable(call.payload.batch.length))
  const client = connect()
  assert.equal(client.capture('oversized', { text: '🦔'.repeat(13_000) }), false)
  for (let i = 0; i < 15; i++) assert.equal(client.capture('event', { index: i, text: '🦔'.repeat(4096) }), true)
  assert.equal(client.capture('overflow', { text: '🦔'.repeat(4096) }), false)
  assert.equal(calls.length, 1)
  assert.equal(calls[0].payload.batch.length, 2)
  acknowledgeFirst(durable(2))
  await tick(0)
  await tick(1000)
  assert.equal(calls.flatMap(call => call.payload.batch).length, 15)
  assert.ok(calls.every(call => Buffer.byteLength(call.options.body) <= 48 * 1024))
})

test('times out a stalled request and expires unsent events', async t => {
  const { calls, connect, tick } = page(t, ({ options }) => new Promise((_, reject) => {
    options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true })
  }))
  const client = connect()
  client.capture('stalled')
  await tick(1000)
  await tick(10_000)
  assert.equal(calls[0].options.signal.aborted, true)
  await tick(120_000)
  assert.equal(calls.length, 1)
})

test('disable aborts in-flight work and cancels scheduled work and listeners', async t => {
  const { calls, connect, tick, window, document } = page(t, ({ options }) => new Promise((_, reject) => {
    options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true })
  }))
  const scheduled = connect({ capture_pageview: true })
  scheduled.disable()
  await tick(1000)
  assert.equal(calls.length, 0)
  const client = connect({ capture_pageview: true })
  await tick(1000)
  client.capture('while_sending')
  assert.equal(calls.length, 1)
  client.disable()
  client.disable()
  assert.equal(calls[0].options.signal.aborted, true)
  assert.equal(client.capture('disabled'), false)
  document.visibilityState = 'hidden'
  document.dispatchEvent(new Event('visibilitychange'))
  window.dispatchEvent(new Event('pagehide'))
  await tick(30_000)
  assert.equal(calls.length, 1)
})

test('disable cancels scheduled retries', async t => {
  const { calls, connect, tick } = page(t, () => new Response(null, { status: 503 }))
  const client = connect()
  client.capture('retrying')
  await tick(1000)
  client.disable()
  await tick(30_000)
  assert.equal(calls.length, 1)
})

test('classic bundle exposes the same public interface', async () => {
  const source = await readFile(new URL('../dist/porcupine.js', import.meta.url), 'utf8')
  const context = {}
  runInNewContext(source, context)
  assert.deepEqual(Object.keys(context.Porcupine).sort(), ['init', 'initCloud'])
})

test('captureException uses the cookieless queue with bounded, sanitized stack frames', async t => {
  const { calls, connect, tick, window } = page(t)
  const client = connect({ build_id: 'web-42', release: 'v1.2.3' })
  const error = new TypeError('Could not load https://user:password@example.test/assets/app.js?token=hidden#private')
  error.stack = [
    'TypeError: Could not load',
    '    at render (https://user:password@example.test/assets/app.js?token=hidden#private:12:34)',
    'start@https://cdn.example.test/assets/entry.js#private:2:1',
    '    at data:text/javascript,private:1:2',
  ].join('\n')
  window.dispatchEvent(Object.assign(new Event('error'), { error }))
  assert.equal(client.captureException(error), true)
  await tick(1000)
  const [event] = calls[0].payload.batch
  assert.equal(calls[0].payload.batch.length, 1)
  assert.equal(event.event, '$exception')
  assert.equal(event.distinct_id, '$posthog_cookieless')
  assert.deepEqual(event.properties, {
    $exception_build_id: 'web-42',
    $exception_release: 'v1.2.3',
    $exception_list: [{
      type: 'TypeError',
      value: 'Could not load [url]',
      stacktrace: { type: 'raw', frames: [
        { function: 'start', filename: 'https://cdn.example.test/assets/entry.js', lineno: 2, colno: 1 },
        { function: 'render', filename: 'https://example.test/assets/app.js', lineno: 12, colno: 34 },
      ] },
    }],
  })
})

test('captures window errors and promise rejections only when enabled and removes both listeners on disable', async t => {
  const { calls, connect, tick, window } = page(t)
  const add = t.mock.method(window, 'addEventListener')
  const remove = t.mock.method(window, 'removeEventListener')
  const client = connect({ capture_exceptions: true })
  const error = Object.assign(new Event('error', { cancelable: true }), {
    message: 'Script failed', filename: '/assets/app.js?secret#fragment', lineno: 2, colno: 5,
  })
  const rejection = Object.assign(new Event('unhandledrejection', { cancelable: true }), {
    reason: '/profile?private=yes',
  })
  window.dispatchEvent(new Event('error'))
  assert.equal(window.dispatchEvent(error), true)
  assert.equal(window.dispatchEvent(rejection), true)
  await tick(1000)
  assert.deepEqual(calls[0].payload.batch.map(event => event.properties.$exception_list[0]), [
    {
      type: 'Error',
      value: 'Script failed',
      stacktrace: { type: 'raw', frames: [{ filename: '/assets/app.js', lineno: 2, colno: 5 }] },
    },
    { type: 'Error', value: '[redacted]' },
  ])
  client.disable()
  for (const name of ['error', 'unhandledrejection']) {
    const [, listener] = add.mock.calls.find(call => call.arguments[0] === name).arguments
    assert.ok(remove.mock.calls.some(call => call.arguments[0] === name && call.arguments[1] === listener))
  }
  assert.equal(client.captureException(new Error('disabled')), false)
  window.dispatchEvent(error)
  window.dispatchEvent(rejection)
  await tick(1000)
  assert.equal(calls.length, 1)
})

test('bounds exception bursts, skips arbitrary object fields, and guards reentrant error getters', async t => {
  const { calls, connect, tick } = page(t)
  const client = connect({ build_id: 'https://example.test?private' })
  let reads = 0
  const object = { get message() { reads++; return 'private' }, toString() { reads++; return 'private' } }
  const error = new Error('error')
  let reentered
  Object.defineProperty(error, 'message', { get() {
    reentered = client.captureException('recursive')
    throw new Error('blocked')
  } })
  error.stack = Array.from({ length: 30 }, (_, i) => `at assets/app.js:${i + 1}:2`).join('\n')
  assert.equal(client.captureException(object), true)
  assert.equal(client.captureException(error), true)
  assert.equal(reentered, false)
  assert.equal(reads, 0)
  for (let i = 0; i < 8; i++) assert.equal(client.captureException('x'.repeat(3000)), true)
  assert.equal(client.captureException('rate limited'), false)
  assert.equal(client.capture('still_available'), true)
  await tick(1000)
  const batch = calls[0].payload.batch
  assert.equal(batch.length, 11)
  assert.deepEqual(batch[0].properties, { $exception_list: [{ type: 'Error', value: 'Non-Error thrown' }] })
  assert.equal(batch[1].properties.$exception_list[0].stacktrace.frames.length, 12)
  assert.equal(batch[2].properties.$exception_list[0].value.length, 1024)
  await tick(59_000)
  assert.equal(client.captureException('window reset'), true)
})
