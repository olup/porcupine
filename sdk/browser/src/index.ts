export interface BrowserOptions {
  api_host: string
  /** Queue one $pageview during init. Defaults to true. */
  capture_pageview?: boolean
}

export interface BrowserClient {
  /** Return whether the event entered the local queue. */
  capture(event: string, properties?: Readonly<Record<string, unknown>>): boolean
  /** Drop pending events and stop this client. */
  disable(): void
}

const maxBatchEvents = 20
const maxBatchBytes = 48 * 1024
const maxQueueEvents = 200
const maxQueueBytes = 256 * 1024
const maxEventAgeMs = 120_000
const flushDelayMs = 1_000
const requestTimeoutMs = 10_000
const maxRetries = 3
const maxRetryDelayMs = 30_000
const retryJitterMs = 250
const encoder = new TextEncoder()
const browserEvents = new Set(['$pageview', '$pageleave', '$autocapture', '$screen', '$exception'])

type QueuedEvent = { json: string; bytes: number; expiresAt: number }

type Delivery =
  | { kind: 'idle' }
  | { kind: 'scheduled' | 'retry'; timer: ReturnType<typeof setTimeout> }
  | { kind: 'sending'; controller: AbortController }
  | { kind: 'disabled' }

type Outcome =
  | { kind: 'accepted' | 'refused' }
  | { kind: 'retry'; retryAfterMs?: number }

function batchEndpoint(apiHost: string): string {
  const message = 'api_host must be an absolute HTTP(S) base URL without credentials, a query, or a fragment'
  let url: URL
  try {
    url = new URL(apiHost)
  } catch {
    throw new TypeError(message)
  }
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    throw new TypeError(message)
  }
  url.pathname = url.pathname.replace(/\/+$/, '') + '/batch/'
  return url.href
}

function eventUUID(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  bytes[6] = (bytes[6] & 0x0f) | 0x40
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('')
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-')
}

function retryAfter(value: string | null): number | undefined {
  if (!value) return undefined
  if (/^\d+$/.test(value)) return Number(value) * 1_000
  const date = Date.parse(value)
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : undefined
}

function isCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}

function acknowledgment(value: unknown, count: number): Outcome {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return { kind: 'retry' }
  if ('quota_limited' in value || ('code' in value && value.code === 'ingestion_budget_exceeded')) {
    return { kind: 'refused' }
  }
  if (!('status' in value) || value.status !== 1 || !('durability' in value) || 'code' in value || 'error' in value) {
    return { kind: 'retry' }
  }
  if (value.durability === 'buffered' && 'received' in value && value.received === count) {
    return { kind: 'accepted' }
  }
  if (
    value.durability === 'durable' &&
    'accepted' in value && isCount(value.accepted) &&
    'duplicates' in value && isCount(value.duplicates) &&
    'identity_ignored' in value && value.identity_ignored === 0 &&
    value.accepted + value.duplicates === count
  ) {
    return { kind: 'accepted' }
  }
  return { kind: 'retry' }
}

export function init(publicKey: string, options: BrowserOptions): BrowserClient {
  if (typeof publicKey !== 'string' || !/^pc_ci_[a-zA-Z0-9_-]{1,128}$/.test(publicKey)) {
    throw new TypeError('A public pc_ci_ ingestion key is required')
  }
  const endpoint = batchEndpoint(options?.api_host)
  const keyJson = JSON.stringify(publicKey)
  const envelopeBytes = encoder.encode(batchBody('2000-01-01T00:00:00.000Z', '')).length
  const queue: QueuedEvent[] = []
  let queueBytes = 0
  let delivery: Delivery = { kind: 'idle' }

  function batchBody(sentAt: string, events: string): string {
    return `{"api_key":${keyJson},"sent_at":"${sentAt}","batch":[${events}]}`
  }

  function discard(count: number): void {
    for (const entry of queue.splice(0, count)) queueBytes -= entry.bytes + 1
  }

  function discardExpired(): number {
    const now = performance.now()
    let count = 0
    for (const entry of queue) {
      if (entry.expiresAt > now) break
      count++
    }
    discard(count)
    return count
  }

  function flush(): void {
    if (delivery.kind === 'scheduled') clearTimeout(delivery.timer)
    else if (delivery.kind !== 'idle') return
    delivery = { kind: 'idle' }
    discardExpired()
    let count = 0
    let bytes = envelopeBytes
    for (const entry of queue) {
      bytes += entry.bytes + (count === 0 ? 0 : 1)
      if (count === maxBatchEvents || bytes > maxBatchBytes) break
      count++
    }
    if (count > 0) void send(count, 0)
  }

  function scheduleFlush(): void {
    if (delivery.kind !== 'idle' && delivery.kind !== 'scheduled') return
    if (queue.length >= maxBatchEvents || queueBytes + envelopeBytes >= maxBatchBytes) {
      flush()
    } else if (delivery.kind === 'idle' && queue.length > 0) {
      delivery = { kind: 'scheduled', timer: setTimeout(flush, flushDelayMs) }
    }
  }

  function finish(count: number, retries: number, outcome: Outcome): void {
    if (delivery.kind === 'disabled') return
    if (outcome.kind === 'retry' && retries < maxRetries) {
      const backoff = 1_000 * 2 ** retries + Math.floor(Math.random() * retryJitterMs)
      const delay = Math.max(backoff, outcome.retryAfterMs ?? 0)
      if (delay <= maxRetryDelayMs && queue[0].expiresAt > performance.now() + delay) {
        const timer = setTimeout(() => {
          const remaining = count - discardExpired()
          if (remaining > 0) {
            void send(remaining, retries + 1)
          } else {
            delivery = { kind: 'idle' }
            flush()
          }
        }, delay)
        delivery = { kind: 'retry', timer }
        return
      }
    }
    discard(count)
    delivery = { kind: 'idle' }
    scheduleFlush()
  }

  async function send(count: number, retries: number): Promise<void> {
    const controller = new AbortController()
    delivery = { kind: 'sending', controller }
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs)
    let outcome: Outcome
    try {
      const batch = queue.slice(0, count).map(entry => entry.json).join(',')
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: batchBody(new Date().toISOString(), batch),
        credentials: 'omit',
        referrerPolicy: 'no-referrer',
        redirect: 'error',
        keepalive: true,
        signal: controller.signal,
      })
      if (response.status === 200) {
        outcome = acknowledgment(await response.json(), count)
      } else {
        void response.body?.cancel().catch(() => {})
        outcome = response.status === 429 || response.status >= 500
          ? { kind: 'retry', retryAfterMs: retryAfter(response.headers.get('Retry-After')) }
          : { kind: 'refused' }
      }
    } catch {
      outcome = { kind: 'retry' }
    } finally {
      clearTimeout(timeout)
    }
    finish(count, retries, outcome)
  }

  function hasSpace(): boolean {
    return delivery.kind !== 'disabled' && queue.length < maxQueueEvents && queueBytes < maxQueueBytes
  }

  function capture(event: string, properties: Readonly<Record<string, unknown>> = {}): boolean {
    if (!hasSpace() || typeof event !== 'string' || event.length > 500 || !event.trim() || encoder.encode(event).length > 500) return false
    if ((event.startsWith('$') && !browserEvents.has(event)) || !properties || typeof properties !== 'object' || Array.isArray(properties)) {
      return false
    }
    const timestamp = new Date().toISOString()
    let json: string
    try {
      const props: Record<string, unknown> = { __proto__: null, ...properties }
      // The PostHog endpoint treats this field as another credential.
      delete props.token
      if (typeof props.toJSON === 'function') delete props.toJSON
      json = JSON.stringify({ event, uuid: eventUUID(), distinct_id: '$posthog_cookieless', timestamp, properties: props })
    } catch {
      return false
    }
    if (json.length + envelopeBytes > maxBatchBytes) return false
    const bytes = encoder.encode(json).length
    if (!hasSpace() || bytes + envelopeBytes > maxBatchBytes || queueBytes + bytes + 1 > maxQueueBytes) return false
    queue.push({ json, bytes, expiresAt: performance.now() + maxEventAgeMs })
    queueBytes += bytes + 1
    scheduleFlush()
    return true
  }

  function onVisibilityChange(): void {
    if (document.visibilityState === 'hidden') flush()
  }

  function disable(): void {
    const previous = delivery
    delivery = { kind: 'disabled' }
    discard(queue.length)
    if (previous.kind === 'sending') previous.controller.abort()
    if (previous.kind === 'scheduled' || previous.kind === 'retry') clearTimeout(previous.timer)
    if (typeof window !== 'undefined') window.removeEventListener('pagehide', flush)
    if (typeof document !== 'undefined') document.removeEventListener('visibilitychange', onVisibilityChange)
  }

  if (typeof window !== 'undefined') window.addEventListener('pagehide', flush)
  if (typeof document !== 'undefined') document.addEventListener('visibilitychange', onVisibilityChange)
  if (options.capture_pageview !== false) capture('$pageview')

  return { capture, disable }
}
