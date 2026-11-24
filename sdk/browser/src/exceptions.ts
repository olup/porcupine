const maxMessageLength = 1024
const maxFrames = 12
const maxFrameLength = 512

type Frame = { filename: string; lineno: number; colno: number; function?: string }
type Location = { filename: unknown; lineno: unknown; colno: unknown }

function message(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  return value.slice(0, maxMessageLength)
    .replace(/[a-z][a-z\d+.-]*:\/\/\S+|(?:data|blob|javascript):\S+/gi, '[url]')
    .replace(/[^\s]*[?#][^\s]*/g, '[redacted]')
    .replace(/\s+/g, ' ')
    .trim() || undefined
}

export function buildMetadata(value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length > 128 || !/^[\w.-]+$/.test(value)) return undefined
  return value
}

function frame(location: Location): Frame | undefined {
  const { filename, lineno, colno } = location
  if (typeof filename !== 'string' || filename.length > maxFrameLength) return undefined
  if (typeof lineno !== 'number' || !Number.isSafeInteger(lineno) || lineno < 1) return undefined
  if (typeof colno !== 'number' || !Number.isSafeInteger(colno) || colno < 0) return undefined

  const path = filename.split(/[?#]/, 1)[0]
  if (/^https?:\/\//i.test(path)) {
    try {
      const url = new URL(path)
      return { filename: url.origin + url.pathname, lineno, colno }
    } catch {
      return undefined
    }
  }
  if (path.startsWith('//') || !/^[\w./@%+~$-]+$/.test(path)) return undefined
  return { filename: path, lineno, colno }
}

function stackFrames(stack: unknown): Frame[] {
  if (typeof stack !== 'string') return []
  const frames: Frame[] = []
  for (const line of stack.slice(0, 16_384).split('\n')) {
    if (frames.length === maxFrames) break
    const entry = line.trim()
    const chrome = /^at (?:(.*?) \((.*)\)|(.*))$/.exec(entry)
    const firefox = chrome ? null : /^(.*?)@((?:(?:https?:)?\/\/|\.?\/|[\w-]+\/).*)$/i.exec(entry)
    const location = chrome ? chrome[2] ?? chrome[3] : firefox?.[2]
    if (!location) continue
    const parts = /^(.+):(\d+):(\d+)$/.exec(location)
    if (!parts) continue
    const parsed = frame({ filename: parts[1], lineno: Number(parts[2]), colno: Number(parts[3]) })
    if (!parsed) continue
    const name = chrome?.[1] ?? firefox?.[1]
    if (name && name.length <= 128 && /^[\w.<> $-]+$/.test(name)) parsed.function = name
    frames.push(parsed)
  }
  return frames.reverse()
}

function errorProperty(error: Error, field: 'name' | 'message' | 'stack'): unknown {
  try {
    return error[field]
  } catch {
    return undefined
  }
}

export function exception(error: unknown, location?: Location) {
  let type = 'Error'
  let value = message(error)
  let frames: Frame[] = []
  const isError = error instanceof Error
  if (isError) {
    type = buildMetadata(errorProperty(error, 'name')) ?? type
    value = message(errorProperty(error, 'message'))
    frames = stackFrames(errorProperty(error, 'stack'))
  }
  if (frames.length === 0 && location) {
    const source = frame(location)
    if (source) frames.push(source)
  }

  const entry = { type, value: value ?? (isError ? type : 'Non-Error thrown') }
  return frames.length === 0 ? entry : { ...entry, stacktrace: { type: 'raw', frames } }
}
