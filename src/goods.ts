// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Buffer } from 'node:buffer'
import process from 'node:process'
import { createInterface } from 'node:readline'
import { Readable } from 'node:stream'
import { type Mode } from 'node:fs'
import {
  $,
  within,
  ProcessOutput,
  type ProcessPromise,
  path,
  os,
  Fail,
} from './core.ts'
import {
  type Duration,
  getLast,
  identity,
  isStringLiteral,
  parseBool,
  parseDuration,
  randomId,
  toCamelCase,
} from './util.ts'
import {
  type RequestInfo,
  type RequestInit,
  nodeFetch,
  minimist,
  fs,
} from './vendor.ts'

export { versions } from './versions.ts'

export function tempdir(
  prefix: string = `zx-${randomId()}`,
  mode?: Mode
): string {
  const dirpath = path.join(os.tmpdir(), prefix)
  fs.mkdirSync(dirpath, { recursive: true, mode })

  return dirpath
}

export function tempfile(
  name?: string,
  data?: string | Buffer,
  mode?: Mode
): string {
  const filepath = name
    ? path.join(tempdir(), name)
    : path.join(os.tmpdir(), `zx-${randomId()}`)

  if (data === undefined) fs.closeSync(fs.openSync(filepath, 'w', mode))
  else fs.writeFileSync(filepath, data, { mode })

  return filepath
}

export { tempdir as tmpdir, tempfile as tmpfile }

type ArgvOpts = minimist.Opts & { camelCase?: boolean; parseBoolean?: boolean }

export const parseArgv = (
  args: string[] = process.argv.slice(2),
  opts: ArgvOpts = {},
  defs: Record<string, any> = {}
): minimist.ParsedArgs =>
  Object.entries<string>(minimist(args, opts)).reduce<minimist.ParsedArgs>(
    (m, [k, v]) => {
      const kTrans = opts.camelCase ? toCamelCase : identity
      const vTrans = opts.parseBoolean ? parseBool : identity
      const [_k, _v] = k === '--' || k === '_' ? [k, v] : [kTrans(k), vTrans(v)]
      m[_k] = _v
      return m
    },
    defs as minimist.ParsedArgs
  )

export function updateArgv(args?: string[], opts?: ArgvOpts) {
  for (const k in argv) delete argv[k]
  parseArgv(args, opts, argv)
}

export const argv: minimist.ParsedArgs = parseArgv()

export function sleep(duration: Duration): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, parseDuration(duration))
  })
}

const responseToReadable = (response: Response, rs: Readable) => {
  const reader = response.body?.getReader()
  if (!reader) {
    rs.push(null)
    return rs
  }
  rs._read = async () => {
    const result = await reader.read()
    rs.push(result.done ? null : Buffer.from(result.value))
  }
  return rs
}

export function fetch(
  url: RequestInfo,
  init?: RequestInit
): Promise<Response> & {
  pipe: {
    (dest: TemplateStringsArray, ...args: any[]): ProcessPromise
    <D>(dest: D): D
  }
} {
  $.log({ kind: 'fetch', url, init, verbose: !$.quiet && $.verbose })
  const p = nodeFetch(url, init)

  return Object.assign(p, {
    pipe(dest: any, ...args: any[]) {
      const rs = new Readable()
      const _dest = isStringLiteral(dest, ...args)
        ? $({
            halt: true,
            signal: init?.signal as AbortSignal,
          })(dest as TemplateStringsArray, ...args)
        : dest
      p.then(
        (r) => responseToReadable(r, rs).pipe(_dest.run?.()),
        (err) => _dest.abort?.(err)
      )
      return _dest
    },
  })
}

export function echo(...args: any[]): void
export function echo(pieces: TemplateStringsArray, ...args: any[]) {
  const msg = isStringLiteral(pieces, ...args)
    ? args.map((a, i) => pieces[i] + stringify(a)).join('') + getLast(pieces)
    : [pieces, ...args].map(stringify).join(' ')

  console.log(msg)
}

function stringify(arg: any) {
  return arg instanceof ProcessOutput ? arg.toString().trimEnd() : `${arg}`
}

export async function question(
  query?: string,
  {
    choices,
    input = process.stdin,
    output = process.stdout,
  }: {
    choices?: string[]
    input?: NodeJS.ReadStream
    output?: NodeJS.WriteStream
  } = {}
): Promise<string> {
  /* c8 ignore next 5 */
  const completer = Array.isArray(choices)
    ? (line: string) => {
        const hits = choices.filter((c) => c.startsWith(line))
        return [hits.length ? hits : choices, line]
      }
    : undefined
  const rl = createInterface({
    input,
    output,
    terminal: true,
    completer,
  })

  return new Promise((resolve) =>
    rl.question(query ?? '', (answer) => {
      rl.close()
      resolve(answer)
    })
  )
}

export async function stdin(stream: Readable = process.stdin): Promise<string> {
  let buf = ''
  for await (const chunk of stream.setEncoding('utf8')) {
    buf += chunk
  }
  return buf
}

export async function retry<T>(count: number, callback: () => T): Promise<T>
export async function retry<T>(
  count: number,
  duration: Duration | Generator<number>,
  callback: () => T
): Promise<T>
export async function retry<T>(
  count: number,
  d: Duration | Generator<number> | (() => T),
  cb?: () => T
): Promise<T> {
  if (typeof d === 'function') return retry(count, 0, d)
  if (!cb) throw new Fail('Callback is required for retry')

  const total = count
  const gen =
    typeof d === 'object'
      ? d
      : (function* (d) {
          while (true) yield d
        })(parseDuration(d))

  let attempt = 0
  let lastErr: unknown
  while (count-- > 0) {
    attempt++
    try {
      return await cb()
    } catch (err) {
      lastErr = err
      const delay = gen.next().value

      $.log({
        kind: 'retry',
        total,
        attempt,
        delay,
        exception: err,
        verbose: !$.quiet && $.verbose,
        error: `FAIL Attempt: ${attempt}/${total}, next: ${delay}`, // legacy
      })
      if (delay > 0) await sleep(delay)
    }
  }
  throw lastErr
}

export function* expBackoff(
  max: Duration = '60s',
  delay: Duration = '100ms'
): Generator<number, void, unknown> {
  const maxMs = parseDuration(max)
  const randMs = parseDuration(delay)
  let n = 0
  while (true) {
    yield Math.min(randMs * 2 ** n++, maxMs)
  }
}

export async function spinner<T>(callback: () => T): Promise<T>
export async function spinner<T>(title: string, callback: () => T): Promise<T>
export async function spinner<T>(
  title: string | (() => T),
  callback?: () => T
): Promise<T> {
  if (typeof title === 'function') return spinner('', title)
  if ($.quiet || process.env.CI) return callback!()

  let i = 0
  const stream = $.log.output || process.stderr
  const spin = () => stream.write(`  ${'⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'[i++ % 10]} ${title}\r`)
  return within(async () => {
    $.verbose = false
    const id = setInterval(spin, 100)

    try {
      return await callback!()
    } finally {
      clearInterval(id as ReturnType<typeof setTimeout>)
      stream.write(' '.repeat((process.stdout.columns || 1) - 1) + '\r')
    }
  })
}

export function chunk<T>(arr: T[], size: number = 1): T[][] {
  if (typeof size !== 'number' || size < 1 || !Number.isFinite(size)) {
    throw new Fail('Chunk size must be at least 1')
  }
  const chunks: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size))
  }
  return chunks
}

export type PoolOptions = {
  concurrency?: number
}

export interface WorkerPool {
  <R>(fn: () => R | Promise<R>): Promise<R>
  add<R>(fn: () => R | Promise<R>): Promise<R>
  map<T, R>(
    items: Iterable<T> | AsyncIterable<T>,
    callback: (item: T, index: number) => R | Promise<R>
  ): Promise<Awaited<R>[]>
  readonly concurrency: number
  readonly size: number
  readonly pending: number
  clear(): void
  drain(): Promise<void>
  then<TResult1 = void, TResult2 = never>(
    onfulfilled?: ((value: void) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null
  ): Promise<TResult1 | TResult2>
}

async function toArray<T>(items: Iterable<T> | AsyncIterable<T>): Promise<T[]> {
  if (Array.isArray(items)) return items
  if (items && typeof (items as any)[Symbol.iterator] === 'function') {
    return Array.from(items as Iterable<T>)
  }
  if (items && typeof (items as any)[Symbol.asyncIterator] === 'function') {
    const arr: T[] = []
    for await (const item of items as AsyncIterable<T>) {
      arr.push(item)
    }
    return arr
  }
  throw new Fail('Items must be an iterable or array')
}

export function pool(concurrency?: number | PoolOptions): WorkerPool
export function pool<T, R>(
  concurrency: number,
  items: Iterable<T> | AsyncIterable<T>,
  callback: (item: T, index: number) => R | Promise<R>
): Promise<Awaited<R>[]>
export function pool<T, R>(
  items: Iterable<T> | AsyncIterable<T>,
  callback: (item: T, index: number) => R | Promise<R>,
  optsOrConcurrency?: PoolOptions | number
): Promise<Awaited<R>[]>
export function pool(arg1?: any, arg2?: any, arg3?: any): any {
  if (arg2 === undefined) {
    const raw = typeof arg1 === 'number' ? arg1 : (arg1?.concurrency ?? 4)
    const concurrency = Math.floor(raw)
    if (
      typeof concurrency !== 'number' ||
      concurrency < 1 ||
      !Number.isFinite(concurrency)
    ) {
      throw new Fail('Pool concurrency must be at least 1')
    }

    let running = 0
    const queue: Array<() => void> = []
    const drainListeners: Array<() => void> = []

    const pump = () => {
      while (running < concurrency && queue.length > 0) {
        running++
        const task = queue.shift()!
        task()
      }
      if (running === 0 && queue.length === 0) {
        while (drainListeners.length > 0) {
          drainListeners.shift()!()
        }
      }
    }

    const add = <R>(fn: () => R | Promise<R>): Promise<R> => {
      return new Promise<R>((resolve, reject) => {
        const run = async () => {
          try {
            resolve(await fn())
          } catch (err) {
            reject(err)
          } finally {
            running--
            pump()
          }
        }

        queue.push(run)
        pump()
      })
    }

    const map = async <T, R>(
      items: Iterable<T> | AsyncIterable<T>,
      callback: (item: T, index: number) => R | Promise<R>
    ): Promise<Awaited<R>[]> => {
      const arr = await toArray(items)
      if (arr.length === 0) return []

      const results: Awaited<R>[] = new Array(arr.length)
      let idx = 0
      let error: unknown = null
      let hasError = false

      async function worker() {
        while (!hasError && idx < arr.length) {
          const i = idx++
          try {
            results[i] = (await callback(arr[i], i)) as Awaited<R>
          } catch (err) {
            hasError = true
            error = err
            break
          }
        }
      }

      const workerCount = Math.min(concurrency, arr.length)
      const workers = Array.from({ length: workerCount }, () => worker())
      await Promise.all(workers)

      if (hasError) {
        throw error
      }

      return results
    }

    const clear = () => {
      queue.length = 0
    }

    const drain = (): Promise<void> => {
      if (running === 0 && queue.length === 0) {
        return Promise.resolve()
      }
      return new Promise<void>((resolve) => {
        drainListeners.push(resolve)
      })
    }

    const poolFn = <R>(fn: () => R | Promise<R>) => add(fn)
    Object.defineProperties(poolFn, {
      add: { value: add, writable: true, configurable: true },
      map: { value: map, writable: true, configurable: true },
      concurrency: { get: () => concurrency, configurable: true },
      size: { get: () => queue.length, configurable: true },
      pending: { get: () => running, configurable: true },
      clear: { value: clear, writable: true, configurable: true },
      drain: { value: drain, writable: true, configurable: true },
      then: {
        value: (onfulfilled?: any, onrejected?: any) =>
          drain().then(onfulfilled, onrejected),
        writable: true,
        configurable: true,
      },
    })

    return poolFn as WorkerPool
  }

  if (typeof arg1 === 'number' && typeof arg3 === 'function') {
    const p = pool(arg1)
    return p.map(arg2, arg3)
  }

  if (typeof arg2 === 'function') {
    const c = typeof arg3 === 'number' ? arg3 : (arg3?.concurrency ?? 4)
    const p = pool(c)
    return p.map(arg1, arg2)
  }

  throw new Fail('Invalid arguments for pool()')
}
