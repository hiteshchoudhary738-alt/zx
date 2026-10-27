// Copyright 2021 Google LLC
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

import assert from 'node:assert'
import { test, describe, after } from 'node:test'
import { Duplex } from 'node:stream'
import { $, chalk, fs, path, dotenv } from '../src/index.ts'
import {
  echo,
  sleep,
  argv,
  parseArgv,
  updateArgv,
  stdin,
  spinner,
  fetch,
  retry,
  question,
  expBackoff,
  tempfile,
  tempdir,
  tmpdir,
  tmpfile,
  versions,
  pool,
  chunk,
} from '../src/goods.ts'
import { Writable } from 'node:stream'
import process from 'node:process'

const __dirname = new URL('.', import.meta.url).pathname
const root = path.resolve(__dirname, '..')

describe('goods', () => {
  function zx(script) {
    return $`node build/cli.js --eval ${script}`.nothrow().timeout('5s')
  }

  describe('question()', async () => {
    test('works', async () => {
      let contents = ''
      class Input extends Duplex {
        constructor() {
          super()
        }
        _read() {}
        _write(chunk, encoding, callback) {
          this.push(chunk)
          callback()
        }
        _final() {
          this.push(null)
        }
      }
      const input = new Input() as any
      const output = new Writable({
        write: function (chunk, encoding, next) {
          contents += chunk.toString()
          next()
        },
      }) as NodeJS.WriteStream

      setTimeout(() => {
        input.write('foo\n')
        input.end()
      }, 10)
      const result = await question('foo or bar? ', {
        choices: ['foo', 'bar'],
        input,
        output,
      })

      assert.equal(result, 'foo')
      assert(contents.includes('foo or bar? '))
    })

    test('integration', async () => {
      const p = $`node build/cli.js --eval "
  let answer = await question('foo or bar? ', { choices: ['foo', 'bar'] })
  echo('Answer is', answer)
"`
      p.stdin.write('foo\n')
      p.stdin.end()
      assert.match((await p).stdout, /Answer is foo/)
    })
  })

  test('echo() works', async () => {
    const log = console.log
    let stdout = ''
    console.log = (...args) => {
      stdout += args.join(' ')
    }
    echo(chalk.cyan('foo'), chalk.green('bar'), chalk.bold('baz'))
    echo`${chalk.cyan('foo')} ${chalk.green('bar')} ${chalk.bold('baz')}`
    echo(
      await $`echo ${chalk.cyan('foo')}`,
      await $`echo ${chalk.green('bar')}`,
      await $`echo ${chalk.bold('baz')}`
    )
    console.log = log
    assert.match(stdout, /foo/)
  })

  test('sleep() works', async () => {
    const now = Date.now()
    await sleep(100)
    assert.ok(Date.now() >= now + 99)
  })

  describe('retry()', () => {
    test('works', async () => {
      let count = 0
      const result = await retry(5, () => {
        count++
        if (count < 5) throw new Error('fail')
        return 'success'
      })
      assert.equal(result, 'success')
      assert.equal(count, 5)
    })

    test('works with custom delay and limit', async () => {
      const now = Date.now()
      let count = 0
      try {
        await retry(3, '2ms', () => {
          count++
          throw new Error('fail')
        })
      } catch (e) {
        assert.match(e.message, /fail/)
        assert.ok(Date.now() >= now + 4)
        assert.equal(count, 3)
      }
    })

    test('throws undefined on count misconfiguration', async () => {
      try {
        await retry(0, () => 'ok')
      } catch (e) {
        assert.equal(e, undefined)
      }
    })

    test('throws err on empty callback', async () => {
      try {
        // @ts-ignore
        await retry(5)
      } catch (e) {
        assert.match(e.message, /Callback is required for retry/)
      }
    })

    test('supports expBackoff', async () => {
      const result = await retry(5, expBackoff('10ms'), () => {
        if (Math.random() < 0.1) throw new Error('fail')
        return 'success'
      })

      assert.equal(result, 'success')
    })

    test('integration', async () => {
      const now = Date.now()
      const p = await zx(`
    try {
      await retry(5, '50ms', () => $\`exit 123\`)
    } catch (e) {
      echo('exitCode:', e.exitCode)
    }
    await retry(5, () => $\`exit 0\`)
    echo('success')
`)
      assert.ok(p.toString().includes('exitCode: 123'))
      assert.ok(p.toString().includes('success'))
      assert.ok(Date.now() >= now + 50 * (5 - 1))
    })

    test('integration with expBackoff', async () => {
      const now = Date.now()
      const p = await zx(`
    try {
      await retry(5, expBackoff('60s', 0), () => $\`exit 123\`)
    } catch (e) {
      echo('exitCode:', e.exitCode)
    }
    echo('success')
`)
      assert.ok(p.toString().includes('exitCode: 123'))
      assert.ok(p.toString().includes('success'))
      assert.ok(Date.now() >= now + 2 + 4 + 8 + 16 + 32)
    })
  })

  test('expBackoff()', async () => {
    const g = expBackoff('10s', '100ms')

    const [a, b, c] = [
      g.next().value,
      g.next().value,
      g.next().value,
    ] as number[]

    assert.equal(a, 100)
    assert.equal(b, 200)
    assert.equal(c, 400)
  })

  describe('spinner()', () => {
    test('works', async () => {
      let contents = ''
      const { CI } = process.env
      const output = new Writable({
        write: function (chunk, encoding, next) {
          contents += chunk.toString()
          next()
        },
      })

      delete process.env.CI
      $.log.output = output as NodeJS.WriteStream

      const p = spinner(() => sleep(100))

      delete $.log.output
      process.env.CI = CI

      await p
      assert(contents.includes('⠋'))
    })

    describe('integration', () => {
      test('works', async () => {
        const out = await zx(
          `
    process.env.CI = ''
    echo(await spinner(async () => {
      await sleep(100)
      await $\`echo hidden\`
      return $\`echo result\`
    }))
  `
        )
        assert(out.stdout.includes('result'))
        assert(out.stderr.includes('⠋'))
        assert(!out.stderr.includes('result'))
        assert(!out.stderr.includes('hidden'))
      })

      test('with title', async () => {
        const out = await zx(
          `
    process.env.CI = ''
    await spinner('processing', () => sleep(100))
  `
        )
        assert.match(out.stderr, /processing/)
      })

      test('disabled in CI', async () => {
        const out = await zx(
          `
    process.env.CI = 'true'
    await spinner('processing', () => sleep(100))
  `
        )
        assert.doesNotMatch(out.stderr, /processing/)
      })

      test('stops on throw', async () => {
        const out = await zx(`
    await spinner('processing', () => $\`wtf-cmd\`)
  `)
        assert.match(out.stderr, /Error:/)
        assert(out.exitCode !== 0)
      })
    })
  })

  describe('args', () => {
    test('parseArgv() works', () => {
      assert.deepEqual(
        parseArgv(
          // prettier-ignore
          [
          '--foo-bar', 'baz',
          '-a', '5',
          '-a', '42',
          '--aaa', 'AAA',
          '--force',
          './some.file',
          '--b1', 'true',
          '--b2', 'false',
          '--b3',
          '--b4', 'false',
          '--b5', 'true',
          '--b6', 'str'
        ],
          {
            boolean: ['force', 'b3', 'b4', 'b5', 'b6'],
            camelCase: true,
            parseBoolean: true,
            alias: { a: 'aaa' },
          },
          {
            def: 'def',
          }
        ),
        {
          a: [5, 42, 'AAA'],
          aaa: [5, 42, 'AAA'],
          fooBar: 'baz',
          force: true,
          _: ['./some.file', 'str'],
          b1: true,
          b2: false,
          b3: true,
          b4: false,
          b5: true,
          b6: true,
          def: 'def',
        }
      )
    })

    test('updateArgv() works', () => {
      updateArgv(['--foo', 'bar'])
      assert.deepEqual(argv, {
        _: [],
        foo: 'bar',
      })
    })
  })

  test('stdin()', async () => {
    const stream = fs.createReadStream(path.resolve(root, 'package.json'))
    const input = await stdin(stream)
    assert.match(input, /"name": "zx"/)
  })

  test('fetch()', async () => {
    const req1 = fetch('https://github.com/')
    const req2 = fetch('https://github.com/')
    const req3 = fetch('https://github.com/', { method: 'OPTIONS' })

    const p1 = (await req1.pipe`cat`).stdout
    const p2 = (await req2.pipe($`cat`)).stdout
    const p3 = (await req3.pipe`cat`).stdout

    assert.equal((await req1).status, 200)
    assert.equal((await req2).status, 200)
    assert.equal((await req3).status, 404)
    assert(p1.includes('GitHub'))
    assert(p2.includes('GitHub'))
    assert(p3.includes('GitHub'))
  })

  describe('dotenv', () => {
    test('parse()', () => {
      assert.deepEqual(dotenv.parse(''), {})
      assert.deepEqual(
        dotenv.parse('ENV=v1\nENV2=v2\n\n\n  ENV3  =    v3   \nexport ENV4=v4'),
        {
          ENV: 'v1',
          ENV2: 'v2',
          ENV3: 'v3',
          ENV4: 'v4',
        }
      )

      const multiline = `SIMPLE=xyz123
# comment ###
NON_INTERPOLATED='raw text without variable interpolation' 
MULTILINE = """
long text here, # not-comment
e.g. a private SSH key
"""
ENV=v1\nENV2=v2\n\n\n\t\t  ENV3  =    v3   \n   export ENV4=v4
ENV5=v5 # comment
`
      assert.deepEqual(dotenv.parse(multiline), {
        SIMPLE: 'xyz123',
        NON_INTERPOLATED: 'raw text without variable interpolation',
        MULTILINE: 'long text here, # not-comment\ne.g. a private SSH key',
        ENV: 'v1',
        ENV2: 'v2',
        ENV3: 'v3',
        ENV4: 'v4',
        ENV5: 'v5',
      })
    })

    describe('load()', () => {
      const file1 = tempfile('.env.1', 'ENV1=value1\nENV2=value2')
      const file2 = tempfile('.env.2', 'ENV2=value222\nENV3=value3')
      after(() => Promise.all([fs.remove(file1), fs.remove(file2)]))

      test('loads env from files', () => {
        const env = dotenv.load(file1, file2)
        assert.equal(env.ENV1, 'value1')
        assert.equal(env.ENV2, 'value2')
        assert.equal(env.ENV3, 'value3')
      })

      test('throws error on ENOENT', () => {
        try {
          dotenv.load('./.env')
          throw new Error('unreachable')
        } catch (e) {
          assert.equal(e.code, 'ENOENT')
          assert.equal(e.errno, -2)
        }
      })
    })

    describe('loadSafe()', () => {
      const file1 = tempfile('.env.1', 'ENV1=value1\nENV2=value2')
      const file2 = '.env.notexists'

      after(() => fs.remove(file1))

      test('loads env from files', () => {
        const env = dotenv.loadSafe(file1, file2)
        assert.equal(env.ENV1, 'value1')
        assert.equal(env.ENV2, 'value2')
      })
    })

    describe('config()', () => {
      test('updates process.env', () => {
        const file1 = tempfile('.env.1', 'ENV1=value1')

        assert.equal(process.env.ENV1, undefined)
        dotenv.config(file1)
        assert.equal(process.env.ENV1, 'value1')
        delete process.env.ENV1
      })
    })
  })

  describe('temp*', () => {
    test('tempdir() creates temporary folders', () => {
      assert.equal(tmpdir, tempdir)
      assert.match(tempdir(), /\/zx-/)
      assert.match(tempdir('foo'), /\/foo$/)
    })

    test('tempfile() creates temporary files', () => {
      assert.equal(tmpfile, tempfile)
      assert.match(tempfile(), /\/zx-.+/)
      assert.match(tempfile('foo.txt'), /\/zx-.+\/foo\.txt$/)

      const tf = tempfile('bar.txt', 'bar')
      assert.match(tf, /\/zx-.+\/bar\.txt$/)
      assert.equal(fs.readFileSync(tf, 'utf-8'), 'bar')
    })
  })

  describe('versions', () => {
    test('exports deps versions', () => {
      assert.deepEqual(
        Object.keys(versions).sort(),
        [
          'chalk',
          'depseek',
          'dotenv',
          'fetch',
          'fs',
          'glob',
          'minimist',
          'ps',
          'which',
          'yaml',
          'zx',
        ].sort()
      )
    })
  })

  describe('chunk()', () => {
    test('splits array into chunks', () => {
      assert.deepEqual(chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]])
      assert.deepEqual(chunk([1, 2, 3, 4], 2), [
        [1, 2],
        [3, 4],
      ])
      assert.deepEqual(chunk([1, 2], 5), [[1, 2]])
      assert.deepEqual(chunk([], 2), [])
    })

    test('default size is 1', () => {
      assert.deepEqual(chunk([1, 2, 3]), [[1], [2], [3]])
    })

    test('throws on invalid size', () => {
      assert.throws(() => chunk([1, 2], 0), /Chunk size must be at least 1/)
      assert.throws(() => chunk([1, 2], -1), /Chunk size must be at least 1/)
    })
  })

  describe('pool()', () => {
    test('processes all items with order preserved', async () => {
      const items = [10, 50, 20, 40, 30]
      const results = await pool(
        items,
        async (item, index) => {
          await sleep(item)
          return item * 2 + index
        },
        3
      )
      assert.deepEqual(results, [20, 101, 42, 83, 64])
    })

    test('respects concurrency limit', async () => {
      let active = 0
      let maxActive = 0
      const items = [1, 2, 3, 4, 5, 6]

      await pool(
        items,
        async () => {
          active++
          maxActive = Math.max(maxActive, active)
          await sleep(20)
          active--
        },
        2
      )

      assert.equal(maxActive, 2)
      assert.equal(active, 0)
    })

    test('fast worker immediately picks up work from queue', async () => {
      const startTimes: number[] = []
      const items = [100, 10, 10] // worker 1 takes 100ms, worker 2 takes 10ms, then should immediately pick up item 3
      const t0 = Date.now()

      await pool(
        items,
        async (duration, i) => {
          startTimes[i] = Date.now() - t0
          await sleep(duration)
        },
        2
      )

      // Item 2 (third item) should start around ~10-20ms (as soon as worker 2 finishes item 1),
      // well before worker 1 finishes item 0 (~100ms).
      assert.ok(
        startTimes[2] < 60,
        `Expected item 2 to start early, started at ${startTimes[2]}ms`
      )
    })

    test('supports options object', async () => {
      const results = await pool([1, 2, 3], (x) => x * 10, { concurrency: 2 })
      assert.deepEqual(results, [10, 20, 30])
    })

    test('supports pool(concurrency, items, callback) argument order', async () => {
      const results = await pool(2, [1, 2, 3], (x) => x * 3)
      assert.deepEqual(results, [3, 6, 9])
    })

    test('supports default concurrency when omitted', async () => {
      const results = await pool([1, 2, 3], (x) => x + 1)
      assert.deepEqual(results, [2, 3, 4])
    })

    test('supports Iterable inputs like Set and Generator', async () => {
      const set = new Set(['a', 'b', 'c'])
      const results1 = await pool(set, (x) => x.toUpperCase(), 2)
      assert.deepEqual(results1, ['A', 'B', 'C'])

      function* gen() {
        yield 1
        yield 2
        yield 3
      }
      const results2 = await pool(gen(), (x) => x * 2, 2)
      assert.deepEqual(results2, [2, 4, 6])
    })

    test('supports AsyncIterable inputs', async () => {
      async function* asyncGen() {
        yield 10
        yield 20
        yield 30
      }
      const results = await pool(asyncGen(), (x) => x / 10, 2)
      assert.deepEqual(results, [1, 2, 3])
    })

    test('returns empty array for empty inputs', async () => {
      const results = await pool([], () => 123)
      assert.deepEqual(results, [])
    })

    test('stops queue and rejects if a task throws', async () => {
      const executed: number[] = []
      try {
        await pool(
          [1, 2, 3, 4, 5, 6],
          async (x) => {
            executed.push(x)
            if (x === 2) throw new Error('task-failed')
            await sleep(50)
          },
          2
        )
        assert.fail('should have thrown')
      } catch (err: any) {
        assert.equal(err.message, 'task-failed')
      }
      // Workers should not have processed subsequent queued items
      assert.ok(!executed.includes(5))
      assert.ok(!executed.includes(6))
    })

    test('throws on invalid concurrency', () => {
      assert.throws(
        () => pool([], () => {}, 0),
        /Pool concurrency must be at least 1/
      )
      assert.throws(
        () => pool([], () => {}, -1),
        /Pool concurrency must be at least 1/
      )
    })

    describe('WorkerPool instance', () => {
      test('add() and callable execution with concurrency control', async () => {
        const p = pool(2)
        assert.equal(p.concurrency, 2)
        assert.equal(p.size, 0)
        assert.equal(p.pending, 0)

        let active = 0
        let maxActive = 0
        const makeTask = (val: number) => async () => {
          active++
          maxActive = Math.max(maxActive, active)
          await sleep(20)
          active--
          return val
        }

        const task1 = p.add(makeTask(1))
        const task2 = p(makeTask(2))
        const task3 = p.add(makeTask(3))

        const results = await Promise.all([task1, task2, task3])
        assert.deepEqual(results, [1, 2, 3])
        assert.equal(maxActive, 2)
        assert.equal(p.pending, 0)
      })

      test('drain() / await p', async () => {
        const p = pool(2)
        const completed: number[] = []

        p.add(async () => {
          await sleep(20)
          completed.push(1)
        })
        p.add(async () => {
          await sleep(10)
          completed.push(2)
        })

        await p
        assert.equal(completed.length, 2)
      })

      test('clear() clears queued tasks', async () => {
        const p = pool(1)
        let ran = 0

        p.add(async () => {
          await sleep(50)
          ran++
        })
        p.add(async () => {
          ran++
        })

        assert.equal(p.size, 1)
        p.clear()
        assert.equal(p.size, 0)

        await p
        assert.equal(ran, 1)
      })

      test('p.map() runs items', async () => {
        const p = pool(3)
        const results = await p.map([1, 2, 3], (x) => x * 100)
        assert.deepEqual(results, [100, 200, 300])
      })
    })
  })
})
