import { spawn, type ChildProcess } from 'node:child_process'
import { createServer, type ServerResponse } from 'node:http'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { afterEach, beforeEach, describe, expect, test } from 'vitest'

// These exercise actual process crashes and wire behavior, beyond the shared
// protocol suite. Build with --features subscriptions before enabling them.
describe.runIf(process.env.RUST_SERVER_SUBSCRIPTIONS === `1`)(
  `Durable subscription recovery`,
  () => {
    let child: ChildProcess | null = null
    let directory: string
    let base: string
    let port: number
    let output = ``
    const cleanup: Array<() => Promise<void>> = []
    const binary =
      process.env.RUST_SERVER_BINARY ??
      path.resolve(__dirname, `../target/release/durable-streams-server`)

    async function stop() {
      if (!child || child.exitCode !== null || child.signalCode !== null) return
      const exited = new Promise<void>((resolve) =>
        child!.once(`exit`, () => resolve())
      )
      child.kill(`SIGKILL`)
      await exited
      child = null
    }

    async function start(subscriptions = true, extraArgs: string[] = []) {
      output = ``
      child = spawn(
        binary,
        [
          `--port`,
          String(port),
          `--data-dir`,
          directory,
          `--worker-threads`,
          `2`,
          `--wal-shards`,
          `2`,
          `--wal-segment-bytes`,
          `1048576`,
          ...(subscriptions ? [`--subscriptions`] : []),
          ...extraArgs,
        ],
        { stdio: [`ignore`, `pipe`, `pipe`] }
      )
      child.stdout!.on(`data`, (data) => {
        output += String(data)
      })
      child.stderr!.on(`data`, (data) => {
        output += String(data)
      })
      await until(async () => {
        if (child!.exitCode !== null) throw new Error(output)
        try {
          return (await fetch(`${base}/health`)).ok
        } catch {
          return false
        }
      })
    }

    beforeEach(async () => {
      directory = await mkdtemp(path.join(tmpdir(), `ds-sub-recovery-`))
      const listener = createServer()
      await new Promise<void>((resolve) =>
        listener.listen(0, `127.0.0.1`, resolve)
      )
      port = (listener.address() as { port: number }).port
      await new Promise<void>((resolve) => listener.close(() => resolve()))
      base = `http://127.0.0.1:${port}`
      await start()
    })

    afterEach(async () => {
      await stop()
      for (const close of cleanup.splice(0)) await close()
      await rm(directory, { recursive: true, force: true })
    })

    const stream = (name: string) => `${base}/v1/stream/${name}`
    const subscription = (id = `test`) =>
      stream(`__ds/subscriptions/${encodeURIComponent(id)}`)
    async function put(name: string, data: unknown = []) {
      const reply = await fetch(stream(name), {
        method: `PUT`,
        headers: { 'content-type': `application/json` },
        body: JSON.stringify(data),
      })
      expect(reply.status, await reply.text()).toBe(201)
    }
    async function append(name: string, data: unknown) {
      const reply = await fetch(stream(name), {
        method: `POST`,
        headers: { 'content-type': `application/json` },
        body: JSON.stringify(data),
      })
      expect(reply.status, await reply.text()).toBe(204)
      return reply.headers.get(`stream-next-offset`)!
    }
    async function create(input: unknown, id = `test`) {
      return fetch(subscription(id), {
        method: `PUT`,
        headers: { 'content-type': `application/json` },
        body: JSON.stringify(input),
      })
    }
    async function pull(ttl = 10_000) {
      await put(`wake/pool`)
      expect(
        (
          await create({
            type: `pull-wake`,
            pattern: `events/**`,
            wake_stream: `wake/pool`,
            lease_ttl_ms: ttl,
          })
        ).status
      ).toBe(201)
    }
    async function claim(worker = `worker`) {
      return fetch(`${subscription()}/claim`, {
        method: `POST`,
        headers: { 'content-type': `application/json` },
        body: JSON.stringify({ worker }),
      })
    }
    async function acknowledge(
      wake: any,
      acks: unknown[],
      done = true,
      token = wake.token
    ) {
      return fetch(`${subscription()}/ack`, {
        method: `POST`,
        headers: {
          'content-type': `application/json`,
          authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          wake_id: wake.wake_id,
          generation: wake.generation,
          acks,
          done,
        }),
      })
    }
    async function receiver(initialStatus = 200, initialDone = false) {
      const records: Array<{ body: any; raw: string; signature: string }> = []
      const state = { status: initialStatus, done: initialDone, hold: false }
      const held: ServerResponse[] = []
      const server = createServer(async (request, response) => {
        const chunks: Buffer[] = []
        for await (const chunk of request) chunks.push(Buffer.from(chunk))
        const raw = Buffer.concat(chunks).toString()
        records.push({
          body: JSON.parse(raw),
          raw,
          signature: String(request.headers[`webhook-signature`]),
        })
        if (state.hold) held.push(response)
        else {
          response.writeHead(state.status, {
            'content-type': `application/json`,
          })
          response.end(JSON.stringify({ done: state.done }))
        }
      })
      await new Promise<void>((resolve) =>
        server.listen(0, `127.0.0.1`, resolve)
      )
      cleanup.push(async () => {
        for (const response of held) response.destroy()
        server.closeAllConnections()
        await new Promise<void>((resolve) => server.close(() => resolve()))
      })
      return {
        records,
        state,
        url: `http://127.0.0.1:${(server.address() as { port: number }).port}/hook`,
        release() {
          for (const response of held.splice(0)) {
            response.writeHead(200, { 'content-type': `application/json` })
            response.end(`{"done":true}`)
          }
        },
      }
    }

    test(`persists acknowledged cursors and signing identity through SIGKILL`, async () => {
      await pull()
      const keys = await (await fetch(stream(`__ds/jwks.json`))).json()
      await put(`events/a`, { n: 1 })
      const claimed = await (await claim()).json()
      const offset = claimed.streams.find(
        (item: any) => item.path === `events/a`
      ).tail_offset
      expect(
        (await acknowledge(claimed, [{ stream: `events/a`, offset }])).status
      ).toBe(200)
      await stop()
      await start()
      expect(await (await fetch(stream(`__ds/jwks.json`))).json()).toEqual(keys)
      const saved = await (await fetch(subscription())).json()
      expect(saved.streams).toContainEqual({
        path: `events/a`,
        link_type: `glob`,
        acked_offset: offset,
      })
      expect((await claim()).status).toBe(409)
      await append(`events/a`, { n: 2 })
      expect((await claim()).status).toBe(200)
    })

    test(`recovers an append that occurred while the control plane was stopped`, async () => {
      await pull()
      await stop()
      await start(false)
      await put(`events/crash-gap`, { committed: true })
      await stop()
      await start()
      await until(
        async () => (await (await fetch(stream(`wake/pool`))).json()).length > 0
      )
      const reply = await claim()
      expect(reply.status).toBe(200)
      expect((await reply.json()).streams).toContainEqual(
        expect.objectContaining({ path: `events/crash-gap`, has_pending: true })
      )
    })

    test(`preserves a live lease and token across restart`, async () => {
      await pull()
      await put(`events/a`, { n: 1 })
      const claimed = await (await claim(`owner`)).json()
      await stop()
      await start()
      const busy = await claim(`other`)
      expect(busy.status).toBe(409)
      expect((await busy.json()).error.current_holder).toBe(`owner`)
      expect(
        (
          await acknowledge(claimed, [
            { stream: `events/a`, offset: claimed.streams[0].tail_offset },
          ])
        ).status
      ).toBe(200)
    })

    test(`expires persisted leases and fences a pre-crash worker`, async () => {
      await pull(1000)
      await put(`events/a`, { n: 1 })
      const old = await (await claim()).json()
      await stop()
      await new Promise((resolve) => setTimeout(resolve, 1200))
      await start()
      const next = await (await claim(`new-owner`)).json()
      expect(next.generation).toBeGreaterThan(old.generation)
      const rejected = await acknowledge(old, [])
      expect(rejected.status).toBe(409)
      expect((await rejected.json()).error.code).toBe(`FENCED`)
    })

    test(`retains webhook retry state and the same wake across restart`, async () => {
      const sink = await receiver(503)
      await create({
        type: `webhook`,
        pattern: `events/**`,
        webhook: { url: sink.url },
      })
      await put(`events/retry`, { n: 1 })
      await until(
        async () =>
          (await (await fetch(subscription())).json()).status === `failed`
      )
      const first = sink.records[0].body
      await stop()
      sink.state.status = 200
      sink.state.done = true
      await start()
      await until(async () => sink.records.length >= 2)
      expect(sink.records[1].body.wake_id).toBe(first.wake_id)
      expect(sink.records[1].body.generation).toBe(first.generation)
      await until(async () => {
        const saved = await (await fetch(subscription())).json()
        return saved.streams[0]?.acked_offset === first.streams[0].tail_offset
      })
    })

    test(`auto-acks only the delivered snapshot when more data arrives`, async () => {
      const sink = await receiver(200, true)
      sink.state.hold = true
      await create({
        type: `webhook`,
        pattern: `events/**`,
        webhook: { url: sink.url },
      })
      await put(`events/a`, { n: 1 })
      await until(async () => sink.records.length === 1)
      const first = sink.records[0].body
      const secondOffset = await append(`events/a`, { n: 2 })
      sink.state.hold = false
      sink.release()
      await until(async () => sink.records.length >= 2)
      const second = sink.records[1].body
      expect(second.generation).toBeGreaterThan(first.generation)
      expect(second.streams[0].tail_offset).toBe(secondOffset)
      expect(second.streams[0].acked_offset).toBe(first.streams[0].tail_offset)
    })

    test(`late webhook completion cannot acknowledge a recreated stream`, async () => {
      const sink = await receiver(200, true)
      sink.state.hold = true
      await create({
        type: `webhook`,
        pattern: `events/**`,
        webhook: { url: sink.url },
      })
      await put(`events/a`, {
        text: `a much longer previous stream incarnation`,
      })
      await until(async () => sink.records.length === 1)
      const old = sink.records[0].body
      expect(
        (await fetch(stream(`events/a`), { method: `DELETE` })).status
      ).toBe(204)
      await put(`events/a`, { n: 2 })
      await until(async () => sink.records.length >= 2)
      const next = sink.records[1].body
      expect(next.generation).toBeGreaterThan(old.generation)
      expect(next.streams[0].acked_offset).toBe(
        `0000000000000000_0000000000000000`
      )
      sink.state.hold = false
      sink.release()
      await until(async () => {
        const saved = await (await fetch(subscription())).json()
        return saved.streams[0]?.acked_offset === next.streams[0].tail_offset
      })
    })

    test(`uses the configured public stream root and serves an unambiguous JWKS response`, async () => {
      await stop()
      await start(true, [
        `--public-url`,
        `https://streams.example.com/custom-root`,
      ])
      const sink = await receiver()
      const reply = await create(
        { type: `webhook`, pattern: `events/**`, webhook: { url: sink.url } },
        `nested/id-one`
      )
      expect(reply.status).toBe(201)
      const body = await reply.json()
      expect(body.id).toBe(`nested/id-one`)
      expect(body.webhook.signing.jwks_url).toBe(
        `https://streams.example.com/custom-root/__ds/jwks.json`
      )
      const keys = await fetch(stream(`__ds/jwks.json`))
      expect(keys.headers.get(`content-type`)).toBe(`application/jwk-set+json`)
      expect(keys.headers.get(`cache-control`)).toBe(`public, max-age=300`)
    })

    test(`validates every cursor before committing any partial acknowledgement`, async () => {
      await pull()
      await put(`events/a`, { n: 1 })
      const claimed = await (await claim()).json()
      const reply = await acknowledge(claimed, [
        { stream: `events/a`, offset: claimed.streams[0].tail_offset },
        { stream: `events/unknown`, offset: claimed.streams[0].tail_offset },
      ])
      expect(reply.status).toBe(409)
      const saved = await (await fetch(subscription())).json()
      expect(saved.streams[0].acked_offset).not.toBe(
        claimed.streams[0].tail_offset
      )
      expect(
        (await acknowledge(claimed, [], false, `wrong-token`)).status
      ).toBe(401)
    })

    test(`does not let an old wake acknowledge a deleted and recreated stream`, async () => {
      await pull()
      await put(`events/a`, { text: `long first incarnation` })
      const old = await (await claim()).json()
      expect(
        (await fetch(stream(`events/a`), { method: `DELETE` })).status
      ).toBe(204)
      await put(`events/a`, { n: 2 })
      const rejected = await acknowledge(old, [
        { stream: `events/a`, offset: old.streams[0].tail_offset },
      ])
      expect(rejected.status).toBe(409)
      expect((await rejected.json()).error.code).toBe(`FENCED`)
      const next = await (await claim()).json()
      expect(next.streams[0].has_pending).toBe(true)
    })

    test(`backfills patterns at the current tail and retains glob membership after explicit removal`, async () => {
      await put(`events/existing`, { old: true })
      await pull()
      expect((await claim()).status).toBe(409)
      const add = await fetch(`${subscription()}/streams`, {
        method: `POST`,
        headers: { 'content-type': `application/json` },
        body: JSON.stringify({ streams: [`events/existing`] }),
      })
      expect(add.status).toBe(204)
      const remove = await fetch(
        `${subscription()}/streams/${encodeURIComponent(`events/existing`)}`,
        { method: `DELETE` }
      )
      expect(remove.status).toBe(204)
      const saved = await (await fetch(subscription())).json()
      expect(saved.streams[0].link_type).toBe(`glob`)
      await append(`events/existing`, { new: true })
      expect((await claim()).status).toBe(200)
    })

    test.each([
      [`events/**/main`, `events/main`, true],
      [`events/**/main`, `events/a/b/main`, true],
      [`events/*/main`, `events/a/b/main`, false],
      [`events/a*`, `events/abc`, false],
    ])(
      `matches subscription path segments: %s against %s`,
      async (pattern, streamPath, matches) => {
        await put(`wake/pool`)
        expect(
          (
            await create({
              type: `pull-wake`,
              pattern,
              wake_stream: `wake/pool`,
            })
          ).status
        ).toBe(201)
        await put(String(streamPath), { n: 1 })
        const reply = await claim()
        expect(reply.status).toBe(matches ? 200 : 409)
      }
    )

    test(`rejects conflicting configuration, malformed bodies, reserved paths, and unsafe URLs`, async () => {
      await pull()
      const conflict = await create({
        type: `pull-wake`,
        streams: [`other`],
        wake_stream: `wake/pool`,
      })
      expect(conflict.status).toBe(409)
      expect((await conflict.json()).error.code).toBe(
        `SUBSCRIPTION_ALREADY_EXISTS`
      )
      for (const url of [
        `http://10.0.0.1/hook`,
        `https://127.0.0.1/hook`,
        `https://[::ffff:127.0.0.1]/hook`,
        `https://user:pass@example.com/hook`,
      ]) {
        expect(
          (
            await create(
              { type: `webhook`, pattern: `**`, webhook: { url } },
              `unsafe`
            )
          ).status
        ).toBe(400)
      }
      expect(
        (
          await create(
            {
              type: `pull-wake`,
              streams: [`__ds/jwks.json`],
              wake_stream: `wake/pool`,
            },
            `reserved`
          )
        ).status
      ).toBe(400)
      expect(
        (await fetch(subscription(), { method: `POST`, body: `not-json` }))
          .status
      ).toBe(400)
      expect(
        (await fetch(stream(`__ds/missing`), { method: `PUT` })).status
      ).toBe(404)
    })
  }
)

async function until(condition: () => Promise<boolean>, timeout = 10_000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await condition()) return
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error(`condition did not become true within ${timeout} ms`)
}
