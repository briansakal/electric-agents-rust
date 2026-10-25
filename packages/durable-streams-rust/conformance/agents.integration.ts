import { spawn, type ChildProcess } from 'node:child_process'
import { createServer } from 'node:net'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { beforeAll, afterAll, describe } from 'vitest'
import { ElectricAgentsServer } from '../../agents-server/src/server'
import {
  createMockStreamFn,
  runMockAgentTests,
} from '../../agents-server-conformance-tests/src/index'

// Opt-in integration: requires the sibling Agents packages to be built and a
// dedicated Postgres + Electric backend. No model credentials or LLM calls.
describe(`Electric Agents with the Rust subscription backend`, () => {
  let child: ChildProcess | undefined
  let server: ElectricAgentsServer | undefined
  let directory: string | undefined
  const config = { baseUrl: `` }

  beforeAll(async () => {
    const reservation = createServer()
    await new Promise<void>((resolve) =>
      reservation.listen(0, `127.0.0.1`, resolve)
    )
    const port = (reservation.address() as { port: number }).port
    await new Promise<void>((resolve) => reservation.close(() => resolve()))
    directory = await mkdtemp(path.join(tmpdir(), `rust-agents-integration-`))
    const binary =
      process.env.RUST_SERVER_BINARY ??
      path.resolve(__dirname, `../target/release/durable-streams-server`)
    child = spawn(
      binary,
      [
        `--port`,
        String(port),
        `--data-dir`,
        directory,
        `--subscriptions`,
        `--worker-threads`,
        `2`,
        `--wal-shards`,
        `2`,
        `--wal-segment-bytes`,
        `1048576`,
      ],
      { stdio: [`ignore`, `ignore`, `pipe`] }
    )
    let error = ``
    child.stderr!.on(`data`, (data) => {
      error += String(data)
    })
    const origin = `http://127.0.0.1:${port}`
    const deadline = Date.now() + 10_000
    for (;;) {
      if (child.exitCode !== null) throw new Error(error)
      try {
        if ((await fetch(`${origin}/health`)).ok) break
      } catch {
        /* starting */
      }
      if (Date.now() > deadline)
        throw new Error(`Rust backend did not start: ${error}`)
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    server = new ElectricAgentsServer({
      port: 0,
      service: `rust-subscription-integration`,
      durableStreamsUrl: `${origin}/v1/stream`,
      postgresUrl:
        process.env.DATABASE_URL ??
        `postgres://electric_agents:electric_agents@localhost:55438/electric_agents`,
      electricUrl: process.env.ELECTRIC_URL ?? `http://localhost:53068`,
      mockStreamFn: createMockStreamFn(
        `This response was persisted by the Rust backend.`
      ),
    })
    config.baseUrl = await server.start()
  })

  afterAll(async () => {
    await server?.stop()
    if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>((resolve) =>
        child!.once(`exit`, () => resolve())
      )
      child.kill(`SIGKILL`)
      await exited
    }
    if (directory) await rm(directory, { recursive: true, force: true })
  })

  runMockAgentTests(config)
})
