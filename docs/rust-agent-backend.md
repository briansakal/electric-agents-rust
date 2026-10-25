# Rust Streams backend for Electric Agents

The Rust server can provide both stream storage and the Durable Streams subscription
control plane. The Electric Agents coordinator remains a separate Node process with
Postgres and Electric Sync. Agent handlers can run elsewhere, including your web app.

## Build and run

From the repository root:

```sh
cargo build --release --manifest-path packages/durable-streams-rust/Cargo.toml \
  --features subscriptions

packages/durable-streams-rust/target/release/durable-streams-server \
  --host 0.0.0.0 --port 4438 \
  --data-dir /var/lib/durable-streams \
  --subscriptions \
  --public-url https://streams.example.com/v1/stream
```

`--subscriptions` requires a build with the `subscriptions` Cargo feature and WAL
durability (the default). Memory durability is rejected because a persisted
subscription cursor must never acknowledge stream bytes that a crash can discard.
Without the feature, stream operations retain their existing implementation and
the subscription dependencies are not compiled.

`--public-url` is the external stream-root URL used in webhook callback and JWKS
URLs. Preserve that path when forwarding requests to the server. Without this
option the server derives an HTTP origin from the request's Host header, suitable
for local development. It does not trust forwarded headers. In production place
the endpoint behind TLS and an authenticating/authorizing proxy; subscription
endpoints, like the existing stream endpoints, do not implement user authentication.

Point the Agents coordinator at the same stream root:

```sh
ELECTRIC_AGENTS_DURABLE_STREAMS_URL=https://streams.example.com/v1/stream
```

That setting replaces the coordinator's embedded Node Streams server. It does not
replace the coordinator itself or its Postgres/Electric configuration.

To build a container with the feature enabled:

```sh
docker build -f packages/durable-streams-rust/Dockerfile \
  --build-arg CARGO_FEATURES=subscriptions -t rust-agent-streams .
docker run --rm -p 4438:4438 -v rust-streams-data:/data rust-agent-streams \
  --host 0.0.0.0 --port 4438 --data-dir /data --subscriptions \
  --public-url https://streams.example.com/v1/stream
```

## Persistent state and delivery

The existing stream files and write-ahead log store stream contents. The new
`<data-dir>/subscriptions/control.sqlite3` database stores subscription definitions,
explicit/glob membership, acknowledged offsets, wake generations, immutable delivery
snapshots, worker leases, retry schedules, and the Ed25519 signing identity. Its
directory is private to the owner on Unix. SQLite uses WAL journaling and FULL
synchronous commits. Keep the whole data directory on persistent storage and use
SQLite-aware or crash-consistent volume backups.

Control-plane mutations commit before the HTTP response or delivery side effect.
Outbound network requests do not hold SQLite locks. Webhooks carry Ed25519
signatures; public keys are available at `<stream-root>/__ds/jwks.json`. Callbacks
and pull-wake operations use random, subscription/wake-scoped bearer tokens and
generation fencing. Signing identity and active tokens survive process restart.

Appends notify the reconciler after the ordinary durable append path completes.
Notifications are coalesced in a bounded set. Startup and overflow reconciliation
scan stream metadata, so a committed append whose notification was lost in a
crash is discovered. Routine discovery checks changed paths rather than rescanning
every stream. A 100 ms loop also checks linked durable tails, lease deadlines, and
retry schedules. Newly created pattern subscriptions start existing streams at
their current tail; streams discovered later start at offset zero.

Webhook delivery is at least once, with exponential backoff and jitter. A restart
can redeliver the same wake; receivers should handle duplicates. A synchronous
`{"done":true}` response acknowledges only the persisted wake snapshot. Data
appended later remains pending and receives another wake. Pull-wake notifications
are appended through the existing Rust WAL path, with an idempotent producer per
subscription incarnation. Old generations are fenced after lease expiration or
stream deletion/recreation.

Webhook URL validation permits public HTTPS destinations and the protocol's local
HTTP development exception. Delivery validates DNS answers and pins them for that
request; redirects and environment proxies are disabled.

This is a single-node implementation: run one server process per data directory.
It does not add replication or multi-node coordination. Subscription processing
has a bounded 32-delivery concurrency limit; control-plane throughput has not been
benchmarked against the original stream-only kernel-speed results. Linked-stream
and subscription state still require periodic reconciliation work.

## Validation

Install workspace dependencies from the repository root before running pnpm tests:

```sh
pnpm install --filter @electric-ax/durable-streams-server-rust... --frozen-lockfile
cargo test --manifest-path packages/durable-streams-rust/Cargo.toml --features subscriptions

RUST_SERVER_SUBSCRIPTIONS=1 pnpm -C packages/durable-streams-rust test:conformance
```

Build the release binary first, or set `RUST_SERVER_BINARY` to an absolute binary
path. The shared protocol suite enables its subscription tests when
`RUST_SERVER_SUBSCRIPTIONS=1`; additional tests kill/restart the real Rust process
and verify cursor, lease, retry, signing-key, and missed-notification recovery.

The optional `test:agents` integration uses the real coordinator, Postgres, Electric,
and a mock model. Build the sibling Agents packages first (see
[Agents development](agents-development.md)), then run against a dedicated database:

```sh
DATABASE_URL=postgres://electric_agents:electric_agents@localhost:55438/electric_agents \
ELECTRIC_URL=http://localhost:53068 \
pnpm -C packages/durable-streams-rust test:agents
```

The integration creates a coordinator service named `rust-subscription-integration`
and runs the existing mock-agent end-to-end tests. It does not call an LLM provider.

Implementation: [subscriptions.rs](../packages/durable-streams-rust/src/subscriptions.rs).
