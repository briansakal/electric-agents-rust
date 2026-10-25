//! Durable, single-node subscription control plane. Stream bytes stay in Store;
//! SQLite commits cursor/lease transitions before any delivery side effect.
//! Reconciliation reads durable tails, so an append-to-notification crash cannot
//! strand work. Network requests never hold the database lock.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use bytes::Bytes;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{Ed25519KeyPair, KeyPair};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tokio::sync::Notify;

use crate::api::{Body, Method, Req, Resp};
use crate::store::{format_offset, Store};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
const POLL: Duration = Duration::from_millis(100);
const MAX_CONTROL_BODY: usize = 1024 * 1024;
const MAX_STREAMS: usize = 10_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Config {
    #[serde(rename = "type")]
    kind: String,
    pattern: Option<String>,
    streams: Vec<String>,
    webhook: Option<String>,
    wake_stream: Option<String>,
    lease_ttl_ms: u64,
    description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Link {
    acked_offset: String,
    explicit: bool,
    glob: bool,
    // Detect expiry/deletion/recreation, including while this server was down.
    stream_id: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamInfo {
    path: String,
    link_type: String,
    acked_offset: String,
    tail_offset: String,
    has_pending: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Wake {
    id: String,
    token: String,
    snapshot: Vec<StreamInfo>,
    lease_until: u64,
    holder: Option<String>,
    delivery_pending: bool,
    next_attempt_at: u64,
    retry_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Subscription {
    id: String,
    root: String,
    control_base: String,
    incarnation: String,
    config: Config,
    created_at: String,
    links: BTreeMap<String, Link>,
    generation: u64,
    wake: Option<Wake>,
}

pub struct Subscriptions {
    db: Mutex<Connection>,
    store: Weak<Store>,
    public_url: Option<String>,
    signing_key: Ed25519KeyPair,
    kid: String,
    changed: Notify,
    dirty_paths: Mutex<BTreeSet<String>>,
    full_scan: AtomicBool,
    stopping: AtomicBool,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone)]
struct Delivery {
    subscription: Subscription,
}

impl Subscriptions {
    pub fn open(store: &Arc<Store>, public_url: Option<String>) -> Result<Arc<Self>> {
        if let Some(base) = &public_url {
            let url = reqwest::Url::parse(base)?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err("--public-url must be an absolute HTTP(S) stream-root URL".into());
            }
        }
        let directory = store.data_dir.join("subscriptions");
        std::fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let db = Connection::open(directory.join("control.sqlite3"))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS subscriptions (
                root TEXT NOT NULL, id TEXT NOT NULL, body TEXT NOT NULL,
                PRIMARY KEY (root, id));",
        )?;
        let key: Option<Vec<u8>> = db
            .query_row(
                "SELECT value FROM metadata WHERE key='ed25519'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let key = match key {
            Some(key) => key,
            None => {
                let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                    .map_err(|_| "signing key generation failed")?
                    .as_ref()
                    .to_vec();
                db.execute("INSERT INTO metadata VALUES ('ed25519', ?1)", [&key])?;
                // Persist the directory entry as well as SQLite's contents.
                std::fs::File::open(&directory)?.sync_all()?;
                key
            }
        };
        let signing_key =
            Ed25519KeyPair::from_pkcs8(&key).map_err(|_| "invalid persisted Ed25519 key")?;
        let kid = format!(
            "ds_{}",
            B64.encode(
                ring::digest::digest(&ring::digest::SHA256, signing_key.public_key().as_ref())
                    .as_ref()
            )
        );
        Ok(Arc::new(Self {
            db: Mutex::new(db),
            store: Arc::downgrade(store),
            public_url,
            signing_key,
            kid,
            changed: Notify::new(),
            dirty_paths: Mutex::new(BTreeSet::new()),
            full_scan: AtomicBool::new(true),
            stopping: AtomicBool::new(false),
            task: Mutex::new(None),
        }))
    }

    pub fn start(self: &Arc<Self>) {
        let service = Arc::clone(self);
        *self.task.lock().unwrap() = Some(tokio::spawn(async move {
            service.run().await;
        }));
    }

    pub fn notify(&self) {
        self.changed.notify_one();
    }

    pub fn stream_changed(&self, path: String) {
        let mut paths = self.dirty_paths.lock().unwrap();
        if paths.len() < 4096 {
            paths.insert(path);
        } else {
            self.full_scan.store(true, Ordering::Release);
        }
        drop(paths);
        self.notify();
    }

    pub async fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        self.notify();
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    // All SQLite I/O (including fsync) runs on the blocking pool. No guard is
    // held across an await or an outbound HTTP request.
    async fn transact<T, F>(self: &Arc<Self>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Transaction<'_>, &Store) -> Result<T> + Send + 'static,
    {
        let service = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let store = service.store.upgrade().ok_or("stream store stopped")?;
            let mut connection = service
                .db
                .lock()
                .map_err(|_| "subscription database poisoned")?;
            let tx = connection.transaction()?;
            let output = f(&tx, &store)?;
            tx.commit()?;
            Ok(output)
        })
        .await?
    }

    pub async fn handle(self: &Arc<Self>, request: Req) -> Resp {
        if request.body.len() > MAX_CONTROL_BODY {
            return failure(413, "INVALID_REQUEST", "control body too large");
        }
        let Some((root, suffix)) = request.path.split_once("/__ds") else {
            return failure(404, "NOT_FOUND", "unknown control route");
        };
        let root = root.to_string();
        let suffix = suffix.to_string();
        if request.method == Method::Options {
            return response(204, Value::Null);
        }
        if suffix == "/jwks.json" {
            if request.method != Method::Get {
                return method_not_allowed();
            }
            let mut reply = response(
                200,
                json!({"keys": [{"kty":"OKP", "crv":"Ed25519",
                "alg":"EdDSA", "use":"sig", "kid":self.kid,
                "x": B64.encode(self.signing_key.public_key().as_ref())}]}),
            );
            reply
                .headers
                .retain(|(name, _)| !matches!(*name, "content-type" | "cache-control"));
            reply
                .headers
                .push(("content-type", "application/jwk-set+json".into()));
            reply
                .headers
                .push(("cache-control", "public, max-age=300".into()));
            return reply;
        }
        let Some(rest) = suffix.strip_prefix("/subscriptions/") else {
            return failure(404, "NOT_FOUND", "unknown control route");
        };
        let (encoded_id, action) = rest.split_once('/').unwrap_or((rest, ""));
        let Ok(id) = percent_encoding::percent_decode_str(encoded_id).decode_utf8() else {
            return failure(400, "INVALID_REQUEST", "invalid subscription id");
        };
        if id.is_empty() || id.len() > 1024 {
            return failure(400, "INVALID_REQUEST", "invalid subscription id");
        }
        let id = id.into_owned();
        let action = action.to_string();
        let input = if request.body.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice::<Value>(&request.body) {
                Ok(value) if value.is_object() => value,
                _ => return failure(400, "INVALID_REQUEST", "expected a JSON object"),
            }
        };
        let method = request.method;
        let token = request
            .header("authorization")
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(str::to_owned);
        let control_base = if let Some(base) = &self.public_url {
            format!("{}/__ds", base.trim_end_matches('/'))
        } else {
            let host = request.header("host").unwrap_or("127.0.0.1:4437");
            if host
                .bytes()
                .any(|c| c.is_ascii_control() || c == b'/' || c == b'\\' || c == b'@')
            {
                return failure(400, "INVALID_REQUEST", "invalid Host header");
            }
            format!("http://{host}{root}/__ds")
        };
        let kid = self.kid.clone();
        let result = self.transact(move |tx, store| {
            if action.is_empty() && method == Method::Put {
                let config = match parse_config(&input) { Ok(c) => c, Err(reply) => return Ok(reply) };
                if let Some(existing) = load(tx, &root, &id)? {
                    return Ok(if existing.config == config { response(200, serialize(&existing, &kid)) }
                        else { failure(409, "SUBSCRIPTION_ALREADY_EXISTS", "subscription configuration differs") });
                }
                let mut subscription = Subscription { id, root, control_base, incarnation: random_id()?, config,
                    created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    links: BTreeMap::new(), generation: 0, wake: None };
                for path in &subscription.config.streams {
                    let stream = tail(store, &subscription.root, path);
                    subscription.links.insert(path.clone(), Link { explicit: true, glob: false,
                        acked_offset: stream.as_ref().map(|(_, offset)| offset.clone()).unwrap_or_else(|| format_offset(0)),
                        stream_id: stream.map(|(id, _)| id) });
                }
                discover(&mut subscription, store, true, None);
                save(tx, &subscription)?;
                return Ok(response(201, serialize(&subscription, &kid)));
            }
            if action.is_empty() && method == Method::Delete {
                tx.execute("DELETE FROM subscriptions WHERE root=?1 AND id=?2", params![root, id])?;
                return Ok(response(204, Value::Null));
            }
            let Some(mut subscription) = load(tx, &root, &id)? else {
                return Ok(failure(404, "SUBSCRIPTION_NOT_FOUND", "subscription not found"));
            };
            let previous = serde_json::to_string(&subscription)?;
            // Mutating control operations must see appends even when the
            // background reconciler has not consumed their notifications yet.
            discover(&mut subscription, store, false,
                if method == Method::Get { Some(&[]) } else { None });
            expire_lease(&mut subscription);
            let reply = match (method, action.as_str()) {
                (Method::Get, "") => response(200, serialize(&subscription, &kid)),
                (Method::Post, "streams") => {
                    let paths = match string_paths(input.get("streams")) { Ok(p) if !p.is_empty() => p,
                        _ => return Ok(failure(400, "INVALID_REQUEST", "streams must be a nonempty string array")) };
                    if subscription.links.len() + paths.len() > MAX_STREAMS {
                        return Ok(failure(400, "INVALID_REQUEST", "too many subscription streams"));
                    }
                    for path in paths {
                        if let Some(link) = subscription.links.get_mut(&path) { link.explicit = true; }
                        else {
                            let stream = tail(store, &subscription.root, &path);
                            subscription.links.insert(path, Link { explicit: true, glob: false,
                                acked_offset: stream.as_ref().map(|(_, offset)| offset.clone()).unwrap_or_else(|| format_offset(0)),
                                stream_id: stream.map(|(id, _)| id) });
                        }
                    }
                    response(204, Value::Null)
                }
                (Method::Delete, action) if action.starts_with("streams/") => {
                    let path = percent_encoding::percent_decode_str(&action[8..]).decode_utf8_lossy();
                    if let Some(link) = subscription.links.get_mut(path.trim_matches('/')) { link.explicit = false; }
                    subscription.links.retain(|_, link| link.explicit || link.glob);
                    subscription.wake = None;
                    response(204, Value::Null)
                }
                (Method::Post, "claim") => {
                    if subscription.config.kind != "pull-wake" { failure(400, "INVALID_REQUEST", "not a pull-wake subscription") }
                    else if let Some(holder) = subscription.wake.as_ref().and_then(|w| w.holder.as_ref()) {
                        response(409, json!({"error":{"code":"ALREADY_CLAIMED", "current_holder":holder,
                            "generation":subscription.generation}}))
                    } else if let Some(worker) = input.get("worker").and_then(Value::as_str).filter(|s| !s.is_empty() && s.len() <= 1024) {
                        if !pending(&subscription, store) { failure(409, "NO_PENDING_WORK", "no pending work") }
                        else {
                            ensure_wake(&mut subscription, store)?;
                            let streams = stream_infos(&subscription, store);
                            let wake = subscription.wake.as_mut().unwrap();
                            wake.holder = Some(worker.into());
                            wake.delivery_pending = false;
                            wake.lease_until = now_ms() + subscription.config.lease_ttl_ms;
                            response(200, json!({"wake_id":wake.id,"generation":subscription.generation,
                                "token":wake.token,"streams":streams,"lease_ttl_ms":subscription.config.lease_ttl_ms}))
                        }
                    } else { failure(400, "INVALID_REQUEST", "worker is required") }
                }
                (Method::Post, "callback" | "ack" | "release") => {
                    let correct_type = (action == "callback") == (subscription.config.kind == "webhook");
                    if !correct_type { failure(400, "INVALID_REQUEST", "wrong delivery type") }
                    else if let Some(reply) = validate_wake(&subscription, token.as_deref(), &input) { reply }
                    else if let Some(reply) = validate_acks(&subscription, store, &input) { reply }
                    else {
                        if action != "release" { apply_acks(&mut subscription, &input); }
                        let done = action == "release" || input.get("done") == Some(&Value::Bool(true));
                        if done { subscription.wake = None; }
                        else if let Some(wake) = &mut subscription.wake {
                            wake.lease_until = now_ms() + subscription.config.lease_ttl_ms;
                        }
                        let next_wake = done && pending(&subscription, store);
                        if next_wake { ensure_wake(&mut subscription, store)?; }
                        if action == "release" { response(204, Value::Null) }
                        else { response(200, json!({"ok":true,"next_wake":next_wake})) }
                    }
                }
                _ if action.is_empty() || matches!(action.as_str(), "streams" | "claim" | "callback" | "ack" | "release") => method_not_allowed(),
                _ => failure(404, "NOT_FOUND", "unknown control route"),
            };
            if serde_json::to_string(&subscription)? != previous { save(tx, &subscription)?; }
            Ok(reply)
        }).await;
        self.notify();
        match result {
            Ok(reply) => reply,
            Err(err) => {
                tracing::error!(error=%err, "subscription transaction failed");
                failure(500, "INTERNAL_ERROR", "subscription storage failed")
            }
        }
    }

    async fn run(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(POLL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut deliveries = tokio::task::JoinSet::new();
        loop {
            tokio::select! { _ = ticker.tick() => {}, _ = self.changed.notified() => {},
                Some(result) = deliveries.join_next(), if !deliveries.is_empty() => {
                    if let Err(error) = result { tracing::error!(%error, "subscription delivery task failed"); }
                    continue;
                }
            }
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            let capacity = 32usize.saturating_sub(deliveries.len());
            let paths: Vec<_> = std::mem::take(&mut *self.dirty_paths.lock().unwrap())
                .into_iter()
                .collect();
            let full_scan = self.full_scan.swap(false, Ordering::AcqRel);
            let ready = self
                .transact(move |tx, store| {
                    let mut statement =
                        tx.prepare("SELECT body FROM subscriptions ORDER BY root,id")?;
                    let rows = statement
                        .query_map([], |row| row.get::<_, String>(0))?
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    drop(statement);
                    let mut ready = Vec::new();
                    for body in rows {
                        let mut subscription: Subscription = serde_json::from_str(&body)?;
                        discover(
                            &mut subscription,
                            store,
                            false,
                            if full_scan { None } else { Some(&paths) },
                        );
                        expire_lease(&mut subscription);
                        if pending(&subscription, store) {
                            ensure_wake(&mut subscription, store)?;
                        }
                        if let Some(wake) = &mut subscription.wake {
                            if ready.len() < capacity
                                && wake.holder.is_none()
                                && wake.delivery_pending
                                && wake.next_attempt_at <= now_ms()
                            {
                                // Persist an in-flight deadline. A crash before/after delivery
                                // may duplicate a wake, but cannot lose it or auto-ack it.
                                wake.next_attempt_at = now_ms() + 6000;
                                wake.lease_until = wake
                                    .lease_until
                                    .max(wake.next_attempt_at + subscription.config.lease_ttl_ms);
                                ready.push(Delivery {
                                    subscription: subscription.clone(),
                                });
                            }
                        }
                        if serde_json::to_string(&subscription)? != body {
                            save(tx, &subscription)?;
                        }
                    }
                    Ok(ready)
                })
                .await;
            match ready {
                Ok(ready) => {
                    for delivery in ready {
                        let service = Arc::clone(&self);
                        deliveries.spawn(async move {
                            service.deliver(delivery).await;
                        });
                    }
                }
                Err(error) => {
                    self.full_scan.store(true, Ordering::Release);
                    tracing::error!(%error, "subscription reconciliation failed");
                }
            }
        }
        while deliveries.join_next().await.is_some() {}
    }

    async fn deliver(self: Arc<Self>, delivery: Delivery) {
        let subscription = delivery.subscription;
        let wake = subscription.wake.as_ref().unwrap();
        let (success, done) = if let Some(url) = &subscription.config.webhook {
            let body = json!({"subscription_id":subscription.id,"wake_id":wake.id,
                "generation":subscription.generation,"streams":wake.snapshot,
                "callback_url":format!("{}/subscriptions/{}/callback", subscription.control_base, encode(&subscription.id)),
                "callback_token":wake.token}).to_string();
            let timestamp = now_ms() / 1000;
            let signature = self
                .signing_key
                .sign(format!("{timestamp}.{body}").as_bytes());
            let header = format!(
                "t={timestamp},kid={},ed25519={}",
                self.kid,
                B64.encode(signature.as_ref())
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                match self.safe_webhook_post(url, body, header).await {
                    Ok(reply) if reply.status().is_success() => {
                        let data = read_reply(reply).await;
                        (
                            true,
                            data.as_ref().and_then(|v| v.get("done")) == Some(&Value::Bool(true)),
                        )
                    }
                    _ => (false, false),
                }
            })
            .await
            .unwrap_or((false, false))
        } else {
            (self.write_pull_wake(&subscription).await, false)
        };
        let root = subscription.root.clone();
        let id = subscription.id.clone();
        let wake_id = wake.id.clone();
        let generation = subscription.generation;
        let result = self
            .transact(move |tx, store| {
                let Some(mut current) = load(tx, &root, &id)? else {
                    return Ok(());
                };
                // Delivery can finish after a stream was deleted/recreated or
                // after its lease expired. Never apply its old snapshot then.
                discover(&mut current, store, false, None);
                expire_lease(&mut current);
                if current.generation != generation
                    || current.wake.as_ref().map(|w| &w.id) != Some(&wake_id)
                {
                    save(tx, &current)?;
                    return Ok(());
                }
                // A worker may have claimed a pull wake while its notification
                // append was in flight. That completion must not extend or
                // restart the worker's independently persisted lease.
                if current.config.kind == "pull-wake"
                    && current
                        .wake
                        .as_ref()
                        .is_some_and(|wake| wake.holder.is_some())
                {
                    return Ok(());
                }
                if success && done {
                    let snapshot = current.wake.as_ref().unwrap().snapshot.clone();
                    for stream in snapshot {
                        if let Some(link) = current.links.get_mut(&stream.path) {
                            if stream.tail_offset > link.acked_offset {
                                link.acked_offset = stream.tail_offset;
                            }
                        }
                    }
                    current.wake = None;
                    if pending(&current, store) {
                        ensure_wake(&mut current, store)?;
                    }
                } else {
                    let wake = current.wake.as_mut().unwrap();
                    if success {
                        wake.delivery_pending = false;
                        wake.retry_count = 0;
                        wake.lease_until = now_ms() + current.config.lease_ttl_ms;
                    } else {
                        wake.retry_count = wake.retry_count.saturating_add(1);
                        let base =
                            (1000u64 << wake.retry_count.saturating_sub(1).min(6)).min(60_000);
                        let mut jitter = [0u8; 2];
                        SystemRandom::new()
                            .fill(&mut jitter)
                            .map_err(|_| "random jitter failed")?;
                        let factor = 0.8 + f64::from(u16::from_be_bytes(jitter)) / 65535.0 * 0.4;
                        wake.next_attempt_at = now_ms() + (base as f64 * factor) as u64;
                        wake.lease_until = wake.next_attempt_at + current.config.lease_ttl_ms;
                        wake.delivery_pending = true;
                    }
                }
                save(tx, &current)
            })
            .await;
        if let Err(error) = result {
            tracing::error!(%error, "subscription delivery commit failed");
        }
        self.notify();
    }

    async fn safe_webhook_post(
        &self,
        raw_url: &str,
        body: String,
        signature: String,
    ) -> Result<reqwest::Response> {
        let url = reqwest::Url::parse(raw_url)?;
        validate_url(&url).map_err(|_| "unsafe webhook URL")?;
        let host = url.host_str().ok_or("webhook host missing")?;
        let port = url.port_or_known_default().ok_or("webhook port missing")?;
        // Validate DNS at delivery time and pin the checked addresses, closing
        // the DNS-rebinding gap between validation and the HTTP connection.
        let addresses: Vec<_> = tokio::net::lookup_host((host, port)).await?.collect();
        let dev = url.scheme() == "http"
            && (host == "localhost" || host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback()));
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|address| !(public_ip(address.ip()) || dev && address.ip().is_loopback()))
        {
            return Err("webhook resolved to a non-public address".into());
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .resolve_to_addrs(host, &addresses)
            .build()?;
        Ok(client
            .post(url)
            .header("content-type", "application/json")
            .header("webhook-signature", signature)
            .body(body)
            .send()
            .await?)
    }

    async fn write_pull_wake(&self, subscription: &Subscription) -> bool {
        let Some(store) = self.store.upgrade() else {
            return false;
        };
        let Some(path) = &subscription.config.wake_stream else {
            return false;
        };
        let wake = subscription.wake.as_ref().unwrap();
        let full_path = absolute(&subscription.root, path);
        if tail(&store, &subscription.root, path).is_none() {
            return false;
        }
        let event = json!({"type":"wake", "subscription_id":subscription.id,
            "stream":wake.snapshot.iter().find(|s| s.has_pending).map(|s| s.path.as_str()).unwrap_or(""),
            "generation":subscription.generation,"ts":now_ms()});
        let req = Req {
            method: Method::Post,
            path: full_path,
            query: None,
            headers: vec![
                ("content-type".into(), "application/json".into()),
                (
                    "producer-id".into(),
                    format!("ds-sub-{}", subscription.incarnation),
                ),
                ("producer-epoch".into(), subscription.generation.to_string()),
                ("producer-seq".into(), "0".into()),
            ],
            body: Bytes::from(event.to_string()),
        };
        // Box breaks the static future recursion through dispatch; the wake
        // stream append uses the exact same WAL/durability path as external writes.
        let reply = Box::pin(crate::handlers::handle(store, req)).await;
        (200..300).contains(&reply.status)
    }
}

async fn read_reply(mut reply: reqwest::Response) -> Option<Value> {
    let mut body = Vec::new();
    while let Some(chunk) = reply.chunk().await.ok()? {
        if body.len() + chunk.len() > 64 * 1024 {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).ok()
}

fn load(tx: &Transaction<'_>, root: &str, id: &str) -> Result<Option<Subscription>> {
    let body: Option<String> = tx
        .query_row(
            "SELECT body FROM subscriptions WHERE root=?1 AND id=?2",
            params![root, id],
            |row| row.get(0),
        )
        .optional()?;
    body.map(|body| serde_json::from_str(&body).map_err(Into::into))
        .transpose()
}

fn save(tx: &Transaction<'_>, subscription: &Subscription) -> Result<()> {
    tx.execute(
        "INSERT INTO subscriptions(root,id,body) VALUES (?1,?2,?3)
        ON CONFLICT(root,id) DO UPDATE SET body=excluded.body",
        params![
            subscription.root,
            subscription.id,
            serde_json::to_string(subscription)?
        ],
    )?;
    Ok(())
}

fn tail(store: &Store, root: &str, path: &str) -> Option<(u64, String)> {
    // Avoid Store::get's expiry mutation under the subscription DB transaction.
    // The stream registry owns deletion; subscriptions only observe it.
    let stream = store.streams.get(&absolute(root, path))?;
    if stream.is_expired() || stream.shared.read().unwrap().soft_deleted {
        return None;
    }
    Some((stream.id, format_offset(stream.tail().bytes)))
}

fn absolute(root: &str, path: &str) -> String {
    format!("{root}/{}", path.trim_matches('/'))
}

fn discover(
    subscription: &mut Subscription,
    store: &Store,
    initial: bool,
    candidates: Option<&[String]>,
) {
    if let Some(pattern) = &subscription.config.pattern {
        let prefix = format!("{}/", subscription.root);
        let paths: Vec<String> = match candidates {
            Some(paths) => paths.to_vec(),
            None => store
                .streams
                .iter()
                .map(|entry| entry.key().clone())
                .collect(),
        };
        for full_path in paths {
            let Some(path) = full_path.strip_prefix(&prefix) else {
                continue;
            };
            if path.starts_with("__ds/") || !glob_match(pattern, path) {
                continue;
            }
            let Some((id, offset)) = tail(store, &subscription.root, path) else {
                continue;
            };
            let link = subscription
                .links
                .entry(path.to_string())
                .or_insert_with(|| Link {
                    acked_offset: if initial { offset } else { format_offset(0) },
                    explicit: false,
                    glob: true,
                    stream_id: Some(id),
                });
            link.glob = true;
        }
    }
    let mut invalidated = false;
    subscription.links.retain(|path, link| {
        if let Some((id, _)) = tail(store, &subscription.root, path) {
            if link.stream_id.is_some_and(|old| old != id) {
                invalidated = true;
                link.acked_offset = format_offset(0);
            }
            link.stream_id = Some(id);
            true
        } else {
            if link.stream_id.is_some() {
                invalidated = true;
            }
            link.stream_id.is_none()
        }
    });
    if invalidated {
        subscription.wake = None;
    }
}

fn stream_infos(subscription: &Subscription, store: &Store) -> Vec<StreamInfo> {
    subscription
        .links
        .iter()
        .map(|(path, link)| {
            let offset = tail(store, &subscription.root, path)
                .map(|(_, offset)| offset)
                .unwrap_or_else(|| format_offset(0));
            StreamInfo {
                path: path.clone(),
                link_type: if link.explicit { "explicit" } else { "glob" }.into(),
                has_pending: offset > link.acked_offset,
                acked_offset: link.acked_offset.clone(),
                tail_offset: offset,
            }
        })
        .collect()
}

fn pending(subscription: &Subscription, store: &Store) -> bool {
    subscription.links.iter().any(|(path, link)| {
        tail(store, &subscription.root, path).is_some_and(|(_, offset)| offset > link.acked_offset)
    })
}

fn ensure_wake(subscription: &mut Subscription, store: &Store) -> Result<()> {
    if subscription.wake.is_some() || !pending(subscription, store) {
        return Ok(());
    }
    subscription.generation = subscription
        .generation
        .checked_add(1)
        .ok_or("generation overflow")?;
    subscription.wake = Some(Wake {
        id: format!("w_{}", random_id()?),
        token: random_id()?,
        snapshot: stream_infos(subscription, store),
        lease_until: now_ms() + subscription.config.lease_ttl_ms,
        holder: None,
        delivery_pending: true,
        next_attempt_at: now_ms(),
        retry_count: 0,
    });
    Ok(())
}

fn expire_lease(subscription: &mut Subscription) {
    if subscription
        .wake
        .as_ref()
        .is_some_and(|w| w.lease_until <= now_ms())
    {
        subscription.wake = None;
    }
}

fn validate_wake(subscription: &Subscription, token: Option<&str>, input: &Value) -> Option<Resp> {
    let Some(token) = token else {
        return Some(failure(401, "TOKEN_INVALID", "bearer token required"));
    };
    let Some(wake) = &subscription.wake else {
        return Some(failure(409, "FENCED", "wake is no longer active"));
    };
    if input.get("generation").and_then(Value::as_u64) != Some(subscription.generation)
        || input.get("wake_id").and_then(Value::as_str) != Some(wake.id.as_str())
    {
        return Some(failure(409, "FENCED", "wake generation is stale"));
    }
    if token.as_bytes().ct_eq(wake.token.as_bytes()).unwrap_u8() != 1 {
        return Some(failure(401, "TOKEN_INVALID", "invalid bearer token"));
    }
    None
}

fn validate_acks(subscription: &Subscription, store: &Store, input: &Value) -> Option<Resp> {
    if input.get("done").is_some_and(|done| !done.is_boolean()) {
        return Some(failure(400, "INVALID_REQUEST", "done must be boolean"));
    }
    let acks = input.get("acks")?;
    let Some(acks) = acks.as_array() else {
        return Some(failure(400, "INVALID_REQUEST", "acks must be an array"));
    };
    for ack in acks {
        let path = ack
            .get("stream")
            .or_else(|| ack.get("path"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_matches('/');
        let offset = ack.get("offset").and_then(Value::as_str).unwrap_or("");
        let Some(link) = subscription.links.get(path) else {
            return Some(failure(
                409,
                "INVALID_OFFSET",
                "unknown subscription stream",
            ));
        };
        let tail = tail(store, &subscription.root, path)
            .map(|(_, offset)| offset)
            .unwrap_or_else(|| format_offset(0));
        if !valid_offset(offset) || offset < link.acked_offset.as_str() || offset > tail.as_str() {
            return Some(failure(
                409,
                "INVALID_OFFSET",
                "offset is invalid, regresses, or exceeds the durable tail",
            ));
        }
    }
    None
}

fn apply_acks(subscription: &mut Subscription, input: &Value) {
    if let Some(acks) = input.get("acks").and_then(Value::as_array) {
        for ack in acks {
            let path = ack
                .get("stream")
                .or_else(|| ack.get("path"))
                .and_then(Value::as_str)
                .unwrap()
                .trim_matches('/');
            let offset = ack.get("offset").and_then(Value::as_str).unwrap();
            let link = subscription.links.get_mut(path).unwrap();
            if offset > link.acked_offset.as_str() {
                link.acked_offset = offset.into();
            }
        }
    }
}

fn valid_offset(offset: &str) -> bool {
    offset.len() == 33
        && offset.as_bytes()[16] == b'_'
        && offset
            .bytes()
            .enumerate()
            .all(|(i, byte)| i == 16 || byte.is_ascii_digit())
        && offset[..16] == "0000000000000000"[..]
}

fn serialize(subscription: &Subscription, kid: &str) -> Value {
    let config = &subscription.config;
    json!({"id":subscription.id,"subscription_id":subscription.id,"type":config.kind,
        "pattern":config.pattern,"description":config.description,"wake_stream":config.wake_stream,
        "lease_ttl_ms":config.lease_ttl_ms,"created_at":subscription.created_at,
        "status":if subscription.wake.as_ref().is_some_and(|w| w.retry_count>0) { "failed" } else { "active" },
        "webhook":config.webhook.as_ref().map(|url| json!({"url":url,"signing":{"alg":"ed25519",
            "kid":kid,"jwks_url":format!("{}/jwks.json",subscription.control_base)}})),
        "streams":subscription.links.iter().map(|(path,link)| json!({"path":path,
            "link_type":if link.explicit { "explicit" } else { "glob" },"acked_offset":link.acked_offset})).collect::<Vec<_>>()})
}

fn parse_config(input: &Value) -> std::result::Result<Config, Resp> {
    let invalid = || failure(400, "INVALID_REQUEST", "invalid subscription configuration");
    let kind = input
        .get("type")
        .and_then(Value::as_str)
        .filter(|s| matches!(*s, "webhook" | "pull-wake"))
        .ok_or_else(invalid)?
        .to_string();
    let pattern = optional_string(input, "pattern")
        .ok_or_else(invalid)?
        .map(|s| s.trim_matches('/').to_string());
    let streams = string_paths(input.get("streams")).map_err(|_| invalid())?;
    if pattern
        .as_ref()
        .is_some_and(|p| p.is_empty() || p.len() > 1024)
        || (pattern.is_none() && streams.is_empty())
    {
        return Err(invalid());
    }
    let webhook = match input.get("webhook") {
        Some(value) => Some(
            value
                .get("url")
                .and_then(Value::as_str)
                .ok_or_else(invalid)?
                .to_string(),
        ),
        None => None,
    };
    if kind == "webhook" && webhook.is_none() {
        return Err(invalid());
    }
    if let Some(raw) = &webhook {
        let url = reqwest::Url::parse(raw)
            .map_err(|_| failure(400, "WEBHOOK_URL_REJECTED", "invalid webhook URL"))?;
        validate_url(&url)
            .map_err(|_| failure(400, "WEBHOOK_URL_REJECTED", "unsafe webhook URL"))?;
    }
    let wake_stream = optional_string(input, "wake_stream").ok_or_else(invalid)?;
    let wake_stream = wake_stream.map(|s| s.trim_matches('/').to_string());
    if kind == "pull-wake" && wake_stream.as_ref().map_or(true, |s| !valid_path(s)) {
        return Err(invalid());
    }
    if kind == "webhook" && wake_stream.is_some() || kind == "pull-wake" && webhook.is_some() {
        return Err(invalid());
    }
    let lease_ttl_ms = match input.get("lease_ttl_ms") {
        Some(value) => value.as_u64().ok_or_else(invalid)?,
        None => 30_000,
    };
    if !(1000..=600_000).contains(&lease_ttl_ms) {
        return Err(invalid());
    }
    let description = optional_string(input, "description").ok_or_else(invalid)?;
    Ok(Config {
        kind,
        pattern,
        streams,
        webhook,
        wake_stream,
        lease_ttl_ms,
        description,
    })
}

fn optional_string(input: &Value, field: &str) -> Option<Option<String>> {
    match input.get(field) {
        None => Some(None),
        Some(value) => value.as_str().map(|s| Some(s.to_string())),
    }
}

fn string_paths(value: Option<&Value>) -> std::result::Result<Vec<String>, ()> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or(())?;
    if values.len() > MAX_STREAMS {
        return Err(());
    }
    let mut paths = BTreeSet::new();
    for value in values {
        let path = value.as_str().ok_or(())?.trim_matches('/');
        if !valid_path(path) {
            return Err(());
        }
        paths.insert(path.to_string());
    }
    Ok(paths.into_iter().collect())
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && path != "__ds"
        && !path.starts_with("__ds/")
        && !path.bytes().any(|b| b.is_ascii_control())
        && !path.split('/').any(|p| p == "." || p == "..")
}

fn validate_url(url: &reqwest::Url) -> std::result::Result<(), ()> {
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(());
    }
    let host = url.host_str().ok_or(())?;
    if url.scheme() == "http"
        && (host == "localhost" || host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback()))
    {
        return Ok(());
    }
    if url.scheme() != "https" || host == "localhost" || host.ends_with(".localhost") {
        return Err(());
    }
    if let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() {
        if !public_ip(ip) {
            return Err(());
        }
    }
    Ok(())
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && a != 0
                && a < 240
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (b == 18 || b == 19))
                && !(a == 192 && b == 0 && (c == 0 || c == 2))
                && !(a == 198 && b == 51 && c == 100)
                && !(a == 203 && b == 0 && c == 113)
        }
        IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(ip));
            }
            let segments = ip.segments();
            (segments[0] & 0xe000) == 0x2000 && !(segments[0] == 0x2001 && segments[1] == 0xdb8)
        }
    }
}

fn glob_match(pattern: &str, path: &str) -> bool {
    // Match the protocol/reference server's segment semantics: * is exactly
    // one segment, ** is zero or more, and other segments are literals.
    // DP avoids recursive/exponential backtracking on untrusted patterns.
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    let mut previous = vec![false; parts.len() + 1];
    previous[0] = true;
    for segment in pattern.split('/').filter(|part| !part.is_empty()) {
        let mut current = vec![false; parts.len() + 1];
        if segment == "**" {
            current[0] = previous[0];
            for j in 1..=parts.len() {
                current[j] = previous[j] || current[j - 1];
            }
        } else {
            let literal = segment.replace("%2A", "*").replace("%2a", "*");
            for j in 1..=parts.len() {
                current[j] = previous[j - 1] && (segment == "*" || literal == parts[j - 1]);
            }
        }
        previous = current;
    }
    previous[parts.len()]
}

fn random_id() -> Result<String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "random generation failed")?;
    Ok(B64.encode(bytes))
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn encode(value: &str) -> String {
    const COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~')
        .remove(b'!')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')')
        .remove(b'*');
    percent_encoding::utf8_percent_encode(value, COMPONENT).to_string()
}
fn method_not_allowed() -> Resp {
    failure(405, "INVALID_REQUEST", "method not allowed")
}
fn failure(status: u16, code: &str, message: &str) -> Resp {
    response(status, json!({"error":{"code":code,"message":message}}))
}
fn response(status: u16, value: Value) -> Resp {
    let mut reply = Resp::new(status);
    if status != 204 {
        reply
            .headers
            .push(("content-type", "application/json".into()));
        reply.body = Body::Full(Bytes::from(value.to_string()));
    }
    reply.headers.push(("cache-control", "no-store".into()));
    reply
}
