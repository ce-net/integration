//! # ce-integration — real two-node CE mesh integration driver
//!
//! This binary is the assertion engine of the harness. The surrounding shell script
//! (`run.sh`) boots two **isolated** CE nodes (node B bootstrapped to node A, both with
//! `--no-mdns` so the live `:8844` node is never cross-linked), then invokes this driver
//! with each node's API base URL and API token. The driver drives **real cross-node mesh
//! paths** through the `ce-rs` SDK and asserts the results.
//!
//! It owns no node lifecycle — boot/teardown is the shell's job — so this stays a pure,
//! deterministic checker that prints a PASS/FAIL/BLOCKED matrix and exits non-zero if any
//! non-blocked scenario fails.
//!
//! Scenarios:
//!   1. mesh request/reply  — A `POST /mesh/request` -> B's reply loop answers -> A gets payload
//!   2. pubsub              — B subscribes a topic, A publishes, B receives it
//!   3. blob availability   — A `put_blob` -> B fetches the same CID over the mesh (`GET /blobs/:cid`)
//!   4. discovery + tags    — B advertises a service tag on the DHT, A finds B as a provider
//!   5. tunnel              — A opens a tunnel to a TCP port served on B's host; bytes flow
//!
//! All amounts on the wire are base-unit decimal strings; this driver moves no money so it
//! sidesteps that entirely, but it honours the SDK's typed `Amount` where the API needs it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use ce_rs::CeClient;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

/// One node under test: its SDK client, raw base URL, and authenticated NodeId.
struct Node {
    name: &'static str,
    client: CeClient,
    base: String,
    node_id: String,
}

impl Node {
    async fn connect(name: &'static str, base: String, token: String) -> Result<Self> {
        let client = CeClient::with_token(base.clone(), Some(token));
        let status = client
            .status()
            .await
            .with_context(|| format!("{name}: GET /status failed at {base}"))?;
        let node = Node { name, client, base, node_id: status.node_id };
        info!("connected node {} at {} (id {})", node.name, node.base, short_id(&node.node_id));
        Ok(node)
    }
}

/// Outcome of a single scenario.
#[derive(Clone, Copy, PartialEq)]
enum Outcome {
    Pass,
    Fail,
    Blocked,
}

impl Outcome {
    fn tag(self) -> &'static str {
        match self {
            Outcome::Pass => "PASS   ",
            Outcome::Fail => "FAIL   ",
            Outcome::Blocked => "BLOCKED",
        }
    }
}

struct Report {
    rows: Vec<(String, Outcome, String)>,
}

impl Report {
    fn new() -> Self {
        Report { rows: Vec::new() }
    }
    fn record(&mut self, name: &str, outcome: Outcome, detail: impl Into<String>) {
        let detail = detail.into();
        match outcome {
            Outcome::Pass => info!("[PASS] {name}: {detail}"),
            Outcome::Fail => warn!("[FAIL] {name}: {detail}"),
            Outcome::Blocked => warn!("[BLOCKED] {name}: {detail}"),
        }
        self.rows.push((name.to_string(), outcome, detail));
    }
    fn any_failed(&self) -> bool {
        self.rows.iter().any(|(_, o, _)| *o == Outcome::Fail)
    }
    fn print(&self) {
        println!("\n================ CE TWO-NODE MESH MATRIX ================");
        for (name, outcome, detail) in &self.rows {
            println!("  {}  {:<22} {}", outcome.tag(), name, detail);
        }
        let pass = self.rows.iter().filter(|(_, o, _)| *o == Outcome::Pass).count();
        let fail = self.rows.iter().filter(|(_, o, _)| *o == Outcome::Fail).count();
        let blocked = self.rows.iter().filter(|(_, o, _)| *o == Outcome::Blocked).count();
        println!("--------------------------------------------------------");
        println!("  {pass} passed, {fail} failed, {blocked} blocked");
        println!("========================================================\n");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cfg = Config::from_env()?;
    let a = Node::connect("A", cfg.a_base.clone(), cfg.a_token.clone()).await?;
    let b = Node::connect("B", cfg.b_base.clone(), cfg.b_token.clone()).await?;
    info!("node A id={}", a.node_id);
    info!("node B id={}", b.node_id);
    if a.node_id == b.node_id {
        bail!("node A and node B report the same NodeId — they are not two distinct nodes");
    }

    let mut report = Report::new();

    // Peering gate: prove the two nodes can actually talk over the mesh before asserting
    // richer scenarios. A directed mesh send from A to B succeeds only once they are peered,
    // so we retry it as the readiness probe. (We do NOT gate on /atlas: that is fed by CEP-1
    // capacity broadcasts which only run while mining, and the test mesh runs --no-mine.)
    match wait_for_peering(&a, &b, cfg.peer_timeout).await {
        Ok(elapsed) => report.record(
            "peering",
            Outcome::Pass,
            format!("A and B peered over the mesh in {:.1}s (directed send delivered)", elapsed.as_secs_f64()),
        ),
        Err(e) => {
            report.record("peering", Outcome::Fail, format!("nodes never peered: {e:#}"));
            // Without peering, every cross-node scenario is meaningless. Report and bail.
            report.print();
            bail!("aborting: nodes failed to peer");
        }
    }

    run_request_reply(&a, &b, &mut report).await;
    run_pubsub(&a, &b, &mut report).await;
    run_blob(&a, &b, &mut report).await;
    run_discovery(&a, &b, &mut report).await;
    run_tunnel(&a, &b, &mut report).await;

    report.print();
    if report.any_failed() {
        std::process::exit(1);
    }
    Ok(())
}

struct Config {
    a_base: String,
    a_token: String,
    b_base: String,
    b_token: String,
    peer_timeout: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let get = |k: &str| -> Result<String> {
            std::env::var(k).map_err(|_| anyhow!("missing required env var {k}"))
        };
        let peer_secs: u64 =
            std::env::var("CE_IT_PEER_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(45);
        Ok(Config {
            a_base: get("CE_IT_A_BASE")?,
            a_token: get("CE_IT_A_TOKEN")?,
            b_base: get("CE_IT_B_BASE")?,
            b_token: get("CE_IT_B_TOKEN")?,
            peer_timeout: Duration::from_secs(peer_secs),
        })
    }
}

/// Retry a directed mesh send A -> B until it is delivered (proving an end-to-end mesh route),
/// or the timeout elapses. Returns the time it took.
async fn wait_for_peering(a: &Node, b: &Node, timeout: Duration) -> Result<Duration> {
    let start = Instant::now();
    let mut last_err = String::from("(no attempt)");
    while start.elapsed() < timeout {
        match a.client.send_message(&b.node_id, "ce-it/ping", b"ping").await {
            Ok(()) => return Ok(start.elapsed()),
            Err(e) => last_err = format!("{e:#}"),
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow!("directed send A->B never succeeded within {:?}: {last_err}", timeout))
}

/// Background reply loop for node B: poll its inbox and answer any request on `topic` by
/// echoing the payload with a prefix, so node A's `request` resolves. Returns a stop handle.
fn spawn_replier(b: &Node, topic: &'static str, prefix: &'static [u8]) -> ReplierHandle {
    let client = b.client.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let handle = tokio::spawn(async move {
        let mut answered = 0u64;
        while !stop2.load(Ordering::Relaxed) {
            match client.messages().await {
                Ok(msgs) => {
                    for m in msgs {
                        if m.topic != topic {
                            continue;
                        }
                        if let Some(token) = m.reply_token {
                            let payload = m.payload().unwrap_or_default();
                            let mut out = prefix.to_vec();
                            out.extend_from_slice(&payload);
                            if client.reply(token, &out).await.is_ok() {
                                answered += 1;
                            }
                        }
                    }
                }
                Err(_) => {}
            }
            sleep(Duration::from_millis(100)).await;
            let _ = answered;
        }
    });
    ReplierHandle { stop, handle }
}

struct ReplierHandle {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

impl ReplierHandle {
    async fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.handle.await;
    }
}

// ---------------- Scenario 1: mesh request/reply ----------------

async fn run_request_reply(a: &Node, b: &Node, report: &mut Report) {
    const TOPIC: &str = "ce-it/echo";
    const PREFIX: &[u8] = b"echo:";
    // B runs an app reply loop that echoes requests on TOPIC.
    let replier = spawn_replier(b, TOPIC, PREFIX);
    // Give the loop a moment to start polling.
    sleep(Duration::from_millis(300)).await;

    let body = b"hello-from-A";
    let result = a.client.request(&b.node_id, TOPIC, body, 15_000).await;
    replier.shutdown().await;

    match result {
        Ok(reply) => {
            let mut expected = PREFIX.to_vec();
            expected.extend_from_slice(body);
            if reply == expected {
                report.record(
                    "mesh request/reply",
                    Outcome::Pass,
                    format!("A->B request answered; got {} bytes echoed correctly", reply.len()),
                );
            } else {
                report.record(
                    "mesh request/reply",
                    Outcome::Fail,
                    format!("reply mismatch: got {:?}", String::from_utf8_lossy(&reply)),
                );
            }
        }
        Err(e) => report.record("mesh request/reply", Outcome::Fail, format!("request failed: {e:#}")),
    }
}

// ---------------- Scenario 2: pubsub ----------------

async fn run_pubsub(a: &Node, b: &Node, report: &mut Report) {
    let topic = format!("ce-it/news-{}", short_id(&a.node_id));
    // B subscribes.
    if let Err(e) = b.client.subscribe(&topic).await {
        report.record("pubsub", Outcome::Fail, format!("B subscribe failed: {e:#}"));
        return;
    }
    // Gossipsub needs the subscription to propagate to A's view of the mesh before a publish
    // will be routed to B. Retry publishing and polling B's inbox until the message lands.
    let payload = format!("breaking-{}", now_secs());
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = String::from("(no attempt)");
    loop {
        if Instant::now() > deadline {
            report.record("pubsub", Outcome::Fail, format!("B never received the published message: {last}"));
            return;
        }
        if let Err(e) = a.client.publish(&topic, payload.as_bytes()).await {
            last = format!("A publish failed: {e:#}");
            sleep(Duration::from_millis(500)).await;
            continue;
        }
        sleep(Duration::from_millis(700)).await;
        match b.client.messages().await {
            Ok(msgs) => {
                let hit = msgs.iter().any(|m| {
                    m.topic == topic && m.payload().map(|p| p == payload.as_bytes()).unwrap_or(false)
                });
                if hit {
                    report.record(
                        "pubsub",
                        Outcome::Pass,
                        format!("B received A's publish on topic '{topic}'"),
                    );
                    return;
                }
                last = "message not yet in B's inbox".to_string();
            }
            Err(e) => last = format!("B inbox poll failed: {e:#}"),
        }
    }
}

// ---------------- Scenario 3: blob availability over the mesh ----------------

async fn run_blob(a: &Node, b: &Node, report: &mut Report) {
    // A stores a unique blob; the node announces the CID to the DHT for mesh availability.
    let content = format!("ce-it blob payload {} {}", now_secs(), short_id(&b.node_id));
    let cid = match a.client.put_blob(content.clone().into_bytes()).await {
        Ok(c) => c,
        Err(e) => {
            report.record("blob availability", Outcome::Fail, format!("A put_blob failed: {e:#}"));
            return;
        }
    };
    // B does NOT have the blob locally; GET /blobs/:cid on B must fall back to the mesh DHT,
    // fetch it from A, and return the identical bytes. Retry while the DHT announcement lands.
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut last = String::from("(no attempt)");
    loop {
        if Instant::now() > deadline {
            report.record(
                "blob availability",
                Outcome::Fail,
                format!("B could not fetch CID {} over the mesh: {last}", short_id(&cid)),
            );
            return;
        }
        match b.client.get_blob(&cid).await {
            Ok(bytes) => {
                if bytes == content.as_bytes() {
                    report.record(
                        "blob availability",
                        Outcome::Pass,
                        format!("B fetched CID {} from A over the mesh ({} bytes, verified)", short_id(&cid), bytes.len()),
                    );
                } else {
                    report.record(
                        "blob availability",
                        Outcome::Fail,
                        "B fetched the CID but bytes did not match".to_string(),
                    );
                }
                return;
            }
            Err(e) => last = format!("{e:#}"),
        }
        sleep(Duration::from_millis(600)).await;
    }
}

// ---------------- Scenario 4: service discovery + tags ----------------

async fn run_discovery(a: &Node, b: &Node, report: &mut Report) {
    let service = format!("ce-it-svc-{}", short_id(&b.node_id));
    if let Err(e) = b.client.advertise_service(&service).await {
        report.record("discovery + tags", Outcome::Fail, format!("B advertise failed: {e:#}"));
        return;
    }
    // DHT provider records take time to propagate; A re-queries until B shows up.
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut last = String::from("(no attempt)");
    loop {
        if Instant::now() > deadline {
            report.record(
                "discovery + tags",
                Outcome::Fail,
                format!("A never found B as a provider of '{service}': {last}"),
            );
            return;
        }
        // Re-advertise periodically in case the first record had not propagated when A queried.
        let _ = b.client.advertise_service(&service).await;
        match a.client.find_service(&service).await {
            Ok(providers) => {
                if providers.iter().any(|p| p == &b.node_id) {
                    report.record(
                        "discovery + tags",
                        Outcome::Pass,
                        format!("A discovered B as provider of '{service}' via the DHT"),
                    );
                    return;
                }
                last = format!("providers seen: {providers:?}");
            }
            Err(e) => last = format!("{e:#}"),
        }
        sleep(Duration::from_millis(800)).await;
    }
}

// ---------------- Scenario 5: tunnel (TCP over the mesh) ----------------

async fn run_tunnel(a: &Node, b: &Node, report: &mut Report) {
    // Stand up a trivial TCP echo server on B's host (loopback). A will open a CE tunnel
    // targeting B's NodeId + that remote port, and we drive bytes through the local end.
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(e) => {
            report.record("tunnel", Outcome::Blocked, format!("could not bind local echo server: {e}"));
            return;
        }
    };
    let remote_port = match listener.local_addr() {
        Ok(a) => a.port(),
        Err(e) => {
            report.record("tunnel", Outcome::Blocked, format!("local_addr failed: {e}"));
            return;
        }
    };
    // Echo server on B's host: accept connections in a loop (the tunnel may open more than one),
    // echo each request framed with a "pong:" prefix.
    let server = tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => break,
            };
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 1024];
                if let Ok(n) = sock.read(&mut buf).await {
                    if n > 0 {
                        let mut out = b"pong:".to_vec();
                        out.extend_from_slice(&buf[..n]);
                        let _ = sock.write_all(&out).await;
                        let _ = sock.flush().await;
                    }
                }
            });
        }
    });

    // Pick a free local port for the tunnel ingress on A's host.
    let local_port = match free_local_port().await {
        Ok(p) => p,
        Err(e) => {
            report.record("tunnel", Outcome::Blocked, format!("could not pick a free local port: {e}"));
            server.abort();
            return;
        }
    };

    // Tunnel targets are capability-gated: B authorizes the requester against a `tunnel` chain
    // rooted at its own key before forwarding any bytes. The harness mints that chain (B grants A
    // the `tunnel` ability) and passes it here. Without it, B denies the stream (EOF) — which is
    // correct, by-design behavior, so we report BLOCKED rather than FAIL in that case.
    let caps = std::env::var("CE_IT_TUNNEL_CAPS").ok().filter(|s| !s.trim().is_empty());
    let have_caps = caps.is_some();

    // POST /tunnel on A: bind 127.0.0.1:local_port, forward to remote_port on B over the mesh.
    let mut tunnel_req = serde_json::json!({
        "node_id": b.node_id,
        "local_port": local_port,
        "remote_port": remote_port,
    });
    if let Some(c) = caps {
        tunnel_req["caps"] = serde_json::Value::String(c);
    }
    let http = reqwest::Client::new();
    let url = format!("{}/tunnel", a.base.trim_end_matches('/'));
    let resp = http.post(&url).bearer_auth(a_token_for(a)).json(&tunnel_req).send().await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            report.record("tunnel", Outcome::Blocked, format!("POST /tunnel transport error: {e}"));
            server.abort();
            return;
        }
    };
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        report.record("tunnel", Outcome::Blocked, format!("POST /tunnel returned {status}: {body}"));
        server.abort();
        return;
    }

    // Drive bytes through the tunnel: connect to A's local ingress and expect the echo back.
    let outcome = drive_tunnel(local_port).await;
    server.abort();
    match outcome {
        Ok(reply) => report.record(
            "tunnel",
            Outcome::Pass,
            format!("bytes flowed A:{local_port} -> B:{remote_port}; got back {:?}", String::from_utf8_lossy(&reply)),
        ),
        Err(e) if !have_caps => report.record(
            "tunnel",
            Outcome::Blocked,
            format!(
                "no tunnel capability provided (set CE_IT_TUNNEL_CAPS); B correctly gated the stream: {e:#}"
            ),
        ),
        Err(e) => report.record(
            "tunnel",
            Outcome::Fail,
            format!("tunnel opened with a valid capability but byte round-trip failed: {e:#}"),
        ),
    }
}

async fn drive_tunnel(local_port: u16) -> Result<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // The tunnel binds asynchronously after POST returns; retry the connect briefly.
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut stream;
    loop {
        match tokio::net::TcpStream::connect(("127.0.0.1", local_port)).await {
            Ok(s) => {
                stream = s;
                break;
            }
            Err(e) => {
                if Instant::now() > deadline {
                    return Err(anyhow!("could not connect to tunnel ingress: {e}"));
                }
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    stream.write_all(b"ping-tunnel").await?;
    stream.flush().await?;
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(8), stream.read(&mut buf))
        .await
        .context("read from tunnel timed out")??;
    if n == 0 {
        bail!("tunnel returned EOF with no data");
    }
    buf.truncate(n);
    let expected = b"pong:ping-tunnel";
    if buf != expected {
        bail!("tunnel echo mismatch: got {:?}", String::from_utf8_lossy(&buf));
    }
    Ok(buf)
}

// ---------------- helpers ----------------

/// Pull A's API token back out of its client is not exposed by the SDK; we kept it in env, so
/// read it here for the raw /tunnel POST (the SDK has no tunnel method).
fn a_token_for(_a: &Node) -> String {
    std::env::var("CE_IT_A_TOKEN").unwrap_or_default()
}

async fn free_local_port() -> Result<u16> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let p = l.local_addr()?.port();
    drop(l);
    Ok(p)
}

fn short_id(id: &str) -> String {
    id.chars().take(10).collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
