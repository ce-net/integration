# ce-integration — real two-node CE mesh harness

The multi-node confidence layer CE was missing. This boots a **2-node isolated test mesh** and
asserts CE's cross-node mesh paths **end-to-end against two live nodes** — not mocks, not a single
node talking to itself. It exercises the same `ce` release binary and the `ce-rs` SDK that apps use.

It **never touches the live node** on `127.0.0.1:8844`: both test nodes run on unique high ports
with `--no-mdns` (so LAN discovery cannot cross-link the live node) on throwaway `--ephemeral`
chains in `mktemp -d` data dirs that are removed on exit.

## Topology

```
node A  api :18901  p2p :14901   (bootstrap seed, --no-mine --ephemeral --no-mdns)
node B  api :18902  p2p :14902   (--bootstrap /ip4/127.0.0.1/tcp/14901/p2p/<A peer-id>)
```

`run.sh` reads A's libp2p peer-id from `GET /bootstrap`, splices in A's loopback listen address to
form a dialable multiaddr, and starts B bootstrapped to it. It waits for both `GET /health == ok`,
reads each node's `<data-dir>/api.token`, mints a `tunnel` capability (B grants A — see below), then
hands off to the Rust driver.

## Run

```bash
./run.sh                                            # default ports 1890x / 1490x
A_API=18911 A_P2P=14911 B_API=18912 B_P2P=14912 ./run.sh   # override ports (rerun in parallel)
CE_BIN=/path/to/ce ./run.sh                         # use a different ce binary
RUST_LOG=debug ./run.sh                             # verbose driver logging
```

Rerunnable: every node process and temp dir is killed/removed by an `EXIT/INT/TERM` trap, even on
failure or Ctrl-C. Exit code mirrors the driver: `0` iff every non-blocked scenario passed.

## Scenarios asserted (all over the real libp2p mesh)

| Scenario            | What it proves |
|---------------------|----------------|
| peering             | A and B actually peer (a directed `POST /mesh/send` from A is delivered to B) — the readiness gate. |
| mesh request/reply  | A `POST /mesh/request` → B's app reply loop answers via `POST /mesh/reply` → A receives the echoed payload. |
| pubsub              | B `POST /mesh/subscribe`s a topic, A `POST /mesh/publish`es, the message lands in B's inbox. |
| blob availability   | A `POST /blobs` (CID announced to the DHT) → B `GET /blobs/:cid` falls back to the mesh, fetches from A, bytes verified. This is the ce-pin content-availability path. |
| discovery + tags    | B `POST /discovery/advertise`s a service, A `GET /discovery/find/:service` resolves B as a provider via the DHT. |
| tunnel              | A `POST /tunnel` to a TCP port served on B's host; bytes round-trip through TCP-over-libp2p. **Capability-gated** (see below). |

### Why the tunnel needs a capability

The tunnel target authorizes the requester against a signed `ce-cap` chain (ability `tunnel`)
rooted at its own key **before forwarding any byte** — CE's capability-only trust model. The harness
mints that chain offline with `ce --data-dir <B> grant <A-node-id> --can tunnel` (B is the resource
owner) and passes the token as `caps` in A's `/tunnel` request. Without a token, B correctly closes
the stream (EOF) and the driver reports the scenario **BLOCKED** (by-design), not FAIL.

## Notes / blocked-by-environment paths

- **`/atlas` is not used as the peering gate.** The atlas is fed by CEP-1 capacity broadcasts that
  only run while mining; the test mesh runs `--no-mine`, so atlas stays empty. Peering is proven by
  a real mesh send instead. (Documented, not a defect.)
- **`mesh-deploy` (container placement) is intentionally not asserted here** — it requires Docker on
  the host and moves credits; it is covered by the job-lifecycle tests in `ce-node`. Add it here once
  a Docker-available CI lane exists.
- **rdev two-node file sync** and **ce-notes two-device share** are app-level (built on
  `request`/`reply` + `ce-cap`); they are out of scope for this primitives-level harness and are
  follow-ups (they need their own per-app fixtures and capability wallets).

## Layout

```
integration/
├── Cargo.toml        # ce-integration bin; depends on ce-rs by path
├── run.sh            # boots/tears down the 2-node mesh, mints caps, runs the driver
├── src/main.rs       # the assertion driver (PASS/FAIL/BLOCKED matrix)
└── README.md
```
