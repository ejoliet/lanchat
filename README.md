# HANDOFF — lanchat

> Local-only. Gitignore this file if repo is created. Committed invariants go to CLAUDE.md.

## You are picking up

**lanchat** — offline LAN chat for events with no internet. Single file: `lanchat.py`. Python 3 stdlib only, zero pip deps. Status: **v1 working, smoke-tested.**

## Context you need

- Use case: event venue, no WiFi/cellular. One laptop runs hotspot + this server. Attendees join hotspot, open `http://<ip>:8000`.
- **Architecture decision (do not re-litigate):** Emmanuel's locked P2P stack (PeerJS 1.5.5, zero-backend) was deliberately NOT used. PeerJS Cloud signaling + public STUN require internet. Offline forces a local hub. Star topology, this process = hub. SSE down, POST up. See `AIDEV-NOTE` at top of file.
- Stdlib-only is a hard constraint: "download beforehand" must mean one file, no `pip install` at the venue.

## Invariants (violate = broken)

1. **Zero pip dependencies.** stdlib only. If you need websockets, that is a v2 fork with explicit approval, not a drift.
2. **Single file.** Page is embedded in `PAGE`. No external assets — nothing loads from internet.
3. **Cap all inbound data.** `MAX_BODY_BYTES=4096`, `MAX_MSG_LEN=500`, `MAX_NAME_LEN=24`, rate limit 8 msgs/10 s per cid. Never remove; unbounded intake = hub memory exhaustion (wallcast/cullroom burn).
4. **XSS: escape server-side (`html.escape`) AND render via `textContent` client-side.** textContent needs no additional escaping — do not double-escape (Pop Rumble burn).
5. **Single reconnect loop client-side.** `retryPending` guard + capped backoff. Do not add parallel error listeners (Pop Rumble burn).
6. `lc.*` namespaced localStorage with in-memory fallback (locked-stack rule).
7. No secrets in this project. Keep it that way.

## Current mechanics (read before touching)

- `broadcast()`: seq-stamped events → ring buffer (`MAX_HISTORY=200`) → per-client `queue.Queue(256)`. Slow consumers dropped; they reconnect and replay via `?since=<seq>`.
- SSE handler: replay history > since, announce join, then block on queue with 20 s timeout → `: ping` keepalive.
- Client dedupes on `seq` — reconnect replay is idempotent.
- Leave detection: SSE connection death → pop client → broadcast "left" + roster.

## Known limits / open items

- `AIDEV-TODO` candidates (none committed yet):
  - Hub dies → chat dies. No failover by design (nothing else can serve the page).
  - ~50–100 attendees ceiling (ThreadingHTTPServer, one thread per SSE stream). Past that: v2 with `websockets` lib or shard.
  - No message persistence across server restart (in-memory only). Acceptable for events; add JSON-lines append log only if asked.
  - Name collisions allowed (two "Alice"s). cid distinguishes internally. Fine.
- Not tested: multi-device on real hotspot. **Manual two-phone test required before any event use.**

## Burn log

| Item | Lesson |
|---|---|
| First smoke test interleaved two curl outputs in same tmp file | Test artifact, not a server bug — single writer per SSE connection confirmed. Use fresh tmp files per test run |
| Step-level timeout killed compound test command | Background the server with nohup + pidfile, hard `timeout` on SSE curl |

## Test procedure (rerun after any change)

```bash
python3 -c "import py_compile; py_compile.compile('lanchat.py', doraise=True)"
nohup python3 lanchat.py 8765 >/tmp/lc.log 2>&1 & echo $! > /tmp/lc.pid
timeout 6 curl -sN "http://127.0.0.1:8765/events?cid=t1&name=A&since=0" > /tmp/sse.$$ &
sleep 1
curl -s -X POST http://127.0.0.1:8765/send -H "Content-Type: application/json" \
  -d '{"cid":"t1","name":"A","text":"<b>x</b>"}'          # expect ok:true
# rapid-fire 9 sends → expect 429 on 9th; check /tmp/sse.$$ shows &lt;b&gt;
kill $(cat /tmp/lc.pid)
```

Pass criteria: page 200, XSS payload arrives escaped, 429 after 8th rapid send, reconnect with `since` replays without duplicates.

## Workflow expectations (Emmanuel's conventions)

- Plan before code for multi-file changes; confirm plan first.
- RDD spec (readme-driven-dev skill) before any v2 rewrite.
- `AIDEV-` comments on non-obvious mechanisms only.
- Caveman-terse communication. No praise, no filler.
- If repo is created: MIT license, README with the 3-step event setup, this file gitignored, invariants above → CLAUDE.md.

## Next actions for you

1. Two-phone hotspot test. Log results here.
2. If asked for v2: spike WebSocket version first (hardest primitive = concurrent fan-out at 100+ clients), GO/NO-GO before README.
3. If asked for QR: stdlib-only QR is painful; suggest printing URL or a pregenerated QR image — do not add a dependency for it.
