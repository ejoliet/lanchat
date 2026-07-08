#!/usr/bin/env python3
"""
lanchat — offline LAN chat for events with no internet.

One laptop runs this. It (or a travel router / phone hotspot) provides
a local WiFi network. Attendees join that WiFi and open
http://<host-ip>:8000 in any browser. No internet, no installs, no deps.

AIDEV-NOTE: PeerJS/WebRTC stack intentionally NOT used here — PeerJS Cloud
signaling and public STUN require internet. Offline => star topology with
this process as the hub, SSE down + POST up. Python stdlib only so the
"download beforehand" requirement is just this one file.

Run:  python3 lanchat.py [port]
"""

import json
import queue
import socket
import sys
import threading
import time
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000

# AIDEV-NOTE: security preflight — cap all inbound data (burned: wallcast).
MAX_BODY_BYTES = 4096          # reject oversized POSTs outright
MAX_NAME_LEN = 24
MAX_MSG_LEN = 500
MAX_HISTORY = 200              # ring buffer of recent messages for late joiners
RATE_LIMIT_MSGS = 8            # per client
RATE_LIMIT_WINDOW = 10.0       # seconds

# ---------------------------------------------------------------- hub state
_lock = threading.Lock()
_history = []                  # list of event dicts
_clients = {}                  # client_id -> queue.Queue of SSE strings
_names = {}                    # client_id -> display name
_rate = {}                     # client_id -> list of send timestamps
_seq = 0


def _sse(event):
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


def broadcast(event):
    """Append to history and fan out to every connected SSE queue."""
    global _seq
    with _lock:
        _seq += 1
        event["seq"] = _seq
        event["ts"] = int(time.time() * 1000)
        _history.append(event)
        del _history[:-MAX_HISTORY]
        dead = []
        for cid, q in _clients.items():
            try:
                q.put_nowait(_sse(event))
            except queue.Full:
                dead.append(cid)  # slow consumer: drop, they can reconnect
        for cid in dead:
            _clients.pop(cid, None)


def roster():
    with _lock:
        return sorted(set(_names.values()))


def rate_ok(cid):
    now = time.time()
    with _lock:
        stamps = [t for t in _rate.get(cid, []) if now - t < RATE_LIMIT_WINDOW]
        if len(stamps) >= RATE_LIMIT_MSGS:
            _rate[cid] = stamps
            return False
        stamps.append(now)
        _rate[cid] = stamps
        return True


# ---------------------------------------------------------------- page
# AIDEV-NOTE: single embedded page, no external assets — must work with
# zero internet. All peer-supplied strings rendered via textContent (no
# innerHTML for user data; burned lesson: don't double-escape).
PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LAN Chat</title>
<style>
  :root { --bg:#0f1115; --panel:#181b22; --fg:#e8e8e8; --dim:#8a90a0;
          --me:#2f6feb; --sys:#3a3f4c; --accent:#39d353; }
  * { box-sizing:border-box; margin:0; }
  body { background:var(--bg); color:var(--fg);
         font:16px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;
         height:100dvh; display:flex; flex-direction:column; }
  header { padding:10px 14px; background:var(--panel);
           display:flex; justify-content:space-between; align-items:center; }
  header b { color:var(--accent); }
  #who { color:var(--dim); font-size:13px; max-width:55%; text-align:right;
         overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  #log { flex:1; overflow-y:auto; padding:12px; display:flex;
         flex-direction:column; gap:8px; }
  .msg { max-width:78%; padding:8px 12px; border-radius:12px;
         background:var(--panel); word-wrap:break-word; }
  .msg .n { font-size:12px; color:var(--dim); margin-bottom:2px; }
  .mine { align-self:flex-end; background:var(--me); }
  .mine .n { color:#cfe0ff; }
  .sys { align-self:center; background:none; color:var(--dim);
         font-size:13px; padding:2px; }
  form { display:flex; gap:8px; padding:10px; background:var(--panel); }
  input { flex:1; padding:10px 12px; border-radius:10px; border:none;
          background:#20242e; color:var(--fg); font-size:16px; outline:none; }
  button { padding:10px 18px; border:none; border-radius:10px;
           background:var(--me); color:#fff; font-size:16px; }
  #status { position:fixed; top:52px; left:50%; transform:translateX(-50%);
            background:#5c2b2b; padding:4px 12px; border-radius:8px;
            font-size:13px; display:none; }
</style>
</head>
<body>
<header><b>LAN Chat</b><span id="who"></span></header>
<div id="status">reconnecting…</div>
<div id="log"></div>
<form id="f">
  <input id="inp" autocomplete="off" placeholder="Message…" maxlength="500">
  <button>Send</button>
</form>
<script>
(function () {
  // AIDEV-NOTE: localStorage with in-memory fallback (locked-stack rule).
  var mem = {};
  var store = {
    get: function (k) { try { return localStorage.getItem("lc." + k); }
                        catch (e) { return mem[k] || null; } },
    set: function (k, v) { try { localStorage.setItem("lc." + k, v); }
                           catch (e) { mem[k] = v; } }
  };

  var name = store.get("name");
  while (!name || !name.trim()) {
    name = prompt("Your name for the chat:") || "";
  }
  name = name.trim().slice(0, 24);
  store.set("name", name);

  var cid = store.get("cid");
  if (!cid) {
    cid = Math.random().toString(36).slice(2, 10);
    store.set("cid", cid);
  }

  var log = document.getElementById("log");
  var who = document.getElementById("who");
  var statusEl = document.getElementById("status");
  var lastSeq = 0;

  function add(ev) {
    if (ev.seq && ev.seq <= lastSeq) return;   // dedupe on reconnect replay
    if (ev.seq) lastSeq = ev.seq;
    var d = document.createElement("div");
    if (ev.type === "sys") {
      d.className = "sys";
      d.textContent = ev.text;                 // textContent = XSS-safe
    } else if (ev.type === "msg") {
      d.className = "msg" + (ev.cid === cid ? " mine" : "");
      var n = document.createElement("div");
      n.className = "n";
      n.textContent = ev.name;
      var t = document.createElement("div");
      t.textContent = ev.text;
      d.appendChild(n); d.appendChild(t);
    } else if (ev.type === "roster") {
      who.textContent = ev.users.join(", ");
      return;
    } else { return; }
    var stick = log.scrollTop + log.clientHeight >= log.scrollHeight - 40;
    log.appendChild(d);
    if (stick) log.scrollTop = log.scrollHeight;
  }

  // AIDEV-NOTE: reconnect guard — single retry loop, capped backoff
  // (burned: Pop Rumble parallel reconnect loops).
  var retryPending = false, backoff = 1000;
  function connect() {
    var es = new EventSource("/events?cid=" + cid +
                             "&name=" + encodeURIComponent(name) +
                             "&since=" + lastSeq);
    es.onopen = function () { backoff = 1000; statusEl.style.display = "none"; };
    es.onmessage = function (e) { add(JSON.parse(e.data)); };
    es.onerror = function () {
      es.close();
      if (retryPending) return;
      retryPending = true;
      statusEl.style.display = "block";
      setTimeout(function () {
        retryPending = false;
        backoff = Math.min(backoff * 2, 15000);
        connect();
      }, backoff);
    };
  }
  connect();

  document.getElementById("f").addEventListener("submit", function (e) {
    e.preventDefault();
    var inp = document.getElementById("inp");
    var text = inp.value.trim();
    if (!text) return;
    inp.value = "";
    fetch("/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cid: cid, name: name, text: text })
    }).catch(function () { statusEl.style.display = "block"; });
  });
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------- handler
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # quiet console
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/events"):
            self._events()
            return

        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _events(self):
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(self.path).query)
        cid = (q.get("cid", [""])[0])[:16]
        name = escape((q.get("name", ["?"])[0]).strip()[:MAX_NAME_LEN]) or "?"
        try:
            since = int(q.get("since", ["0"])[0])
        except ValueError:
            since = 0
        if not cid:
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        ch = queue.Queue(maxsize=256)
        fresh = False
        with _lock:
            fresh = cid not in _names
            _names[cid] = name
            _clients[cid] = ch
            replay = [e for e in _history if e["seq"] > since]

        try:
            for e in replay:
                self.wfile.write(_sse(e).encode())
            self.wfile.flush()
            if fresh:
                broadcast({"type": "sys", "text": f"{name} joined"})
            broadcast({"type": "roster", "users": roster()})

            while True:
                try:
                    chunk = ch.get(timeout=20)
                    self.wfile.write(chunk.encode())
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")  # keepalive
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            gone = False
            with _lock:
                if _clients.get(cid) is ch:
                    _clients.pop(cid, None)
                    _names.pop(cid, None)
                    gone = True
            if gone:
                broadcast({"type": "sys", "text": f"{name} left"})
                broadcast({"type": "roster", "users": roster()})

    def do_POST(self):
        if self.path != "/send":
            self._json(404, {"ok": False})
            return
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0 or length > MAX_BODY_BYTES:
            self._json(413, {"ok": False, "err": "too big"})
            return
        try:
            data = json.loads(self.rfile.read(length))
            cid = str(data.get("cid", ""))[:16]
            name = escape(str(data.get("name", "?")).strip()[:MAX_NAME_LEN]) or "?"
            text = escape(str(data.get("text", "")).strip()[:MAX_MSG_LEN])
        except (json.JSONDecodeError, TypeError, ValueError):
            self._json(400, {"ok": False, "err": "bad json"})
            return
        if not cid or not text:
            self._json(400, {"ok": False, "err": "empty"})
            return
        if not rate_ok(cid):
            self._json(429, {"ok": False, "err": "slow down"})
            return
        broadcast({"type": "msg", "cid": cid, "name": name, "text": text})
        self._json(200, {"ok": True})


# ---------------------------------------------------------------- main
def lan_ip():
    """Best-effort local IP (no packets actually sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    ip = lan_ip()
    print("lanchat running.")
    print(f"  Attendees join your WiFi/hotspot, then open:  http://{ip}:{PORT}")
    print("  Ctrl-C to stop.")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()
