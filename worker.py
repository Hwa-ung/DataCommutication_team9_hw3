#!/usr/bin/env python3
"""HW#3 Worker — Fault-Tolerant Worker Ring"""
import socket, threading, time, argparse, sys, os, math
from collections import deque
from datetime import datetime

# ===================== Prime Counting =====================
def make_small_primes(limit=10000):
    sieve = bytearray(b'\x01' * (limit + 1))
    sieve[0] = sieve[1] = 0
    for p in range(2, int(math.isqrt(limit)) + 1):
        if sieve[p]:
            sieve[p*p::p] = bytearray(len(sieve[p*p::p]))
    return [n for n in range(2, limit + 1) if sieve[n]]

SMALL = make_small_primes()  # 1229 primes, last 9973

def count_primes(k, s):
    """Count primes in step s of task k."""
    lo = k * 100_000 + (s - 1) * 10_000 + 1
    hi = lo + 9999
    mark = bytearray(b'\x01' * (hi - lo + 1))
    for p in SMALL:
        if p * p > hi:
            break
        start = max(p * p, ((lo + p - 1) // p) * p)
        for m in range(start - lo, hi - lo + 1, p):
            mark[m] = 0
    if lo <= 1:
        mark[1 - lo] = 0
    return sum(mark)

# ===================== Shared State =====================
class G:
    """Mutable global state (avoids 'global' keyword)."""
    wid = 0
    step_delay = 100
    # state
    state = 'CONNECTING'     # CONNECTING/RUNNING/CRASHED/REJOINING
    crash_gen = 0
    stopping = False
    start_received = False
    launcher_connected = False
    ready_sent = False
    # queue
    requested = False
    no_task = False
    # sockets
    coord_sock = None
    buddy_sock = None
    launcher_sock = None
    # stats
    ckpt_sent = 0; ckpt_recv = 0; tasks_done = 0
    crash_count = 0; ack_n = 0; reject_n = 0

state_lock = threading.Lock()
state_cv = threading.Condition(state_lock)
q_lock = threading.Lock()
q_cv = threading.Condition(q_lock)
store_lock = threading.Lock()

local_q = deque()       # (k, e, start_step, partial)
store = {}              # k -> (epoch, step, partial)

coord_slock = threading.Lock()   # coord send lock
buddy_slock = threading.Lock()   # buddy send lock
launcher_slock = threading.Lock()

logf = None; logL = threading.Lock()

def _ts():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

def L(tag, msg):
    line = f"[{_ts()}] [W{G.wid}] [{tag}] {msg}"
    print(line, flush=True)
    if logf:
        with logL:
            logf.write(line + '\n')
            logf.flush()

# ===================== Send Helpers =====================
def _send_raw(sock, lock, msg):
    data = (msg + '\n').encode()
    with lock:
        try:
            sock.sendall(data)
        except OSError as e:
            L("WARN", f"send fail ({e}): {msg[:60]}")

def send_coord(msg):
    if G.coord_sock:
        _send_raw(G.coord_sock, coord_slock, msg)

def send_buddy(msg):
    if G.buddy_sock:
        _send_raw(G.buddy_sock, buddy_slock, msg)

def send_launcher(msg):
    if G.launcher_sock:
        _send_raw(G.launcher_sock, launcher_slock, msg)

def send_if_alive(gen, target, msg):
    """Check state+gen, then send. Gap between check and send is OK (epoch protects)."""
    with state_lock:
        if G.state != 'RUNNING' or G.crash_gen != gen:
            return False
    if target == 'coord':
        send_coord(msg)
    elif target == 'buddy':
        send_buddy(msg)
    return True

def recv_line(sock, buf):
    """Blocking read of one \\n-delimited line. Returns None on EOF/close."""
    while b'\n' not in buf:
        try:
            sock.settimeout(1.0)
            data = sock.recv(8192)
        except socket.timeout:
            if G.stopping:
                return None
            continue
        except (OSError, ConnectionError):
            return None
        if not data:
            return None
        buf.extend(data)
    idx = buf.index(b'\n')
    line = bytes(buf[:idx])
    del buf[:idx + 1]
    return line.decode(errors='replace').strip()

# ===================== Handlers =====================
def on_crash():
    with state_lock:
        G.state = 'CRASHED'
        G.crash_gen += 1
        gen = G.crash_gen
        state_cv.notify_all()
    with q_lock:
        local_q.clear()
        G.requested = False
        G.no_task = False
        q_cv.notify_all()
    with store_lock:
        store.clear()
    G.crash_count += 1
    L("CRASH", f"gen={gen}")

def on_restart():
    with state_lock:
        G.state = 'REJOINING'
        state_cv.notify_all()
    L("RESTART", "-> REJOINING")
    send_coord(f"REJOIN {G.wid}")

def on_welcome():
    with state_lock:
        G.state = 'RUNNING'
        G.start_received = True
        state_cv.notify_all()
    send_launcher(f"REJOINED {G.wid}")
    with q_lock:
        q_cv.notify_all()
    L("WELCOME", "-> RUNNING")

def on_assign(k, e):
    with q_lock:
        local_q.append((k, e, 1, 0))
        G.requested = False
        q_cv.notify_all()
    L("ASSIGN", f"Task {k} e={e}")

def on_takeover(k, e, d):
    with q_lock:
        G.no_task = False
        with store_lock:
            ck = store.get(k)
        if ck:
            s0, partial = ck[1] + 1, ck[2]
            L("TAKEOVER", f"Task {k} e={e} from W{d} CKPT(s={ck[1]},p={ck[2]})")
        else:
            s0, partial = 1, 0
            L("TAKEOVER", f"Task {k} e={e} from W{d} no-CKPT")
        local_q.append((k, e, s0, partial))
        q_cv.notify_all()

def on_no_task():
    with q_lock:
        G.no_task = True
        G.requested = False
    L("NO_TASK", "")

def on_retry_later():
    L("RETRY_LATER", "retry REJOIN in 2s")
    def _retry():
        time.sleep(2.0)
        with state_lock:
            should_send = (G.state == 'REJOINING')
        if should_send:
            send_coord(f"REJOIN {G.wid}")
            L("REJOIN", "retry")
    threading.Thread(target=_retry, daemon=True).start()

def on_bye():
    with state_lock:
        G.stopping = True
        state_cv.notify_all()
    with q_lock:
        q_cv.notify_all()
    L("BYE", "received")

# ===================== Threads =====================
def peer_server_thread(peer_srv):
    while not G.stopping:
        peer_srv.settimeout(1.0)
        try:
            conn, addr = peer_srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        threading.Thread(target=_peer_dispatch, args=(conn,), daemon=True).start()

def _peer_dispatch(conn):
    buf = bytearray()
    line = recv_line(conn, buf)
    if line is None:
        conn.close()
        return
    parts = line.split()
    if parts[0] == 'PEER':
        _ckpt_handler(conn, buf, int(parts[1]))
    elif parts[0] == 'LAUNCHER':
        _launcher_handler(conn, buf)
    else:
        conn.close()

def _ckpt_handler(conn, buf, from_wid):
    L("PEER", f"CKPT conn from W{from_wid}")
    while not G.stopping:
        line = recv_line(conn, buf)
        if line is None:
            break
        parts = line.split()
        if parts[0] != 'CKPT' or len(parts) < 5:
            continue
        k, e, s, p = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
        with state_lock:
            if G.state == 'CRASHED':
                continue
        with store_lock:
            ex = store.get(k)
            if ex is None or (e, s) > (ex[0], ex[1]):
                store[k] = (e, s, p)
        G.ckpt_recv += 1

def _launcher_handler(conn, buf):
    G.launcher_sock = conn
    L("LAUNCHER", "control connected")
    with state_lock:
        G.launcher_connected = True
        do_ready = G.start_received and not G.ready_sent
        if do_ready:
            G.ready_sent = True
    if do_ready:
        send_launcher(f"READY {G.wid}")
        L("READY", "-> Launcher (from launcher connect)")
    while not G.stopping:
        line = recv_line(conn, buf)
        if line is None:
            break
        cmd = line.strip().split()[0] if line.strip() else ''
        if cmd == 'CRASH':
            on_crash()
        elif cmd == 'RESTART':
            on_restart()

def control_recv_loop(buf):
    """Read from coordinator socket."""
    while not G.stopping:
        line = recv_line(G.coord_sock, buf)
        if line is None:
            if not G.stopping:
                L("ERROR", "coordinator disconnected")
            break
        parts = line.split()
        if not parts:
            continue
        cmd = parts[0]
        # CRASHED -> discard all except BYE
        with state_lock:
            crashed = (G.state == 'CRASHED')
        if crashed and cmd != 'BYE':
            continue
        if   cmd == 'ASSIGN':        on_assign(int(parts[1]), int(parts[2]))
        elif cmd == 'TAKEOVER':      on_takeover(int(parts[1]), int(parts[2]), int(parts[3]))
        elif cmd == 'ACK':           G.ack_n += 1; L("ACK", f"Task {parts[1]}")
        elif cmd == 'REJECT_STALE':  G.reject_n += 1; L("REJECT", f"Task {parts[1]} {' '.join(parts[2:])}")
        elif cmd == 'NO_TASK':       on_no_task()
        elif cmd == 'WELCOME':       on_welcome()
        elif cmd == 'RETRY_LATER':   on_retry_later()
        elif cmd == 'BYE':           on_bye(); break
        else:                        L("WARN", f"unknown coord msg: {line[:60]}")

def hb_loop():
    while not G.stopping:
        with state_lock:
            while G.state != 'RUNNING' and not G.stopping:
                state_cv.wait()
            if G.stopping:
                return
            gen = G.crash_gen
        send_if_alive(gen, 'coord', f"HB {G.wid}")
        with state_lock:
            if not G.stopping and G.state == 'RUNNING':
                state_cv.wait(timeout=1.0)

def take_task():
    """Block until a task is available or stopping. Returns (k,e,s0,partial,gen) or None."""
    while True:
        # wait until RUNNING or stop
        with state_lock:
            while G.state != 'RUNNING' and not G.stopping:
                state_cv.wait()
            if G.stopping:
                return None
        need_req = False
        gen = 0
        with q_lock:
            with state_lock:
                running = (G.state == 'RUNNING')
                gen = G.crash_gen
            if G.stopping:
                return None
            if running and local_q:
                item = local_q.popleft()
                return (*item, gen)
            need_req = running and not local_q and not G.requested and not G.no_task
            if need_req:
                G.requested = True
            else:
                q_cv.wait()
        if need_req:
            send_if_alive(gen, 'coord', f"REQ {G.wid}")

def compute_loop():
    while not G.stopping:
        result = take_task()
        if result is None:
            break
        k, e, s0, partial, gen = result
        L("COMPUTE", f"Task {k} e={e} s0={s0} partial={partial}")
        for s in range(s0, 11):
            if G.crash_gen != gen:
                break
            partial += count_primes(k, s)
            time.sleep(G.step_delay / 1000.0)
            if s < 10:
                if not send_if_alive(gen, 'buddy', f"CKPT {k} {e} {s} {partial}"):
                    break
                G.ckpt_sent += 1
        else:
            if send_if_alive(gen, 'coord', f"RESULT {k} {e} {partial}"):
                G.tasks_done += 1
                L("RESULT", f"Task {k} e={e} v={partial}")

# ===================== Main =====================
def main():
    global logf
    ap = argparse.ArgumentParser(description="HW#3 Worker")
    ap.add_argument('--id', type=int, required=True)
    ap.add_argument('--coord-ip', required=True)
    ap.add_argument('--coord-port', type=int, required=True)
    ap.add_argument('--peer-ip', default='127.0.0.1')
    ap.add_argument('--step-delay', type=int, default=100)
    ap.add_argument('--log-dir', default='logs')
    args = ap.parse_args()

    G.wid = args.id
    G.step_delay = args.step_delay
    peer_port = 7000 + G.wid
    os.makedirs(args.log_dir, exist_ok=True)
    logf = open(os.path.join(args.log_dir, f'worker_{G.wid}.log'), 'w', encoding='utf-8')

    L("INIT", f"id={G.wid} peer_port={peer_port} step_delay={G.step_delay}ms small_primes={len(SMALL)}")

    # --- peer server ---
    peer_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    peer_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    peer_srv.bind(('0.0.0.0', peer_port))
    peer_srv.listen(5)
    threading.Thread(target=peer_server_thread, args=(peer_srv,), daemon=True).start()

    # --- connect to coordinator ---
    while True:
        try:
            G.coord_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            G.coord_sock.connect((args.coord_ip, args.coord_port))
            break
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    G.coord_sock.sendall(f"HELLO {G.wid} {args.peer_ip} {peer_port}\n".encode())
    L("HELLO", f"sent ({args.peer_ip}:{peer_port})")

    # --- wait for RING ---
    buf = bytearray()
    while True:
        line = recv_line(G.coord_sock, buf)
        if line is None:
            L("ERROR", "disconnected before RING"); return
        parts = line.split()
        if parts[0] == 'RING':
            buddy_id = int(parts[1]); buddy_ip = parts[2]; buddy_port = int(parts[3])
            break
    L("RING", f"buddy=W{buddy_id} {buddy_ip}:{buddy_port}")

    # --- connect to buddy ---
    while True:
        try:
            G.buddy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            G.buddy_sock.connect((buddy_ip, buddy_port))
            break
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    G.buddy_sock.sendall(f"PEER {G.wid}\n".encode())
    G.coord_sock.sendall(f"RING_OK {G.wid}\n".encode())
    L("RING_OK", "sent")

    # --- wait for START ---
    while True:
        line = recv_line(G.coord_sock, buf)
        if line is None:
            L("ERROR", "disconnected before START"); return
        if line.strip() == 'START':
            break

    with state_lock:
        G.state = 'RUNNING'
        G.start_received = True
        do_ready = G.launcher_connected and not G.ready_sent
        if do_ready:
            G.ready_sent = True
        state_cv.notify_all()
    if do_ready:
        send_launcher(f"READY {G.wid}")
        L("READY", "-> Launcher (from START)")
    L("START", "RUNNING")

    # --- start runtime threads ---
    threads = []
    for fn, a in [(control_recv_loop, (buf,)), (hb_loop, ()), (compute_loop, ())]:
        t = threading.Thread(target=fn, args=a, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=600)

    L("TERMINATE", f"tasks={G.tasks_done} ckpt_sent={G.ckpt_sent} ckpt_recv={G.ckpt_recv} crashes={G.crash_count} ack={G.ack_n} reject={G.reject_n}")
    if logf:
        logf.close()

if __name__ == '__main__':
    main()
