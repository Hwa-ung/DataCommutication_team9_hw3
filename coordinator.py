#!/usr/bin/env python3
"""HW#3 Coordinator — Fault-Tolerant Worker Ring"""
import socket, select, threading, time, argparse, sys, os
from collections import deque
from datetime import datetime

# --- Constants ---
PENDING, ASSIGNED, DONE = 'PENDING', 'ASSIGNED', 'DONE'
ALIVE, SUSPECT, DEAD = 'ALIVE', 'SUSPECT', 'DEAD'
N_W = 10    # workers
N_T = 1000  # tasks
N_D = 6     # dispatchers

# --- Data Structures ---
class Task:
    __slots__ = ('state','owner','epoch','result','cc','lock')
    def __init__(self):
        self.state = PENDING; self.owner = None; self.epoch = 0
        self.result = None; self.cc = 0; self.lock = threading.Lock()

class WEntry:
    __slots__ = ('state','lastHB','rjc','sock','slock','pip','pport','rok')
    def __init__(self):
        self.state = ALIVE; self.lastHB = 0.0; self.rjc = 0
        self.sock = None; self.slock = threading.Lock()
        self.pip = None; self.pport = 0; self.rok = False

# --- Globals ---
tasks = [Task() for _ in range(N_T)]
wk = {}                         # wid -> WEntry
wl = threading.Lock()           # worker-table lock
pq = deque(range(N_T))          # pending queue (access inside wl)
s2w = {}                        # sock -> wid
rbufs = {}                      # sock -> bytearray

rq = deque()                    # request queue
rqcv = threading.Condition(threading.Lock())

done_n = 0                      # committed count
done_sum = 0                    # running total
done_cv = threading.Condition(threading.Lock())

t0 = None                       # start time (set after 10 RING_OKs)
stop = False
n_hello = 0
n_rok = 0

# stats
st_dead = 0; st_rj = 0; st_tk = 0; st_rej = 0
logf = None; logL = threading.Lock()

def _ts():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

def L(tag, msg):
    line = f"[{_ts()}] [{tag}] {msg}"
    print(line, flush=True)
    if logf:
        with logL:
            logf.write(line + '\n')
            logf.flush()

def _send(wid, msg):
    """Send \n-terminated msg to worker wid. Must NOT hold any lock."""
    w = wk.get(wid)
    if not w or not w.sock:
        return
    data = (msg + '\n').encode()
    with w.slock:
        try:
            w.sock.sendall(data)
        except OSError:
            L("WARN", f"send W{wid} fail: {msg[:60]}")

# ===================== Listener =====================
def listener_thread(srv):
    socks = [srv]
    while not stop:
        try:
            rd, _, _ = select.select(socks, [], [], 1.0)
        except Exception:
            if stop: break
            continue
        for s in rd:
            if s is srv:
                try:
                    c, _ = srv.accept()
                    c.setblocking(False)
                    socks.append(c)
                    rbufs[c] = bytearray()
                except OSError:
                    pass
                continue
            # worker socket
            try:
                data = s.recv(8192)
            except BlockingIOError:
                continue
            except (OSError, ConnectionError):
                data = b''
            if not data:
                if s in socks:
                    socks.remove(s)
                rbufs.pop(s, None)
                s2w.pop(s, None)
                try: s.close()
                except: pass
                continue
            buf = rbufs.setdefault(s, bytearray())
            buf.extend(data)
            while b'\n' in buf:
                idx = buf.index(b'\n')
                line = bytes(buf[:idx])
                del buf[:idx+1]
                msg = line.decode(errors='replace').strip()
                if not msg:
                    continue
                parts = msg.split()
                cmd = parts[0]
                # HB: handle directly (no queue delay)
                if cmd == 'HB' and len(parts) >= 2:
                    try:
                        wid = int(parts[1])
                    except ValueError:
                        continue
                    with wl:
                        w = wk.get(wid)
                        if w and w.state in (ALIVE, SUSPECT):
                            if w.state == SUSPECT:
                                L("FD", f"W{wid} SUSPECT->ALIVE (HB)")
                                w.state = ALIVE
                            w.lastHB = time.time()
                else:
                    with rqcv:
                        rq.append((s, msg))
                        rqcv.notify()

# ===================== Dispatcher =====================
def dispatcher_thread(did):
    while True:
        with rqcv:
            while not rq and not stop:
                rqcv.wait(timeout=1.0)
            if stop and not rq:
                return
            if not rq:
                continue
            item = rq.popleft()
        if item is None:
            return
        sock, msg = item
        parts = msg.split()
        cmd = parts[0]
        try:
            if   cmd == 'HELLO':    _on_hello(sock, parts, did)
            elif cmd == 'RING_OK':  _on_ring_ok(parts, did)
            elif cmd == 'REQ':      _on_req(sock, parts, did)
            elif cmd == 'RESULT':   _on_result(sock, parts, did)
            elif cmd == 'REJOIN':   _on_rejoin(parts, did)
            else:
                L("WARN", f"D{did} unknown cmd: {msg[:80]}")
        except Exception as e:
            L("ERROR", f"D{did} {cmd}: {e}")

# --- HELLO ---
def _on_hello(sock, parts, did):
    global n_hello
    if len(parts) < 4:
        return
    wid = int(parts[1]); ip = parts[2]; port = int(parts[3])
    if wid < 1 or wid > N_W:
        return
    ring_msgs = None
    with wl:
        if t0 is not None:
            return
        if wid in wk and wk[wid].sock is not None:
            return
        if wid not in wk:
            wk[wid] = WEntry()
        w = wk[wid]
        w.sock = sock; w.pip = ip; w.pport = port
        s2w[sock] = wid
        n_hello += 1
        L("HELLO", f"W{wid} ({ip}:{port}) [{n_hello}/{N_W}]")
        if n_hello == N_W:
            ring_msgs = []
            for i in range(1, N_W + 1):
                b = i % N_W + 1
                wb = wk[b]
                ring_msgs.append((i, f"RING {b} {wb.pip} {wb.pport}"))
    if ring_msgs:
        for wid_t, rm in ring_msgs:
            _send(wid_t, rm)
        L("RING", f"sent RING to all {N_W} workers")

# --- RING_OK ---
def _on_ring_ok(parts, did):
    global n_rok, t0
    wid = int(parts[1])
    do_start = False
    with wl:
        w = wk.get(wid)
        if not w or w.rok:
            return
        w.rok = True
        n_rok += 1
        L("RING_OK", f"W{wid} [{n_rok}/{N_W}]")
        if n_rok == N_W:
            t0 = time.time()
            for i in range(1, N_W + 1):
                wk[i].lastHB = t0
            do_start = True
    if do_start:
        L("START", "-> all workers")
        for i in range(1, N_W + 1):
            _send(i, "START")

# --- REQ ---
def _on_req(sock, parts, did):
    if t0 is None:
        return
    wid = int(parts[1])
    action = None; k = -1
    with wl:
        w = wk.get(wid)
        if not w or w.state == DEAD:
            L("WARN", f"D{did} REQ W{wid} DEAD/missing")
            return
        if not pq:
            action = 'NO_TASK'
        else:
            k = pq.popleft()
            with tasks[k].lock:
                tasks[k].state = ASSIGNED
                tasks[k].owner = wid
                tasks[k].epoch = 1
            action = 'ASSIGN'
    if action == 'NO_TASK':
        _send(wid, "NO_TASK")
        L("REQ", f"D{did} W{wid} -> NO_TASK")
    else:
        _send(wid, f"ASSIGN {k} 1")
        L("ASSIGN", f"D{did} Task {k} -> W{wid} e=1 pend={len(pq)}")

# --- RESULT ---
def _on_result(sock, parts, did):
    global done_n, done_sum, st_rej
    if len(parts) < 4:
        return
    k = int(parts[1]); e = int(parts[2]); v = int(parts[3])
    wid = s2w.get(sock)
    if wid is None:
        L("WARN", f"D{did} RESULT unknown sock")
        return
    # (1) sender DEAD?
    with wl:
        w = wk.get(wid)
        is_dead = (not w or w.state == DEAD)
    if is_dead:
        _send(wid, f"REJECT_STALE {k} DEAD")
        L("REJECT", f"D{did} Task {k} W{wid} DEAD")
        st_rej += 1
        return
    # (2)-(4) task lock
    t = tasks[k]
    reason = None
    with t.lock:
        if t.state == DONE:
            reason = "DONE"
        elif e != t.epoch or wid != t.owner:
            reason = "EPOCH"
        else:
            t.state = DONE
            t.result = v
            t.cc += 1
    if reason:
        _send(wid, f"REJECT_STALE {k} {reason}")
        L("REJECT", f"D{did} Task {k} e={e} W{wid} -> {reason}")
        st_rej += 1
    else:
        with done_cv:
            done_n += 1
            done_sum += v
            dn = done_n
            if done_n >= N_T:
                done_cv.notify_all()
        _send(wid, f"ACK {k}")
        L("COMMIT", f"D{did} Task {k} e={e} W{wid} v={v} [{dn}/{N_T}]")

# --- REJOIN ---
def _on_rejoin(parts, did):
    global st_rj
    wid = int(parts[1])
    action = None
    with wl:
        w = wk.get(wid)
        if not w:
            return
        if w.state == DEAD:
            w.state = ALIVE
            w.lastHB = time.time()
            w.rjc += 1
            action = 'WELCOME'
            st_rj += 1
            L("REJOIN", f"D{did} W{wid} DEAD->ALIVE (rejoin #{w.rjc})")
        else:
            action = 'RETRY_LATER'
            L("REJOIN", f"D{did} W{wid} state={w.state} -> RETRY_LATER")
    if action == 'WELCOME':
        _send(wid, "WELCOME")
    else:
        _send(wid, "RETRY_LATER")

# ===================== Failure Detector =====================
def _next_alive(d):
    """Ring order, skip DEAD and SUSPECT. Fallback to SUSPECT. wl held."""
    for off in range(1, N_W):
        c = (d - 1 + off) % N_W + 1
        if wk[c].state == ALIVE:
            return c
    for off in range(1, N_W):
        c = (d - 1 + off) % N_W + 1
        if wk[c].state == SUSPECT:
            return c
    return None

def fd_thread():
    global st_dead, st_tk
    last_pool = 0.0
    while t0 is None and not stop:
        time.sleep(0.1)
    while not stop:
        time.sleep(1.0)
        if stop:
            break
        now = time.time()
        newly_dead = []
        takeover_msgs = []
        with wl:
            # --- mark states ---
            for i in range(1, N_W + 1):
                w = wk.get(i)
                if not w:
                    continue
                gap = now - w.lastHB
                if w.state == ALIVE and gap >= 3.0:
                    w.state = SUSPECT
                    L("FD", f"W{i} ->SUSPECT ({gap:.1f}s)")
                if w.state in (ALIVE, SUSPECT) and gap >= 5.0:
                    w.state = DEAD
                    newly_dead.append(i)
                    L("FD", f"W{i} ->DEAD ({gap:.1f}s)")
                    st_dead += 1
            # --- takeover ---
            for d in sorted(newly_dead):
                b = _next_alive(d)
                if b is None:
                    L("ERROR", f"No target for W{d}")
                    continue
                for k in range(N_T):
                    t = tasks[k]
                    with t.lock:
                        if t.state == ASSIGNED and t.owner == d:
                            t.epoch += 1
                            t.owner = b
                            takeover_msgs.append((b, k, t.epoch, d))
        # send outside lock
        for b, k, e, d in takeover_msgs:
            _send(b, f"TAKEOVER {k} {e} {d}")
            L("TAKEOVER", f"Task {k} e={e} W{d}->W{b}")
            st_tk += 1
        # --- POOL log ---
        if now - last_pool >= 5.0:
            last_pool = now
            with done_cv:
                dn = done_n
            with wl:
                pn = len(pq)
                al = [i for i in range(1, N_W+1) if wk[i].state == ALIVE]
                dl = [i for i in range(1, N_W+1) if wk[i].state == DEAD]
            L("POOL", f"rq={len(rq)} pend={pn} done={dn} alive={al} dead={dl}")

# ===================== Main =====================
def main():
    global logf, stop, done_n, done_sum
    ap = argparse.ArgumentParser(description="HW#3 Coordinator")
    ap.add_argument('--host', default='0.0.0.0')
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--log-dir', default='logs')
    args = ap.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)
    logf = open(os.path.join(args.log_dir, 'coordinator.log'), 'w', encoding='utf-8')

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(20)
    srv.setblocking(False)
    L("INIT", f"Coordinator on {args.host}:{args.port}")

    # start threads
    threads = []
    t = threading.Thread(target=listener_thread, args=(srv,), daemon=True); t.start(); threads.append(t)
    for d in range(1, N_D + 1):
        t = threading.Thread(target=dispatcher_thread, args=(d,), daemon=True); t.start(); threads.append(t)
    t = threading.Thread(target=fd_thread, daemon=True); t.start(); threads.append(t)

    # wait for 1000 DONE
    with done_cv:
        while done_n < N_T:
            done_cv.wait(timeout=1.0)

    elapsed = time.time() - t0 if t0 else 0
    # VERIFY
    all_done = all(tasks[k].state == DONE for k in range(N_T))
    total = sum(tasks[k].result for k in range(N_T) if tasks[k].result is not None)
    all_once = all(tasks[k].cc == 1 for k in range(N_T))
    dbl = sum(1 for k in range(N_T) if tasks[k].cc > 1)
    L("VERIFY", f"total={total} all_done={all_done} all_commit_once={all_once} double={dbl} elapsed={elapsed:.1f}s")
    if total == 5761455 and all_done and all_once:
        L("VERIFY", "PASS")
    else:
        L("VERIFY", "FAIL")

    # BYE
    for i in range(1, N_W + 1):
        _send(i, "BYE")
    L("BYE", "-> all workers")

    time.sleep(2)
    stop = True
    with rqcv:
        rqcv.notify_all()

    L("TERMINATE", f"elapsed={elapsed:.1f}s total={total} dead={st_dead} takeover={st_tk} rejoin={st_rj} reject={st_rej}")

    if logf:
        logf.close()
    srv.close()
    sys.exit(0)

if __name__ == '__main__':
    main()
