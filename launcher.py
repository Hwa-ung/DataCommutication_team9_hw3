#!/usr/bin/env python3
"""HW#3 Chaos Launcher — starts Workers and injects crashes"""
import socket, select, subprocess, threading, time, random, argparse, sys, os
from datetime import datetime

N_W = 10

class G:
    t0 = None
    stop = False

status = {}        # wid -> 'RUNNING'/'CRASHED'/'REJOINING'/'EXITED'
ctrl_socks = {}    # wid -> socket
ctrl_bufs = {}     # wid -> bytearray
sock2wid = {}      # socket -> wid
procs = {}         # wid -> Popen
restart_at = {}    # wid -> time
events = []        # crash event records

logf = None; logL = threading.Lock()

def _ts():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]

def L(tag, msg):
    line = f"[{_ts()}] [LAUNCHER] [{tag}] {msg}"
    print(line, flush=True)
    if logf:
        with logL:
            logf.write(line + '\n')
            logf.flush()

def recv_lines_nb(sock, buf):
    """Non-blocking: read available data, return complete lines."""
    lines = []
    try:
        data = sock.recv(4096)
        if not data:
            return None  # EOF
        buf.extend(data)
    except BlockingIOError:
        pass
    except (OSError, ConnectionError):
        return None
    while b'\n' in buf:
        idx = buf.index(b'\n')
        line = bytes(buf[:idx]).decode(errors='replace').strip()
        del buf[:idx + 1]
        if line:
            lines.append(line)
    return lines

def connect_ctrl(wid, port, retries=40):
    """Connect to worker's peer port, send LAUNCHER."""
    for _ in range(retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('127.0.0.1', port))
            s.sendall(b"LAUNCHER\n")
            s.setblocking(False)
            return s
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    L("ERROR", f"cannot connect to W{wid} port {port}")
    return None

def main():
    global logf
    ap = argparse.ArgumentParser(description="HW#3 Chaos Launcher")
    ap.add_argument('--coord-ip', required=True)
    ap.add_argument('--coord-port', type=int, required=True)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--step-delay', type=int, default=100)
    ap.add_argument('--log-dir', default='logs')
    ap.add_argument('--peer-ip', default='127.0.0.1')
    args = ap.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)
    logf = open(os.path.join(args.log_dir, 'launcher.log'), 'w', encoding='utf-8')
    rng = random.Random(args.seed)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    worker_py = os.path.join(script_dir, 'worker.py')

    L("INIT", f"seed={args.seed} coord={args.coord_ip}:{args.coord_port}")

    # --- Start 10 worker processes ---
    for i in range(1, N_W + 1):
        cmd = [sys.executable, worker_py,
               '--id', str(i),
               '--coord-ip', args.coord_ip,
               '--coord-port', str(args.coord_port),
               '--peer-ip', args.peer_ip,
               '--step-delay', str(args.step_delay),
               '--log-dir', args.log_dir]
        procs[i] = subprocess.Popen(cmd)
        L("START_W", f"W{i} pid={procs[i].pid}")

    time.sleep(1)  # let workers open peer ports

    # --- Connect control sockets ---
    for i in range(1, N_W + 1):
        s = connect_ctrl(i, 7000 + i)
        if s is None:
            L("ERROR", f"W{i} control connect failed")
            return
        ctrl_socks[i] = s
        ctrl_bufs[i] = bytearray()
        sock2wid[s] = i
        status[i] = 'RUNNING'
    L("CTRL", "all 10 control connections established")

    # --- Wait for 10 READY ---
    ready_count = 0
    while ready_count < N_W:
        socks = [s for s in ctrl_socks.values()]
        if not socks:
            break
        try:
            rd, _, _ = select.select(socks, [], [], 1.0)
        except Exception:
            continue
        for s in rd:
            wid = sock2wid.get(s)
            if wid is None:
                continue
            lines = recv_lines_nb(s, ctrl_bufs[wid])
            if lines is None:
                status[wid] = 'EXITED'
                continue
            for line in lines:
                parts = line.split()
                if parts[0] == 'READY':
                    ready_count += 1
                    L("READY", f"W{int(parts[1])} [{ready_count}/{N_W}]")

    G.t0 = time.time()
    L("T0", f"all {N_W} workers READY - t0 set")

    # --- Schedule crashes ---
    next_crash = G.t0 + rng.uniform(10, 20)
    blocked = False
    crash_no = 0

    while True:
        now = time.time()

        # Check all exited
        if all(s == 'EXITED' for s in status.values()):
            break

        # Read messages (REJOINED, EOF)
        active = [ctrl_socks[i] for i in ctrl_socks if status[i] != 'EXITED']
        if active:
            try:
                rd, _, _ = select.select(active, [], [], 0.2)
            except Exception:
                rd = []
            for s in rd:
                wid = sock2wid.get(s)
                if wid is None:
                    continue
                lines = recv_lines_nb(s, ctrl_bufs[wid])
                if lines is None:
                    # EOF
                    status[wid] = 'EXITED'
                    restart_at.pop(wid, None)
                    L("EXITED", f"W{wid}")
                    continue
                for line in lines:
                    parts = line.split()
                    if parts[0] == 'REJOINED':
                        rwid = int(parts[1])
                        status[rwid] = 'RUNNING'
                        L("REJOINED", f"W{rwid}")
                        # update event record
                        for ev in reversed(events):
                            if ev['victim'] == rwid and ev.get('rejoined_t') is None:
                                ev['rejoined_t'] = time.time()
                                ev['down'] = ev['rejoined_t'] - ev['crash_t']
                                break
                        if blocked:
                            blocked = False
                            next_crash = time.time() + rng.uniform(5, 15)
                            if next_crash - G.t0 >= 80:
                                next_crash = None

        now = time.time()

        # Process restart timers
        for wid in list(restart_at):
            if status[wid] == 'EXITED':
                restart_at.pop(wid, None)
                continue
            if now >= restart_at[wid]:
                try:
                    ctrl_socks[wid].sendall(b"RESTART\n")
                except OSError:
                    pass
                status[wid] = 'REJOINING'
                del restart_at[wid]
                L("RESTART", f"W{wid}")

        # Schedule crash
        if next_crash is not None and now >= next_crash and (now - G.t0) < 80:
            cr_rj = sum(1 for s in status.values() if s in ('CRASHED', 'REJOINING'))
            if cr_rj >= 2:
                blocked = True
                next_crash = None
            else:
                running_w = [i for i, s in status.items() if s == 'RUNNING']
                if running_w:
                    crash_no += 1
                    victim = rng.choice(running_w)
                    try:
                        ctrl_socks[victim].sendall(b"CRASH\n")
                    except OSError:
                        pass
                    status[victim] = 'CRASHED'
                    D = rng.uniform(10, 15)
                    restart_at[victim] = now + D
                    events.append({
                        'no': crash_no, 'victim': victim,
                        'crash_t': now, 'D': D,
                        'restart_t': now + D,
                        'rejoined_t': None, 'down': None,
                    })
                    L("CRASH", f"#{crash_no} W{victim} D={D:.1f}s (t={now-G.t0:.1f}s)")
                    next_crash = now + rng.uniform(5, 15)
                    if next_crash - G.t0 >= 80:
                        next_crash = None

        # Check no pending restarts and no active crashes → done with chaos
        if next_crash is None and not blocked and not restart_at:
            # No more crashes to schedule, no pending restarts
            # Just wait for all workers to exit
            pass

    # --- Wait for all processes ---
    L("WAIT", "waiting for worker processes")
    for i in range(1, N_W + 1):
        if procs[i].poll() is None:
            procs[i].wait(timeout=60)

    # --- Write event log ---
    L("EVENTS", f"total crashes: {len(events)}")
    for ev in events:
        rj = f"{ev['rejoined_t']-G.t0:.1f}s" if ev['rejoined_t'] else "N/A"
        dn = f"{ev['down']:.1f}s" if ev['down'] else "N/A"
        L("EVENT", f"#{ev['no']} W{ev['victim']} crash={ev['crash_t']-G.t0:.1f}s "
          f"D={ev['D']:.1f}s restart={ev['restart_t']-G.t0:.1f}s rejoined={rj} down={dn}")

    L("TERMINATE", "Launcher done")
    if logf:
        logf.close()

if __name__ == '__main__':
    main()
