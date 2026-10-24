"""Run the real 10-worker/1000-task program; no third-party dependencies."""
import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]


def stop_process(proc, tree=False):
    if proc is None or proc.poll() is not None:
        return
    if tree and os.name == 'nt':
        subprocess.run(['taskkill', '/PID', str(proc.pid), '/T', '/F'],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    elif tree:
        os.killpg(proc.pid, signal.SIGTERM)
    else:
        proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if tree and os.name != 'nt':
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
        proc.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step-delay', type=int, default=0)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--timeout', type=float, default=240)
    parser.add_argument('--disconnect-coordinator', action='store_true')
    args = parser.parse_args()
    # Fail before launching anything if the fixed worker ports are in use.
    for port in range(7001, 7011):
        with socket.socket() as probe:
            probe.bind(('0.0.0.0', port))
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    logs = ROOT / '.test-runs' / str(time.time_ns())
    logs.mkdir(parents=True)
    print(f'Logs: {logs}', flush=True)
    options = {'cwd': ROOT}
    if os.name == 'nt':
        options['creationflags'] = subprocess.CREATE_NO_WINDOW
    coord = launch = None
    with (logs / 'coordinator-console.log').open('w', encoding='utf-8') as cout, \
            (logs / 'launcher-console.log').open('w', encoding='utf-8') as lout:
        try:
            coord = subprocess.Popen(
                [sys.executable, 'coordinator.py', '--host', '127.0.0.1',
                 '--port', str(port), '--log-dir', str(logs)],
                stdout=cout, stderr=subprocess.STDOUT, **options)
            deadline = time.monotonic() + args.timeout
            while True:
                if coord.poll() is not None:
                    raise RuntimeError('coordinator failed to start')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('coordinator startup timed out')
                    time.sleep(0.1)
            if os.name != 'nt':
                options['start_new_session'] = True
            launch = subprocess.Popen(
                [sys.executable, 'launcher.py', '--coord-ip', '127.0.0.1',
                 '--coord-port', str(port), '--step-delay', str(args.step_delay),
                 '--seed', str(args.seed), '--log-dir', str(logs)],
                stdout=lout, stderr=subprocess.STDOUT, **options)
            if args.disconnect_coordinator:
                while True:
                    path = logs / 'launcher.log'
                    if path.exists() and '[T0]' in path.read_text(encoding='utf-8'):
                        break
                    if launch.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError('workers did not become READY')
                    time.sleep(0.1)
                stop_process(coord)
                assert launch.wait(timeout=15) == 1, 'launcher must report worker failure'
                print('PASS: coordinator disconnect stopped workers and launcher', flush=True)
                return
            while coord.poll() is None or launch.poll() is None:
                if coord.poll() not in (None, 0) or launch.poll() not in (None, 0):
                    raise RuntimeError('child process failed; inspect console logs')
                if time.monotonic() >= deadline:
                    raise TimeoutError('integration run timed out')
                time.sleep(0.2)
            assert coord.returncode == launch.returncode == 0
            output = (logs / 'coordinator.log').read_text(encoding='utf-8')
            assert '[VERIFY] PASS' in output
            assert 'total=5761455 all_done=True all_commit_once=True double=0' in output
            assert output.count('[COMMIT]') == 1000
            for wid in range(1, 11):
                worker_log = (logs / f'worker_{wid}.log').read_text(encoding='utf-8')
                assert '[TERMINATE]' in worker_log
            if args.step_delay >= 100:
                assert '[REJOIN]' in output and '[TAKEOVER]' in output
            for path in (logs / 'coordinator-console.log', logs / 'launcher-console.log'):
                assert 'Traceback' not in path.read_text(encoding='utf-8')
            for line in output.splitlines():
                if '[VERIFY]' in line or '[TERMINATE]' in line:
                    print(line, flush=True)
        finally:
            stop_process(launch, tree=True)
            stop_process(coord)


if __name__ == '__main__':
    main()
