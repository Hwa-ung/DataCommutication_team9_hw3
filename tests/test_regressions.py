import importlib
import socket
import threading
import unittest
from unittest.mock import Mock, patch

import coordinator
import launcher
import worker


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.c = importlib.reload(coordinator)
        self.log = patch.object(self.c, 'L').start()
        self.send = patch.object(self.c, '_send').start()
        self.addCleanup(patch.stopall)
        self.c.t0 = 1.0
        for wid in range(1, self.c.N_W + 1):
            entry = self.c.WEntry()
            entry.sock = object()
            entry.lastHB = 10.0
            self.c.wk[wid] = entry
            self.c.s2w[entry.sock] = wid
        self.sock = self.c.wk[1].sock

    def assigned(self, k=0, owner=1):
        task = self.c.tasks[k]
        task.state, task.owner, task.epoch = self.c.ASSIGNED, owner, 1
        return task

    def test_request_cannot_impersonate_another_worker(self):
        self.c._on_req(self.sock, ['REQ', '2'], 1)
        self.assertEqual(self.c.tasks[0].state, self.c.PENDING)
        self.send.assert_not_called()

    def test_negative_task_id_cannot_commit_last_task(self):
        task = self.assigned(self.c.N_T - 1)
        self.c._on_result(self.sock, ['RESULT', '-1', '1', '123'], 1)
        self.assertEqual(task.state, self.c.ASSIGNED)
        self.assertEqual(self.c.done_n, 0)

    def test_duplicate_and_old_epoch_results_commit_once(self):
        task = self.assigned()
        self.c._on_result(self.sock, ['RESULT', '0', '0', '99'], 1)
        self.c._on_result(self.sock, ['RESULT', '0', '1', '9592'], 1)
        self.c._on_result(self.sock, ['RESULT', '0', '1', '9592'], 2)
        self.assertEqual((task.cc, self.c.done_n, self.c.done_sum), (1, 1, 9592))

    def test_dead_sender_cannot_commit(self):
        task = self.assigned()
        self.c.wk[1].state = self.c.DEAD
        self.c._on_result(self.sock, ['RESULT', '0', '1', '9592'], 1)
        self.assertEqual(task.state, self.c.ASSIGNED)
        self.send.assert_called_once_with(1, 'REJECT_STALE 0 DEAD')

    def test_ring_ok_cannot_impersonate_another_worker(self):
        self.c._on_ring_ok(self.sock, ['RING_OK', '2'], 1)
        self.assertFalse(self.c.wk[2].rok)
        self.assertEqual(self.c.n_rok, 0)

    def test_failed_send_invalidates_partial_frame_connection(self):
        # Use the real send function; other tests mock transport deliberately.
        patch.stopall()
        conn = Mock()
        conn.sendall.side_effect = socket.timeout
        self.c.wk[1].sock = conn
        with patch.object(self.c, 'L'):
            self.c._send(1, 'ASSIGN 0 1')
        conn.shutdown.assert_called_once_with(socket.SHUT_RDWR)

    def test_rejoin_recovers_tasks_when_no_takeover_target_existed(self):
        task = self.assigned()
        for entry in self.c.wk.values():
            entry.state = self.c.DEAD
        self.c._on_rejoin(self.sock, ['REJOIN', '1'], 1)
        self.assertEqual((task.owner, task.epoch), (1, 2))
        self.assertEqual(self.send.call_args_list[0].args, (1, 'WELCOME'))
        self.send.assert_any_call(1, 'TAKEOVER 0 2 1')

    def test_rejoin_cannot_impersonate_another_worker(self):
        self.c.wk[2].state = self.c.DEAD
        self.c._on_rejoin(self.sock, ['REJOIN', '2'], 1)
        self.assertEqual(self.c.wk[2].state, self.c.DEAD)

    def test_orphaned_task_is_retried_after_another_worker_recovers(self):
        task = self.assigned()
        for entry in self.c.wk.values():
            entry.state = self.c.DEAD
        self.c.check_failures(20.0)
        self.assertEqual(task.owner, 1)
        self.c.wk[2].state = self.c.ALIVE
        self.c.wk[2].lastHB = 20.0
        self.c.check_failures(20.0)
        self.assertEqual((task.owner, task.epoch), (2, 2))
        self.send.assert_any_call(2, 'TAKEOVER 0 2 1')

    def test_result_keeps_worker_lock_until_commit_decision(self):
        task = self.assigned()
        entered = threading.Event()
        proceed = threading.Event()

        class GateLock:
            def __enter__(self):
                entered.set()
                if not proceed.wait(2):
                    raise AssertionError('test gate timed out')

            def __exit__(self, *args):
                pass

        task.lock = GateLock()
        thread = threading.Thread(target=self.c._on_result,
                                  args=(self.sock, ['RESULT', '0', '1', '9592'], 1))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            acquired = self.c.wl.acquire(blocking=False)
            if acquired:
                self.c.wl.release()
            self.assertFalse(acquired, 'failure detector can mark sender DEAD during commit')
        finally:
            proceed.set()
            thread.join(2)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.w = importlib.reload(worker)
        patch.object(self.w, 'L').start()
        self.addCleanup(patch.stopall)

    def test_assignment_during_crash_is_discarded(self):
        self.w.G.state = 'CRASHED'
        self.w.on_assign(4, 1)
        self.w.on_takeover(5, 2, 2)
        self.assertEqual(list(self.w.local_q), [])

    def test_unsolicited_welcome_does_not_resurrect_crashed_worker(self):
        self.w.G.state = 'CRASHED'
        self.w.on_welcome()
        self.assertEqual(self.w.G.state, 'CRASHED')

    def test_duplicate_restart_does_not_pause_running_worker(self):
        self.w.G.state = 'RUNNING'
        self.w.on_restart()
        self.assertEqual(self.w.G.state, 'RUNNING')

    def test_coordinator_disconnect_stops_all_runtime_loops(self):
        self.w.G.state = 'RUNNING'
        with patch.object(self.w, 'recv_line', return_value=None):
            self.w.control_recv_loop(bytearray())
        self.assertTrue(self.w.G.stopping)

    def test_no_messages_are_sent_after_shutdown(self):
        self.w.G.state = 'RUNNING'
        self.w.G.stopping = True
        with patch.object(self.w, 'send_coord') as send:
            self.assertFalse(self.w.send_if_alive(0, 'coord', 'REQ 1'))
        send.assert_not_called()

    def test_crash_is_serialized_with_inflight_send(self):
        self.w.G.state = 'RUNNING'
        entered = threading.Event()
        release = threading.Event()
        crashed = threading.Event()
        sent_states = []

        def sendall(data):
            entered.set()
            if not release.wait(2):
                raise AssertionError('send gate timed out')
            sent_states.append(self.w.G.state)

        self.w.G.coord_sock = Mock()
        self.w.G.coord_sock.sendall.side_effect = sendall
        sender = threading.Thread(target=self.w.send_if_alive,
                                  args=(0, 'coord', 'HB 1'))

        def crash():
            self.w.on_crash()
            crashed.set()

        crasher = threading.Thread(target=crash)
        sender.start()
        try:
            self.assertTrue(entered.wait(2))
            crasher.start()
            self.assertFalse(crashed.wait(0.05))
        finally:
            release.set()
            sender.join(2)
            if crasher.ident is not None:
                crasher.join(2)
        self.assertFalse(sender.is_alive())
        self.assertFalse(crasher.is_alive())
        self.assertEqual(sent_states, ['RUNNING'])
        self.assertEqual(self.w.G.state, 'CRASHED')
        self.assertFalse(self.w.send_if_alive(0, 'coord', 'HB 1'))
        self.w.G.coord_sock.sendall.assert_called_once()

    def test_takeover_preserves_no_task_flag(self):
        self.w.G.state = 'RUNNING'
        self.w.on_no_task()
        self.w.on_takeover(0, 2, 10)
        self.assertTrue(self.w.G.no_task)
        self.assertEqual(list(self.w.local_q), [(0, 2, 1, 0)])

    def test_blank_and_bad_checkpoint_lines_do_not_kill_receiver(self):
        self.w.G.state = 'RUNNING'
        conn = Mock()
        lines = ['', 'CKPT invalid 1 2 3', 'CKPT 0 1 2 2262', None]
        with patch.object(self.w, 'recv_line', side_effect=lines):
            self.w._ckpt_handler(conn, bytearray(), 2)
        self.assertEqual(self.w.store[0], (1, 2, 2262))
        conn.close.assert_called_once()

    def test_future_epoch_checkpoint_is_not_used_for_takeover(self):
        self.w.G.state = 'RUNNING'
        self.w.store[0] = (9, 5, 100)
        self.w.on_takeover(0, 2, 2)
        self.assertEqual(self.w.local_q.pop(), (0, 2, 1, 0))

    def test_checkpoint_resume_matches_full_task_result(self):
        self.w.G.state = 'RUNNING'
        partial = sum(self.w.count_primes(0, s) for s in range(1, 6))
        self.w.store[0] = (1, 5, partial)
        self.w.on_takeover(0, 2, 2)
        k, epoch, start, saved = self.w.local_q.pop()
        self.assertEqual((k, epoch, start), (0, 2, 6))
        self.assertEqual(saved + sum(self.w.count_primes(k, s) for s in range(start, 11)), 9592)

    def test_crash_clears_queue_and_checkpoints(self):
        self.w.G.state = 'RUNNING'
        self.w.on_assign(0, 1)
        self.w.store[0] = (1, 3, 3245)
        self.w.G.requested = True
        self.w.on_crash()
        self.assertEqual(self.w.G.crash_gen, 1)
        self.assertFalse(self.w.local_q)
        self.assertFalse(self.w.store)
        self.assertFalse(self.w.G.requested)

    def test_malformed_control_message_does_not_prevent_bye(self):
        self.w.G.state = 'RUNNING'
        with patch.object(self.w, 'recv_line', side_effect=['ASSIGN bad', 'BYE']):
            self.w.control_recv_loop(bytearray())
        self.assertTrue(self.w.G.stopping)
        self.assertFalse(self.w.G.failed)

    def test_invalid_assignment_does_not_enter_compute_queue(self):
        self.w.G.state = 'RUNNING'
        self.w.on_assign(-1, 1)
        self.w.on_takeover(1000, 2, 1)
        self.assertFalse(self.w.local_q)

    def test_prime_count_known_boundaries(self):
        self.assertEqual(self.w.count_primes(0, 1), 1229)
        self.assertEqual(sum(self.w.count_primes(0, s) for s in range(1, 11)), 9592)


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.l = importlib.reload(launcher)
        patch.object(self.l, 'L').start()
        self.addCleanup(patch.stopall)

    def test_failed_control_connect_closes_socket(self):
        conn = Mock()
        conn.connect.side_effect = ConnectionRefusedError
        with patch.object(self.l.socket, 'socket', return_value=conn), \
                patch.object(self.l.time, 'sleep'):
            self.assertIsNone(self.l.connect_ctrl(1, 7001, retries=1))
        conn.close.assert_called_once()

    def test_cleanup_reaps_children_after_startup_failure(self):
        proc = Mock()
        proc.poll.return_value = None
        proc.wait.side_effect = [launcher.subprocess.TimeoutExpired('worker', 3), 1]
        self.l.procs[1] = proc
        conn = Mock()
        self.l.ctrl_socks[1] = conn
        self.l.cleanup()
        conn.close.assert_called_once()
        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        self.assertEqual(proc.wait.call_count, 2)

    def test_ready_eof_fails_promptly(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        left.setblocking(False)
        self.l.ctrl_socks[1] = left
        self.l.ctrl_bufs[1] = bytearray()
        self.l.sock2wid[left] = 1
        self.l.status[1] = 'RUNNING'
        right.close()
        with self.assertRaisesRegex(RuntimeError, 'READY'):
            self.l.wait_for_ready(timeout=0.2)

    def test_duplicate_ready_does_not_complete_startup(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.setblocking(False)
        self.l.ctrl_socks[1] = left
        self.l.ctrl_bufs[1] = bytearray()
        self.l.sock2wid[left] = 1
        self.l.status[1] = 'RUNNING'
        right.sendall(b'READY 1\n' * self.l.N_W)
        with self.assertRaises(TimeoutError):
            self.l.wait_for_ready(timeout=0.1)


if __name__ == '__main__':
    unittest.main()
