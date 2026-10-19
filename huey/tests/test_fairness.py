import glob
import os
import shutil
import tempfile
import threading
import time
import unittest
from queue import Queue

from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.exceptions import ConfigurationError
from huey.storage import FileStorage
from huey.tests.base import BaseTestCase

try:
    from redis import Redis
except ImportError:
    Redis = None

try:
    from huey.api import PriorityRedisHuey
except ImportError:
    PriorityRedisHuey = None


SQLITE_FILENAME = 'huey_fairness.db'


def build_tasks(huey):
    # Returns (high p=2, medium p=1, low p=0, lowneg p=-1) task wrappers.
    def record(n):
        return n
    high = huey.task(priority=2, name='fair_high')(record)
    medium = huey.task(priority=1, name='fair_medium')(record)
    low = huey.task(name='fair_low')(record)
    lowneg = huey.task(priority=-1, name='fair_lowneg')(record)
    return high, medium, low, lowneg


def dequeue_names(huey, n):
    return [huey.dequeue().name for _ in range(n)]


class TestFairMode(BaseTestCase):
    # In-memory backend, fair mode enabled with the default quota (3).
    def get_huey(self):
        return MemoryHuey(utc=False, fair=True)

    def setUp(self):
        super(TestFairMode, self).setUp()
        self.high, self.medium, self.low, self.lowneg = build_tasks(self.huey)

    def test_strict_order_when_disabled(self):
        # An explicit guard: without fair mode the original strict priority
        # ordering is completely unchanged.
        huey = MemoryHuey(utc=False)
        high, medium, low, _ = build_tasks(huey)
        for _ in range(3):
            low()
            medium()
            high()
        order = dequeue_names(huey, 9)
        self.assertEqual(order, ['fair_high'] * 3 + ['fair_medium'] * 3 +
                         ['fair_low'] * 3)

    def test_basic_rotation(self):
        # Six high-priority and three low-priority tasks waiting up-front:
        # every fourth dequeue must yield a low-priority task.
        for _ in range(6):
            self.high()
        for _ in range(3):
            self.low()
        order = dequeue_names(self.huey, 9)
        self.assertEqual(order,
                         ['fair_high'] * 3 + ['fair_low'] +
                         ['fair_high'] * 3 + ['fair_low'] * 2)

    def test_no_low_waiting(self):
        # With no low-priority tasks waiting, highs keep flowing.
        for _ in range(5):
            self.high()
        self.assertEqual(dequeue_names(self.huey, 5), ['fair_high'] * 5)

        # Once a low-priority task arrives, the over-due rotation forces it
        # to be dequeued before the next high.
        self.low()
        self.high()
        self.assertEqual(dequeue_names(self.huey, 2),
                         ['fair_low', 'fair_high'])

    def test_natural_low_resets_streak(self):
        # When no high is waiting, a low reached via strict order resets the
        # streak: the next batch is allowed a full quota of three highs.
        self.high(), self.high(), self.low()
        self.assertEqual(dequeue_names(self.huey, 3),
                         ['fair_high', 'fair_high', 'fair_low'])
        self.high(), self.high(), self.high(), self.low()
        self.assertEqual(dequeue_names(self.huey, 4),
                         ['fair_high'] * 3 + ['fair_low'])

    def test_low_priority_ordering_within_class(self):
        # When a low is forced, strict order is still respected *within* the
        # low-priority class: p=0 before p=-1, FIFO within equal priority.
        for _ in range(3):
            self.high()
        for _ in range(2):
            self.low()
        for _ in range(2):
            self.lowneg()
        order = dequeue_names(self.huey, 7)
        self.assertEqual(order,
                         ['fair_high'] * 3 + ['fair_low'] * 2 +
                         ['fair_lowneg'] * 2)

    def test_custom_quota(self):
        huey = MemoryHuey(utc=False, fair=True, fair_quota=1)
        high, medium, low, _ = build_tasks(huey)
        for _ in range(3):
            high()
            low()
        self.assertEqual(dequeue_names(huey, 6),
                         ['fair_high', 'fair_low'] * 3)

    def test_custom_threshold(self):
        # With threshold=1, priority 1 counts as "low" and priority 2 high.
        huey = MemoryHuey(utc=False, fair=True, fair_threshold=1)
        high, medium, low, _ = build_tasks(huey)
        for _ in range(3):
            high()
        for _ in range(2):
            medium()
        self.assertEqual(dequeue_names(huey, 5),
                         ['fair_high'] * 3 + ['fair_medium'] * 2)

    def test_invalid_quota_rejected(self):
        self.assertRaises(ConfigurationError, MemoryHuey, utc=False,
                          fair=True, fair_quota=0)

    def test_non_priority_storage_rejected(self):
        from huey.api import RedisHuey
        # Raises either the missing-redis-module error or the fair-mode error,
        # but never silently accepts fair mode on a FIFO backend.
        self.assertRaises(ConfigurationError, RedisHuey, utc=False, fair=True)

    def test_concurrent_workers_invariant(self):
        # Static workload: 40 high and 20 low tasks all enqueued before any
        # dequeue. With 8 concurrent workers, a low-priority task must still
        # be dequeued at every 4th position while both classes have items.
        for _ in range(40):
            self.high()
        for _ in range(20):
            self.low()

        results = Queue()

        def worker():
            while True:
                task = self.huey.dequeue()
                if task is None:
                    return
                results.put(task.name)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        order = [results.get_nowait() for _ in range(results.qsize())]
        self.assertEqual(len(order), 60)
        for position in range(4, 41, 4):
            self.assertEqual(order[position - 1], 'fair_low',
                             'position %s should be a low task' % position)
        self.assertEqual(order.count('fair_low'), 20)
        self.assertEqual(order.count('fair_high'), 40)


class TestFairModeSqlite(BaseTestCase):
    # Verifies durable rotation state using an on-disk SQLite database with
    # fresh storage instances standing in for consumer restarts.
    def get_huey(self):
        return SqliteHuey(utc=False, filename=SQLITE_FILENAME, fair=True)

    def tearDown(self):
        super(TestFairModeSqlite, self).tearDown()
        for filename in glob.glob(SQLITE_FILENAME + '*'):
            if os.path.exists(filename):
                os.unlink(filename)

    def restart(self):
        # Close the existing storage (if any) and reconnect, as a restarted
        # consumer process would.
        self.huey.storage.close()
        huey = SqliteHuey(utc=False, filename=SQLITE_FILENAME, fair=True)
        self.huey = huey
        return build_tasks(huey)

    def test_streak_survives_restart(self):
        high, medium, low, _ = build_tasks(self.huey)
        for _ in range(4):
            high()
        low()

        # Three highs processed, streak is 3 when the consumer restarts.
        self.assertEqual(dequeue_names(self.huey, 3), ['fair_high'] * 3)

        high2, _, low2, _ = self.restart()
        # The first dequeue after restart must honor the persisted streak and
        # yield the waiting low-priority task instead of the fourth high.
        task = self.huey.dequeue()
        self.assertEqual(task.name, 'fair_low')
        self.assertEqual(self.huey.dequeue().name, 'fair_high')
        self.assertTrue(self.huey.dequeue() is None)

    def test_repeated_restart_neither_over_nor_under_counts(self):
        high, medium, low, _ = build_tasks(self.huey)
        for _ in range(5):
            high()
        for _ in range(2):
            low()

        # Two high dequeues -> streak 2; restarting must not re-count them.
        self.assertEqual(dequeue_names(self.huey, 2), ['fair_high'] * 2)
        high2, _, low2, _ = self.restart()
        # One more high brings the streak to exactly 3, then the low is due.
        self.assertEqual(dequeue_names(self.huey, 2),
                         ['fair_high', 'fair_low'])

        # Restart yet again: remaining sequence is strict with a reset streak
        # (two highs, then the natural low), unaffected by reconnecting.
        high3, _, low3, _ = self.restart()
        self.assertEqual(dequeue_names(self.huey, 3),
                         ['fair_high', 'fair_high', 'fair_low'])
        self.assertTrue(self.huey.dequeue() is None)

    def test_flush_queue_resets_streak(self):
        high, medium, low, _ = build_tasks(self.huey)
        for _ in range(3):
            high()
        dequeue_names(self.huey, 3)

        self.huey.storage.flush_queue()
        high(), low()
        # Streak was reset, so strict order applies again: high first.
        self.assertEqual(dequeue_names(self.huey, 2),
                         ['fair_high', 'fair_low'])

    def test_concurrent_workers_invariant(self):
        high, medium, low, _ = build_tasks(self.huey)
        for _ in range(40):
            high()
        for _ in range(20):
            low()

        results = Queue()

        def worker():
            while True:
                task = self.huey.dequeue()
                if task is None:
                    return
                results.put(task.name)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        order = [results.get_nowait() for _ in range(results.qsize())]
        self.assertEqual(len(order), 60)
        for position in range(4, 41, 4):
            self.assertEqual(order[position - 1], 'fair_low')
        self.assertEqual(order.count('fair_low'), 20)
        self.assertEqual(order.count('fair_high'), 40)


def redis_available():
    if Redis is None or PriorityRedisHuey is None:
        return False
    try:
        info = Redis().info()
    except Exception:
        return False
    try:
        return int(info['redis_version'].split('.', 1)[0]) >= 5
    except (KeyError, ValueError):
        return False


@unittest.skipUnless(redis_available(), 'requires redis >= 5.0')
class TestFairModePriorityRedis(BaseTestCase):
    def get_huey(self):
        return PriorityRedisHuey('test_fairness', utc=False, fair=True,
                                 blocking=False)

    def setUp(self):
        super(TestFairModePriorityRedis, self).setUp()
        self.huey.storage.flush_all()

    def tearDown(self):
        self.huey.storage.flush_all()
        super(TestFairModePriorityRedis, self).tearDown()

    def test_rotation(self):
        s = self.huey.storage
        for i in range(6):
            s.enqueue(b'h%d' % i, priority=2)
        for i in range(3):
            s.enqueue(b'l%d' % i, priority=0)
        order = [s.dequeue() for _ in range(9)]
        self.assertEqual(order,
                         [b'h0', b'h1', b'h2', b'l0',
                          b'h3', b'h4', b'h5', b'l1', b'l2'])
        self.assertTrue(s.dequeue() is None)

    def test_streak_persists_across_connections(self):
        # A new storage instance (as used by a restarted consumer) reads the
        # same server-side streak state.
        s = self.huey.storage
        for i in range(4):
            s.enqueue(b'h%d' % i, priority=2)
        s.enqueue(b'l0', priority=0)
        self.assertEqual([s.dequeue() for _ in range(3)],
                         [b'h0', b'h1', b'h2'])

        s2 = PriorityRedisHuey('test_fairness', utc=False, fair=True,
                               blocking=False).storage
        try:
            self.assertEqual(s2.dequeue(), b'l0')
            self.assertEqual(s2.dequeue(), b'h3')
        finally:
            s2.close()

    def test_blocking_wakeup(self):
        # The blocking fair dequeue waits on the wake-up signal and returns
        # promptly when an item is enqueued from another thread.
        s = PriorityRedisHuey('test_fairness_block', utc=False, fair=True,
                              blocking=True, read_timeout=2).storage
        s.flush_all()

        def enqueue_later():
            time.sleep(0.2)
            s.enqueue(b'wake', priority=1)

        thread = threading.Thread(target=enqueue_later)
        start = time.monotonic()
        thread.start()
        try:
            self.assertEqual(s.dequeue(), b'wake')
            self.assertLess(time.monotonic() - start, 1.5)
        finally:
            thread.join()
            s.flush_all()
            s.close()

    def test_flush_resets_streak(self):
        s = self.huey.storage
        for i in range(3):
            s.enqueue(b'h%d' % i, priority=2)
        [s.dequeue() for _ in range(3)]
        s.flush_queue()
        s.enqueue(b'hx', priority=2)
        s.enqueue(b'lx', priority=0)
        self.assertEqual(s.dequeue(), b'hx')
        self.assertEqual(s.dequeue(), b'lx')


class TestFairModeFileStorage(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='huey-fair-')

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def storage(self):
        return FileStorage('huey', self.path, fair=True)

    def test_rotation_and_restart(self):
        s = self.storage()
        for i in range(4):
            s.enqueue(b'h%d' % i, priority=2)
        s.enqueue(b'l0', priority=0)
        self.assertEqual([s.dequeue() for _ in range(3)],
                         [b'h0', b'h1', b'h2'])

        # Reconnect ("restart") against the same on-disk state.
        s2 = self.storage()
        self.assertEqual(s2.dequeue(), b'l0')
        self.assertEqual(s2.dequeue(), b'h3')
        self.assertTrue(s2.dequeue() is None)

    def test_flush_resets_streak(self):
        s = self.storage()
        for i in range(3):
            s.enqueue(b'h%d' % i, priority=2)
        [s.dequeue() for _ in range(3)]
        s.flush_queue()
        s.enqueue(b'hx', priority=2)
        s.enqueue(b'lx', priority=0)
        self.assertEqual(s.dequeue(), b'hx')
        self.assertEqual(s.dequeue(), b'lx')
