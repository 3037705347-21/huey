import datetime
import importlib.util
import time
import unittest


django_tasks_available = importlib.util.find_spec('django') is not None and (
    importlib.util.find_spec('django.tasks') is not None or
    importlib.util.find_spec('django_tasks') is not None)

if django_tasks_available:
    import django
    from django.conf import settings

    from huey import MemoryHuey

    settings.configure(
        USE_TZ=True,
        DATABASES={'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:'}},
        TASKS={'default': {
            'BACKEND': 'huey.contrib.djhuey.tasks_backend.HueyBackend'}},
        HUEY=MemoryHuey('djtasks-test'))
    django.setup()

    from django.db import transaction
    from django.test import override_settings
    from django.utils import timezone

    try:
        from django.tasks import TaskResultStatus, default_task_backend, task
        from django.tasks.exceptions import InvalidTask
        from django.tasks.exceptions import TaskResultDoesNotExist
        from django.tasks.exceptions import TaskResultMismatch
    except ImportError:
        # Django < 6.0: the django-tasks backport provides the same API.
        from django_tasks import TaskResultStatus, default_task_backend, task
        from django_tasks.exceptions import TaskResultDoesNotExist
        from django_tasks.exceptions import TaskResultMismatch
        try:
            from django_tasks.exceptions import InvalidTask
        except ImportError:
            from django_tasks.exceptions import InvalidTaskError as InvalidTask

    from huey.contrib.djhuey import HUEY
    from huey.contrib.djhuey.reconcile import ReconciliationError
    from huey.contrib.djhuey.reconcile import reconcile_running_tasks
    from huey.contrib.djhuey.tasks_backend import KEY
    from huey.tests.base import BaseTestCase
    from huey.utils import Error

    @task
    def add(a, b):
        return a + b

    @task
    def explode():
        raise ValueError('kapow')

    @task(takes_context=True)
    def with_context(context, x):
        return [context.attempt, x]

    @task
    def bad_return():
        return object()

    def plain():
        pass

    async def aplain():
        pass

    anon_task = task(lambda: 1)


@unittest.skipIf(not django_tasks_available,
                 'requires django.tasks or django-tasks backport')
class TestDjangoTasksBackend(unittest.TestCase):
    def setUp(self):
        HUEY.immediate = False
        HUEY.storage.flush_all()

    def run_next(self):
        huey_task = HUEY.dequeue()
        self.assertTrue(huey_task is not None)
        return HUEY.execute(huey_task)

    def test_enqueue_execute(self):
        result = add.enqueue(2, 3)
        self.assertEqual(result.status, TaskResultStatus.READY)
        self.assertTrue(result.enqueued_at is not None)
        self.assertTrue(result.started_at is None)
        self.assertEqual(HUEY.pending_count(), 1)

        self.run_next()

        result.refresh()
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(result.return_value, 5)
        self.assertTrue(result.started_at is not None)
        self.assertTrue(result.finished_at is not None)
        self.assertEqual(result.attempts, 1)

    def test_failure(self):
        result = explode.enqueue()
        self.run_next()

        result.refresh()
        self.assertEqual(result.status, TaskResultStatus.FAILED)
        self.assertEqual(len(result.errors), 1)
        self.assertTrue(result.errors[0].exception_class is ValueError)
        self.assertTrue('kapow' in result.errors[0].traceback)
        self.assertRaises(ValueError, lambda: result.return_value)

    def test_get_result(self):
        result = add.enqueue(1, 1)
        fetched = default_task_backend.get_result(result.id)
        self.assertEqual(fetched.id, result.id)
        self.assertEqual(fetched.args, [1, 1])
        self.assertEqual(fetched.status, TaskResultStatus.READY)

        # Fetching through the matching task works, a mismatched task fails.
        self.assertEqual(add.get_result(result.id).id, result.id)
        self.assertRaises(TaskResultMismatch, explode.get_result, result.id)
        self.assertRaises(TaskResultDoesNotExist,
                          default_task_backend.get_result, 'missing')

    def test_priority_order(self):
        add.enqueue(1, 1)
        add.using(priority=10).enqueue(2, 2)
        add.using(priority=-10).enqueue(3, 3)

        order = [HUEY.dequeue().args[1] for _ in range(3)]
        self.assertEqual(order, [[2, 2], [1, 1], [3, 3]])

    def test_run_after(self):
        run_after = timezone.now() + datetime.timedelta(minutes=5)
        result = add.using(run_after=run_after).enqueue(1, 2)
        self.assertEqual(result.status, TaskResultStatus.READY)

        huey_task = HUEY.dequeue()
        self.assertTrue(huey_task.eta is not None)

        # Naive datetimes are rejected when USE_TZ is enabled.
        naive = datetime.datetime.now() + datetime.timedelta(minutes=5)
        self.assertRaises(InvalidTask, add.using, run_after=naive)

    def test_takes_context(self):
        result = with_context.enqueue(7)
        self.run_next()
        result.refresh()
        self.assertEqual(result.return_value, [1, 7])

    def test_immediate_mode(self):
        HUEY.immediate = True
        result = add.enqueue(3, 4)
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(result.return_value, 7)

    def test_invalid_tasks(self):
        self.assertRaises(InvalidTask, task(queue_name='nope'), plain)
        self.assertRaises(InvalidTask, task, aplain)

        # Module paths that do not round-trip are rejected at enqueue,
        # before anything is stored or enqueued.
        self.assertRaises(InvalidTask, anon_task.enqueue)
        self.assertEqual(HUEY.pending_count(), 0)
        self.assertEqual(HUEY.result_count(), 0)

    def test_unserializable_return(self):
        result = bad_return.enqueue()
        self.run_next()
        result.refresh()
        self.assertEqual(result.status, TaskResultStatus.FAILED)
        self.assertEqual(len(result.errors), 1)
        self.assertRaises(ValueError, lambda: result.return_value)

    def test_unsupported_priority(self):
        HUEY.storage.priority = False
        try:
            self.assertRaises(InvalidTask, add.using, priority=5)
        finally:
            HUEY.storage.priority = True

    def test_moved_function_marks_failed(self):
        result = add.enqueue(1, 2)
        saved = globals().pop('add')
        try:
            self.run_next()
        finally:
            globals()['add'] = saved

        result.refresh()
        self.assertEqual(result.status, TaskResultStatus.FAILED)
        self.assertTrue(result.errors[0].exception_class is ImportError)

    def test_enqueue_on_commit(self):
        backend = {
            'BACKEND': 'huey.contrib.djhuey.tasks_backend.HueyBackend',
            'ENQUEUE_ON_COMMIT': True}
        with override_settings(TASKS={'default': backend}):
            with transaction.atomic():
                result = add.enqueue(5, 5)
                # Nothing is stored or enqueued until the commit fires.
                self.assertEqual(result.status, TaskResultStatus.READY)
                self.assertTrue(result.enqueued_at is None)
                self.assertEqual(HUEY.pending_count(), 0)
                self.assertRaises(TaskResultDoesNotExist, result.refresh)

            self.assertEqual(HUEY.pending_count(), 1)
            self.run_next()
            result.refresh()
            self.assertTrue(result.enqueued_at is not None)
            self.assertEqual(result.return_value, 10)


if django_tasks_available:
    class TestDjangoTasksConsumer(BaseTestCase):
        def get_huey(self):
            HUEY.immediate = False
            HUEY.storage.flush_all()
            return HUEY

        def test_consumer_integration(self):
            with self.consumer_context():
                result = add.enqueue(4, 5)
                for _ in range(100):
                    result.refresh()
                    if result.is_finished:
                        break
                    time.sleep(0.05)
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
            self.assertEqual(result.return_value, 9)


if django_tasks_available:
    class TestDjangoTasksReconciliation(unittest.TestCase):
        def setUp(self):
            HUEY.immediate = False
            HUEY.storage.flush_all()

        def run_next(self):
            huey_task = HUEY.dequeue()
            self.assertTrue(huey_task is not None)
            return HUEY.execute(huey_task)

        def mark_running(self, result_id, *, age_seconds=None,
                         enqueued_at=True):
            key = KEY % result_id
            record = HUEY.get(key, peek=True)
            record['status'] = TaskResultStatus.RUNNING.value
            started = timezone.now()
            if age_seconds is not None:
                started -= datetime.timedelta(seconds=age_seconds)
            record['started_at'] = record['last_attempted_at'] = \
                started.isoformat()
            record['worker_ids'].append('worker-host.123')
            if not enqueued_at:
                record['enqueued_at'] = None
            HUEY.put_result(key, record)

        def error_result(self, result_id, traceback='traceback-kapow'):
            HUEY.put_result(result_id, Error({
                'error': "ValueError('kapow')",
                'retries': 0,
                'traceback': traceback,
                'task_id': result_id}))

        def reconcile(self, **kwargs):
            kwargs.setdefault('stale_seconds', 0)
            return reconcile_running_tasks(HUEY, **kwargs)

        def test_orphaned_resets_to_ready_without_re_enqueueing(self):
            result = add.enqueue(1, 2)
            HUEY.dequeue()  # Consumed, but the worker died mid-execution.
            self.mark_running(result.id, age_seconds=3600)

            report = self.reconcile()

            self.assertEqual(report.scanned, 1)
            self.assertEqual(len(report.reset_ready), 1)
            self.assertEqual(report.reset_ready[0]['reason'], 'orphaned')

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.READY)
            self.assertTrue(result.started_at is None)
            self.assertTrue(result.finished_at is None)
            self.assertEqual(result.worker_ids, [])

            # The fix is status-only: no replacement message is enqueued.
            self.assertEqual(HUEY.pending_count(), 0)
            self.assertEqual(HUEY.scheduled_count(), 0)

            # Idempotent: a second run has nothing to do.
            report = self.reconcile()
            self.assertEqual(report.scanned, 0)
            self.assertEqual(report.actions, [])

        def test_queued_message_resets_to_ready_and_still_runs(self):
            result = add.enqueue(1, 2)
            self.mark_running(result.id, age_seconds=3600)
            self.assertEqual(HUEY.pending_count(), 1)

            report = self.reconcile()
            self.assertEqual(report.reset_ready[0]['reason'], 'queued')

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.READY)
            # The original message is untouched, not duplicated.
            self.assertEqual(HUEY.pending_count(), 1)

            self.run_next()
            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
            self.assertEqual(result.return_value, 3)

        def test_scheduled_message_resets_to_ready(self):
            result = add.enqueue(1, 2)
            huey_task = HUEY.dequeue()
            huey_task.eta = timezone.now() + datetime.timedelta(minutes=10)
            HUEY.add_schedule(huey_task)
            self.mark_running(result.id, age_seconds=3600)

            report = self.reconcile()
            self.assertEqual(report.reset_ready[0]['reason'], 'scheduled')

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.READY)
            self.assertEqual(HUEY.scheduled_count(), 1)

        def test_stored_error_syncs_failed(self):
            result = explode.enqueue()
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600)
            self.error_result(result.id)

            report = self.reconcile()
            self.assertEqual(len(report.synced_failed), 1)

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.FAILED)
            self.assertTrue(result.finished_at is not None)
            self.assertTrue(any('kapow' in e.traceback
                                for e in result.errors))

        def test_stored_value_syncs_successful(self):
            result = add.enqueue(2, 3)
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600)
            HUEY.put_result(result.id, 5)

            report = self.reconcile()
            self.assertEqual(len(report.synced_successful), 1)

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
            self.assertEqual(result.return_value, 5)
            self.assertTrue(result.finished_at is not None)

        def test_uncommitted_record_left_untouched(self):
            result = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600, enqueued_at=False)

            report = self.reconcile()

            self.assertEqual(report.actions, [])
            self.assertEqual(report.skipped[0]['reason'], 'not_committed')
            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.RUNNING)

        def test_recently_started_left_untouched(self):
            result = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(result.id)  # Started just now.

            report = self.reconcile(stale_seconds=300)
            self.assertEqual(report.actions, [])
            self.assertEqual(report.skipped[0]['reason'], 'too_recent')

            # The guard can be disabled explicitly.
            report = self.reconcile(stale_seconds=0)
            self.assertEqual(len(report.reset_ready), 1)

        def test_dry_run_changes_nothing(self):
            result = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600)

            report = self.reconcile(dry_run=True)
            self.assertEqual(len(report.reset_ready), 1)

            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.RUNNING)

        def test_queue_unreadable_blocks_orphan_but_not_terminal(self):
            orphan = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(orphan.id, age_seconds=3600)

            failed = explode.enqueue()
            HUEY.dequeue()
            self.mark_running(failed.id, age_seconds=3600)
            self.error_result(failed.id)

            original = HUEY.storage.enqueued_items

            def broken_items(*args, **kwargs):
                raise RuntimeError('queue is down')

            HUEY.storage.enqueued_items = broken_items
            try:
                report = self.reconcile()
            finally:
                HUEY.storage.enqueued_items = original

            self.assertFalse(report.queue_readable)
            self.assertEqual(len(report.synced_failed), 1)
            skipped = [s for s in report.skipped
                       if s['reason'] == 'queue_unreadable']
            self.assertEqual([s['task_id'] for s in skipped], [orphan.id])
            orphan.refresh()
            self.assertEqual(orphan.status, TaskResultStatus.RUNNING)

        def test_result_store_unreadable_aborts(self):
            original = HUEY.storage.result_items

            def broken_items():
                raise RuntimeError('store is down')

            HUEY.storage.result_items = broken_items
            try:
                self.assertRaises(ReconciliationError, self.reconcile)
            finally:
                HUEY.storage.result_items = original

        def test_corrupt_record_reported_not_touched(self):
            HUEY.storage.put_data(KEY % 'deadbeef', b'not-a-serialized-record')
            report = self.reconcile()
            self.assertEqual(report.scanned, 0)
            self.assertEqual(report.errors[0]['reason'], 'corrupt_record')
            self.assertEqual(report.errors[0]['task_id'], 'deadbeef')

        def test_record_changed_concurrently_left_untouched(self):
            result = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600)
            key = KEY % result.id

            original_peek = HUEY.storage.peek_data
            state = {'flipped': False}

            def flipping_peek(raw_key):
                if raw_key == key and not state['flipped']:
                    state['flipped'] = True
                    record = HUEY.get(key, peek=True)
                    record['status'] = TaskResultStatus.SUCCESSFUL.value
                    HUEY.put_result(key, record)
                return original_peek(raw_key)

            HUEY.storage.peek_data = flipping_peek
            try:
                report = self.reconcile()
            finally:
                HUEY.storage.peek_data = original_peek

            self.assertEqual(report.actions, [])
            self.assertEqual(report.skipped[0]['reason'], 'status_changed')
            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)

        def test_non_running_records_ignored(self):
            result = add.enqueue(1, 2)
            self.assertEqual(result.status, TaskResultStatus.READY)
            report = self.reconcile()
            self.assertEqual(report.scanned, 0)
            self.assertEqual(report.actions, [])

        def test_task_id_filter(self):
            first = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(first.id, age_seconds=3600)
            second = add.enqueue(3, 4)
            HUEY.dequeue()
            self.mark_running(second.id, age_seconds=3600)

            report = self.reconcile(task_ids={first.id})
            self.assertEqual(report.scanned, 1)
            self.assertEqual(report.reset_ready[0]['task_id'], first.id)
            second.refresh()
            self.assertEqual(second.status, TaskResultStatus.RUNNING)

        def test_management_command(self):
            import io
            from huey.contrib.djhuey.management.commands \
                import reconcile_task_statuses

            result = add.enqueue(1, 2)
            HUEY.dequeue()
            self.mark_running(result.id, age_seconds=3600)

            command = reconcile_task_statuses.Command()
            command.stdout = command.stderr = io.StringIO()
            rc = command.handle(dry_run=False, stale_seconds=0, task_id=None)
            output = command.stdout.getvalue()

            self.assertTrue(rc is None)
            self.assertIn('RESET to READY', output)
            self.assertIn('orphaned', output)
            self.assertIn('Summary:', output)
            result.refresh()
            self.assertEqual(result.status, TaskResultStatus.READY)

            # Dry run and a corrupt record produce a non-zero exit code.
            HUEY.storage.put_data(KEY % 'broken', b'garbage')
            command.stdout = command.stderr = io.StringIO()
            rc = command.handle(dry_run=True, stale_seconds=0, task_id=None)
            self.assertEqual(rc, 1)
            self.assertIn('[dry-run]', command.stdout.getvalue())
