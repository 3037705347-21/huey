"""
Reconciliation of django.tasks result records stuck in the RUNNING state.

Tasks executed through :class:`HueyBackend` persist their status record in
huey's result store under the key ``djt.<task-id>``. The worker flips the
record to RUNNING when execution starts; if the worker process is killed
before the terminal status is written, the record stays RUNNING forever,
even though the queue and the result store already show what really
happened:

* a huey ``Error`` result stored under the bare task id means the task
  failed -- the record is synced to FAILED;
* any other value stored under the bare task id means the task returned
  successfully -- the record is synced to SUCCESSFUL;
* a message with the same id still sitting in the queue or the schedule
  means execution never actually started -- the record is reset to READY;
* with no message anywhere and no stored result the worker died
  mid-execution -- the record is also reset to READY, but no new message
  is enqueued. Reconciliation repairs the *displayed status* only; it
  never re-runs or re-enqueues a task.

Records are left untouched (and reported) when the enqueueing transaction
never committed (``enqueued_at`` missing), the task started too recently
to be certain it is not still running, the queue or schedule cannot be
read, or the real status cannot be determined.

The operation is idempotent and safe to run repeatedly.
"""
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

from django.utils import timezone

try:
    from django.tasks import TaskResultStatus
except ImportError:
    # Django < 6.0: the django-tasks backport provides the same API.
    from django_tasks import TaskResultStatus

try:
    from django.utils.json import normalize_json
except ImportError:
    # django-tasks backport (Django < 6.0).
    from django_tasks.utils import normalize_json

from huey.constants import EmptyData
from huey.contrib.djhuey import HUEY
from huey.contrib.djhuey.tasks_backend import KEY
from huey.utils import Error


RECORD_PREFIX = 'djt.'

READY = TaskResultStatus.READY.value
RUNNING = TaskResultStatus.RUNNING.value
FAILED = TaskResultStatus.FAILED.value
SUCCESSFUL = TaskResultStatus.SUCCESSFUL.value

# A task whose RUNNING record is younger than this is left alone: huey has
# no in-progress tracking or heartbeat, so a recent record may belong to a
# task that is genuinely still executing.
DEFAULT_STALE_SECONDS = 300

# Actions applied to records.
RESET_TO_READY = 'reset_to_ready'
SYNCED_SUCCESSFUL = 'synced_successful'
SYNCED_FAILED = 'synced_failed'

# Reasons for the actions above.
QUEUED = 'queued'
SCHEDULED = 'scheduled'
ORPHANED = 'orphaned'
STORED_RESULT = 'stored_result'
STORED_ERROR = 'stored_error'

# Reasons a record was skipped and left untouched.
NOT_COMMITTED = 'not_committed'
TOO_RECENT = 'too_recent'
QUEUE_UNREADABLE = 'queue_unreadable'
UNDETERMINED = 'undetermined'
STATUS_CHANGED = 'status_changed'
RECORD_MISSING = 'record_missing'

# Errors (record could not be processed).
CORRUPT_RECORD = 'corrupt_record'
STORAGE_ERROR = 'storage_error'


class ReconciliationError(RuntimeError):
    """The result store itself could not be read; reconciliation aborted."""


@dataclass
class ReconciliationReport:
    dry_run: bool = False
    stale_seconds: int = DEFAULT_STALE_SECONDS
    immediate: bool = False
    queue_readable: bool = True
    schedule_readable: bool = True
    scanned: int = 0
    actions: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    @property
    def reset_ready(self):
        return [a for a in self.actions if a['action'] == RESET_TO_READY]

    @property
    def synced_successful(self):
        return [a for a in self.actions if a['action'] == SYNCED_SUCCESSFUL]

    @property
    def synced_failed(self):
        return [a for a in self.actions if a['action'] == SYNCED_FAILED]

    @property
    def has_errors(self):
        return bool(self.errors) or not (self.queue_readable and
                                         self.schedule_readable)

    def _count_reasons(self, items, action=None):
        counts = {}
        for item in items:
            if action is None or item['action'] == action:
                counts[item['reason']] = counts.get(item['reason'], 0) + 1
        return counts

    @property
    def summary(self):
        parts = ['scanned=%d' % self.scanned]

        reset = self._count_reasons(self.reset_ready)
        reset_bits = ['%s=%d' % (reason, reset.get(reason, 0))
                      for reason in (QUEUED, SCHEDULED, ORPHANED)]
        parts.append('reset_to_ready=%d (%s)' % (
            len(self.reset_ready), ', '.join(reset_bits)))
        parts.append('synced_successful=%d' % len(self.synced_successful))
        parts.append('synced_failed=%d' % len(self.synced_failed))
        parts.append('skipped=%d' % len(self.skipped))
        parts.append('errors=%d' % len(self.errors))
        if not self.queue_readable:
            parts.append('queue unreadable')
        if not self.schedule_readable:
            parts.append('schedule unreadable')
        return ', '.join(parts)


def _entry(task_id, action_or_reason, detail=None, **extra):
    entry = {'task_id': task_id, 'reason': action_or_reason, 'detail': detail}
    entry.update(extra)
    return entry


def _action(task_id, action, reason, detail=None):
    entry = {'task_id': task_id, 'action': action, 'reason': reason,
             'detail': detail}
    return entry


def _deserialize(huey, raw):
    return huey.serializer.deserialize(raw)


def _message_ids(huey, loader):
    """
    Return (ids, readable). ``readable`` is False if the queue/schedule
    could not be enumerated at all, or if a single message could not be
    deserialized -- in both cases absence of a message cannot be trusted.
    """
    try:
        messages = loader()
    except Exception as exc:
        return set(), False, 'storage error: %s' % exc

    ids = set()
    for data in messages:
        try:
            message = huey.serializer.deserialize(data)
        except Exception as exc:
            return ids, False, 'undecodable message: %s' % exc
        ids.add(message.id)
    return ids, True, None


def _parse_timestamp(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed)
    return parsed


def _reset_to_ready(record):
    record['status'] = READY
    record['started_at'] = None
    record['last_attempted_at'] = None
    record['finished_at'] = None
    record['return_value'] = None
    record['errors'] = []
    record['worker_ids'] = []


def _mark_successful(record, value, now_iso):
    record['status'] = SUCCESSFUL
    if not record.get('finished_at'):
        record['finished_at'] = now_iso
    record['return_value'] = value


def _mark_failed(record, error, now_iso):
    record['status'] = FAILED
    if not record.get('finished_at'):
        record['finished_at'] = now_iso
    metadata = error.metadata or {}
    traceback = metadata.get('traceback') or metadata.get('error') or ''
    errors = record.setdefault('errors', [])
    if traceback and not any(len(e) > 1 and e[1] == traceback for e in errors):
        # The bare huey error carries a traceback and repr, but not the
        # importable exception class path.
        errors.append(['', traceback])


def reconcile_running_tasks(huey=None, *, stale_seconds=DEFAULT_STALE_SECONDS,
                            dry_run=False, now=None, task_ids=None):
    """
    Reconcile django.tasks records stuck in RUNNING.

    :param huey: the huey instance (defaults to the configured one).
    :param int stale_seconds: only reconcile records whose latest attempt
        started at least this many seconds ago; ``0`` disables the guard.
    :param bool dry_run: classify and report without writing anything.
    :param now: injectable current datetime (tests).
    :param task_ids: optional iterable restricting the records considered.
    :returns: :class:`ReconciliationReport`.
    :raises ReconciliationError: if the result store cannot be enumerated
        at all -- nothing is changed.
    """
    huey = huey or HUEY
    now = now or timezone.now()
    now_iso = now.isoformat()
    if task_ids is not None:
        task_ids = set(task_ids)

    report = ReconciliationReport(
        dry_run=dry_run,
        stale_seconds=stale_seconds,
        immediate=huey.immediate)

    try:
        items = huey.storage.result_items()
    except Exception as exc:
        raise ReconciliationError(
            'Could not read the huey result store, aborting: %s'
            % exc) from exc

    queued_ids, queue_readable, queue_detail = _message_ids(
        huey, huey.storage.enqueued_items)
    scheduled_ids, schedule_readable, schedule_detail = _message_ids(
        huey, huey.storage.scheduled_items)
    report.queue_readable = queue_readable
    report.schedule_readable = schedule_readable
    if not queue_readable:
        report.errors.append(_entry(None, QUEUE_UNREADABLE, queue_detail))
    if not schedule_readable:
        report.errors.append(_entry(None, 'schedule_unreadable',
                                    schedule_detail))

    for raw_key, raw_record in items.items():
        if isinstance(raw_key, bytes):
            raw_key = raw_key.decode('utf8')
        if not raw_key.startswith(RECORD_PREFIX):
            continue
        task_id = raw_key[len(RECORD_PREFIX):]
        if task_ids is not None and task_id not in task_ids:
            continue

        try:
            snapshot = _deserialize(huey, raw_record)
        except Exception as exc:
            report.errors.append(_entry(task_id, CORRUPT_RECORD, str(exc)))
            continue
        if not isinstance(snapshot, dict) or 'status' not in snapshot:
            report.errors.append(_entry(task_id, CORRUPT_RECORD,
                                        'record is not a django task status '
                                        'document'))
            continue
        if snapshot.get('status') != RUNNING:
            continue

        report.scanned += 1

        # The enqueueing transaction never committed (ENQUEUE_ON_COMMIT):
        # the document never reached the store in this state under normal
        # flow, so anything missing enqueued_at is not ours to touch.
        if not snapshot.get('enqueued_at'):
            report.skipped.append(_entry(
                task_id, NOT_COMMITTED,
                'record has no enqueued_at; enqueue may not have committed'))
            continue

        try:
            started = _parse_timestamp(
                snapshot.get('started_at') or
                snapshot.get('last_attempted_at'))
        except (TypeError, ValueError) as exc:
            report.skipped.append(_entry(
                task_id, UNDETERMINED,
                'unparseable started_at: %s' % exc))
            continue
        if started is None:
            report.skipped.append(_entry(
                task_id, UNDETERMINED, 'running record has no started_at'))
            continue

        if stale_seconds:
            age = (now - started).total_seconds()
            if age < stale_seconds:
                report.skipped.append(_entry(
                    task_id, TOO_RECENT,
                    'last attempt started %.0fs ago, may still be running'
                    % age))
                continue

        action = reason = detail = None
        raw_bare = items.get(task_id)
        if raw_bare is not None:
            try:
                bare = _deserialize(huey, raw_bare)
            except Exception as exc:
                report.skipped.append(_entry(
                    task_id, UNDETERMINED,
                    'result under task id could not be decoded: %s' % exc))
                continue
            if isinstance(bare, Error):
                action, reason = SYNCED_FAILED, STORED_ERROR
            else:
                action, reason, detail = (
                    SYNCED_SUCCESSFUL, STORED_RESULT, bare)
        elif task_id in queued_ids:
            action, reason = RESET_TO_READY, QUEUED
        elif task_id in scheduled_ids:
            action, reason = RESET_TO_READY, SCHEDULED
        elif not (queue_readable and schedule_readable):
            # No terminal result, but we cannot prove no message is waiting.
            report.skipped.append(_entry(
                task_id, QUEUE_UNREADABLE,
                'queue/schedule unreadable while task is unresolved'))
            continue
        else:
            # Dequeued, never finished, no result left behind: the worker
            # died mid-execution. Show the task as pending again, but do
            # not enqueue a new message.
            action, reason = RESET_TO_READY, ORPHANED

        updated = dict(snapshot)
        if action == RESET_TO_READY:
            _reset_to_ready(updated)
        elif action == SYNCED_SUCCESSFUL:
            try:
                value = normalize_json(bare)
            except Exception as exc:
                report.skipped.append(_entry(
                    task_id, UNDETERMINED,
                    'stored result is not json-serializable: %s' % exc))
                continue
            _mark_successful(updated, value, now_iso)
        else:
            _mark_failed(updated, bare, now_iso)

        if dry_run:
            report.actions.append(_action(task_id, action, reason))
            continue

        # Compare-and-swap: only overwrite if the document is still the
        # exact RUNNING attempt we reconciled.
        fresh_raw = huey.storage.peek_data(KEY % task_id)
        if fresh_raw is EmptyData or fresh_raw is None:
            report.skipped.append(_entry(task_id, RECORD_MISSING))
            continue
        try:
            fresh = _deserialize(huey, fresh_raw)
        except Exception as exc:
            report.errors.append(_entry(task_id, CORRUPT_RECORD, str(exc)))
            continue
        if not isinstance(fresh, dict):
            report.errors.append(_entry(task_id, CORRUPT_RECORD,
                                        'record is no longer a status '
                                        'document'))
            continue
        if (fresh.get('status') != RUNNING or
                fresh.get('started_at') != snapshot.get('started_at') or
                fresh.get('last_attempted_at') !=
                snapshot.get('last_attempted_at') or
                fresh.get('worker_ids') != snapshot.get('worker_ids')):
            report.skipped.append(_entry(
                task_id, STATUS_CHANGED,
                'record changed during reconciliation, left untouched'))
            continue

        try:
            huey.put_result(KEY % task_id, updated)
        except Exception as exc:
            report.errors.append(_entry(task_id, STORAGE_ERROR, str(exc)))
            continue
        report.actions.append(_action(task_id, action, reason, detail))

    return report
