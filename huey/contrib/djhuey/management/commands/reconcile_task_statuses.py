from django.core.management.base import BaseCommand
from django.core.management.base import CommandError


class Command(BaseCommand):
    help = (
        'Reconcile django.tasks records that are stuck in RUNNING against '
        'the actual queue and result store: records with a stored terminal '
        'result are synced to SUCCESSFUL/FAILED, records that never ran are '
        'reset to READY, and nothing is re-enqueued. Ambiguous records are '
        'left untouched and reported. Safe to run repeatedly.')

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true', dest='dry_run',
            help='Only report what would change; do not write anything.')
        parser.add_argument(
            '--stale-seconds', type=int, default=300, metavar='SECONDS',
            help='Only reconcile a RUNNING record whose last attempt '
                 'started at least SECONDS ago (default 300). Use 0 to '
                 'reconcile regardless of age.')
        parser.add_argument(
            '--task-id', action='append', default=None, metavar='ID',
            help='Limit reconciliation to the given task id(s). May be '
                 'supplied multiple times.')

    def handle(self, *args, **options):
        from huey.contrib.djhuey import HUEY
        try:
            from huey.contrib.djhuey.reconcile import (
                ReconciliationError, reconcile_running_tasks)
        except ImportError as exc:
            raise CommandError(
                'Reconciliation requires the django.tasks framework '
                '(Django 6.0+ or the django-tasks backport): %s' % exc)

        if HUEY.immediate:
            self.stdout.write(
                'WARNING: huey is in immediate mode and uses a '
                'process-local in-memory store; there are no persistent '
                'task records to reconcile here.')

        try:
            report = reconcile_running_tasks(
                HUEY,
                stale_seconds=options['stale_seconds'],
                dry_run=options['dry_run'],
                task_ids=options['task_id'])
        except ReconciliationError as exc:
            raise CommandError(str(exc))

        prefix = '[dry-run] would ' if options['dry_run'] else ''

        for item in report.actions:
            label, note = self._describe(item['action'], item['reason'])
            self.stdout.write('%s%s  %s  %s%s' % (
                prefix, item['task_id'], label, item['reason'],
                ('  (%s)' % note) if note else ''))

        for item in report.skipped:
            self.stdout.write('SKIP  %s  %s  (%s)' % (
                item['task_id'], item['reason'], item['detail']))

        for item in report.errors:
            task_id = item['task_id'] or '<storage>'
            self.stdout.write('ERROR  %s  %s  (%s)' % (
                task_id, item['reason'], item['detail']))

        self.stdout.write('Summary: %s%s' % (
            '[dry-run] ' if options['dry_run'] else '', report.summary))

        if report.has_errors:
            return 1

    def _describe(self, action, reason):
        descriptions = {
            'reset_to_ready': 'RESET to READY',
            'synced_successful': 'SYNCED to SUCCESSFUL',
            'synced_failed': 'SYNCED to FAILED',
        }
        notes = {
            'queued': 'message is still waiting in the queue',
            'scheduled': 'message is still waiting in the schedule',
            'orphaned': 'worker vanished mid-execution; no message was '
                        're-enqueued',
            'stored_result': 'a return value is stored under the task id',
            'stored_error': 'an error result is stored under the task id',
        }
        return descriptions.get(action, action), notes.get(reason, '')
