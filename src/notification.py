"""
Pipeline notification dispatch.
Handlers are called after each pipeline run completes.
"""

import os
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class PipelineOutcome:
    run_id: str
    layer: str
    status: str
    tables_loaded: int
    tables_failed: int
    dq_failures: list[dict] = field(default_factory=list)
    tables_rejected: int = 0


def _log_handler(outcome: PipelineOutcome):
    if outcome.status == 'success':
        return
    logger.warning('pipeline_alert', extra={
        'run_id': outcome.run_id,
        'layer': outcome.layer,
        'status': outcome.status,
        'tables_failed': outcome.tables_failed,
        'tables_rejected': outcome.tables_rejected,
        'dq_failure_count': len(outcome.dq_failures),
    })


def _slack_handler(outcome: PipelineOutcome):
    if outcome.status == 'success':
        return
    webhook = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook:
        return
    import requests
    emoji = ':rotating_light:' if outcome.status == 'failed' else ':warning:'
    requests.post(webhook, json={
        'text': (
            f"{emoji} *{outcome.layer}* pipeline run `{outcome.run_id}` "
            f"finished with status *{outcome.status}*\n"
            f"Loaded: {outcome.tables_loaded} | Failed: {outcome.tables_failed} | "
            f"DQ-rejected: {outcome.tables_rejected} | DQ failures: {len(outcome.dq_failures)}"
        ),
    }, timeout=10)


_HANDLERS = [_log_handler, _slack_handler]


def notify(outcome: PipelineOutcome):
    for handler in _HANDLERS:
        try:
            handler(outcome)
        except Exception as e:
            logger.error('notification_handler_failed', extra={
                'handler': handler.__name__, 'error': str(e),
            })