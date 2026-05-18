"""
CLI entrypoint for the gold layer pipeline.

Usage:
    python -m gold.run_gold
    python -m gold.run_gold --snapshot-id <id>
    python -m gold.run_gold --resume-run-id <uuid>
"""

import argparse
import logging
import sys
from logging_config import setup_logging
from .load_gold import load

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Gold layer pipeline')
    parser.add_argument('--snapshot-id', help='Target snapshot ID (default: latest successful silver run)')
    parser.add_argument('--resume-run-id', help='Resume a previously failed run by its run_id')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    try:
        summary = load(
            snapshot_id=args.snapshot_id,
            run_id=args.resume_run_id,
            resume=bool(args.resume_run_id),
        )
        logger.info('gold_pipeline_completed', extra={
            'run_id': summary.run_id,
            'snapshot_id': summary.snapshot_id,
            'tables_loaded': summary.tables_loaded,
            'tables_failed': summary.tables_failed,
            'tables_rejected': summary.tables_rejected,
        })
    except Exception as e:
        logger.error('gold_pipeline_failed', extra={'error': str(e)}, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
