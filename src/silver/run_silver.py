"""
This module provides a CLI interface to run silver layer pipeline.
"""

import argparse
import logging
import sys
from logging_config import setup_logging
from .load_silver import load

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Silver layer pipeline')
    parser.add_argument('--snapshot-id', help='Target snapshot ID(default: latest)')
    parser.add_argument('--resume-run-id', help='Resume a previously failed run by its run_id')
    parser.add_argument('--log-level', default='INFO', help='logging level')
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    try:
        summary = load(
            snapshot_id = args.snapshot_id,
            run_id=args.resume_run_id,
            resume=bool(args.resume_run_id),
            )
        logger.info('silver_pipeline_completed', extra = {
            'run_id': summary.run_id,
            'snapshot_id': summary.snapshot_id,
            'tables_loaded': summary.tables_loaded,
            'tables_failed': summary.tables_failed,
        })
    except Exception as e:
        logger.error('silver_pipeline_failed', extra={'error': str(e)}, exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
