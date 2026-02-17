"""
An orchestrator with CLI interface to run the bronze layer pipeline
"""
import argparse
import logging
import sys
from logging_config import setup_logging
from .extract_bronze import extract
from .load_bronze import load

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description = 'Bronze layer pipeline')
    parser.add_argument('--extract-only', action = 'store_true', help = 'only extract the data, no loading')
    parser.add_argument('--load-only', action = 'store_true', help = 'only load (requires existing manifest)')
    parser.add_argument('--snapshot-id', help = 'override the snapshot ID for load')
    parser.add_argument('--force', action = 'store_true', help = 'force re-download even if source is unchanged')
    parser.add_argument('--log-level', default = 'INFO', help = 'logging level (DEBUG, INFO, WARNING, ERROR)')
    args = parser.parse_args()

    setup_logging(level = args.log_level)

    try:
        if args.load_only:
            summary = load(snapshot_id=args.snapshot_id)
            logger.info("load completed", extra= {'total_rows': summary.total_rows, 'run_id': summary.run_id})
        elif args.extract_only:
            manifest = extract(force=args.force)
            logger.info("extract completed", extra= {'snapshot_id': manifest['snapshot_id']})
        else:
            # the full pipeline
            manifest = extract(force=args.force)
            summary = load(snapshot_id=manifest['snapshot_id'])
            logger.info("pipeline completed", extra={
                'total_rows': summary.total_rows, 
                'snapshot_id': manifest['snapshot_id'],
                'run_id': summary.run_id,
            })
    except Exception as e:
        logger.error('pipeline_failed', extra={
            'error': str(e),
            'error_type': type(e).__name__,
        }, exc_info=True)
        sys.exit(1) # this will tell orchestrators(i.e. docker, airflow, etc) that the pipeline failed

if __name__ == "__main__":
    main()