import argparse
import logging
import sys
from logging_config import setup_logging
from .load_silver import load

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Silver layer pipeline')
    parser.add_argument('--snapshot-id', required=True, help='Target snapshot ID')
    parser.add_argument('--log-level', default='INFO')
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    try:
        load(target_snapshot_id=args.snapshot_id)
    except Exception as e:
        logger.error('silver_pipeline_failed', extra={'error': str(e)}, exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()