"""
An orchestrator with CLI interface to run the bronze layer pipeline
"""
import argparse
import logging
from extract_bronze import extract
from load_bronze import load


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Bronze layer pipeline')
    parser.add_argument('--extract-only', action='store_true', help='only extract the data, no loading')
    parser.add_argument('--load_only', action='store_true', help='only load (requires existing manifest)')
    parser.add_argument('--snapshot-id', help='override the snapshot ID for load')
    args = parser.parse_args()

    if args.load_only:
        summary = load(snapshot_id=args.snapshot_id)
        logger.info(f"Load completed: {summary.total_rows} rows")
    elif args.extract_only:
        manifest = extract()
        logger.info(f"Extract completed: snapshot {manifest['snapshot_id']}")
    else:
        # the full pipeline
        manifest = extract()
        summary = load(snapshot_id=manifest['snapshot_id'])
        logger.info(f"pipeline completed: {summary.total_rows} rows, snapshot {manifest['snapshot_id']}")

if __name__ == "__main__":
    main()