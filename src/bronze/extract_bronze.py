"""
Script to extract raw dataset from the kaggle API
"""

import json
import hashlib
import zipfile
import logging
from pathlib import Path
from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi

from .config import KAGGLE_DATASET, RAW_BASE, MANIFEST_DIR, FILE_TO_TABLE, manifest_path, latest_manifest_path, raw_dir

logger = logging.getLogger(__name__)


def compute_hash(filepath, algorithm='sha256'):
    """ compute file hash for deterministic snapshot ID. """
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f: 
        for chunk in iter(lambda:f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def _source_changed() -> bool:
    """ check if the kaggle dataset has been updated before downloading. """
    api = KaggleApi()
    api.authenticate()

    # since kaggle api uses fuzzy matching, we need to search for the exact dataset name
    dataset_list = api.dataset_list(search=KAGGLE_DATASET)
    dataset = next((d for d in dataset_list if str(d) == KAGGLE_DATASET), None)

    if dataset is None:
        return True # this means we cant verify thus assume it has changed
    
    kaggle_last_updated = str(dataset.lastUpdated)

    # to compare against what we have in our last manifest
    path = latest_manifest_path()
    if path is not None:
        manifest = json.loads(path.read_text())
        if manifest.get('kaggle_last_updated') == kaggle_last_updated:
            return False
    
    return True

def _count_csv_rows(filepath: Path) -> int:
    """ count data rows in a CSV file (headers excluded). """
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1

def extract(force: bool = False) -> dict:
    """ downloads dataset, extracts files and returns manifest. """
    # this try/except will proceed with download rather than failing, even if we cant check
    try:
        if not force and not _source_changed():
            logger.info('extract_skipped', extra={'reason': 'source_unchanged'})
            return json.loads(latest_manifest_path().read_text())
    except Exception as e:
        logger.warning('source_check_failed', extra={
            'error': str(e), 
            'action': 'proceeding_with_download',
        }, exc_info=True)
        # continue with download
    
    tmp_dir = RAW_BASE / 'tmp'
    tmp_dir.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    # download dataset
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=tmp_dir, unzip=False)

    # get the last updated timestamp for the manifest
    try:
        dataset_list = api.dataset_list(search=KAGGLE_DATASET)
        dataset = next((d for d in dataset_list if str(d) == KAGGLE_DATASET), None)
        kaggle_last_updated = str(dataset.lastUpdated) if dataset else None
    except Exception as e: 
        logger.warning('kaggle_metadata_fetch_failed', extra={'error': str(e)}, exc_info =True)
        kaggle_last_updated = None

    # hash the zip
    zip_path = next(tmp_dir.glob('*.zip'))
    snapshot_id = compute_hash(zip_path)[:16]

    # extract
    snapshot_dir = raw_dir(snapshot_id)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(snapshot_dir)
    zip_path.unlink()
    tmp_dir.rmdir() # clean up temporary directory

    # manifest
    manifest = { 
        'snapshot_id': snapshot_id,
        'extracted_at': datetime.now(timezone.utc).isoformat(),
        'kaggle_last_updated': kaggle_last_updated,
        'files': [
            {'filename': f, 
            'hash': compute_hash(snapshot_dir / f), 
            'size': (snapshot_dir / f).stat().st_size, 
            'row_count': _count_csv_rows(snapshot_dir / f)}
            for f in FILE_TO_TABLE.keys()
            if (snapshot_dir / f).exists()
        ],
    }

    manifest_path(snapshot_id).write_text(json.dumps(manifest, indent=2))
    logger.info('extract_completed', extra={
        'snapshot_id': snapshot_id,
        'file_count': len(manifest['files']),
    })
    return manifest

if __name__ == "__main__":
    manifest = extract()
    print(f"Extracted snapshot: {manifest['snapshot_id']}")