"""
Script to extract raw dataset from the kaggle API
"""

import json
import hashlib
import zipfile
import logging
from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi

from config import KAGGLE_DATASET, RAW_DIR, MANIFEST_DIR, FILE_TO_TABLE, manifest_path, latest_manifest_path 

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

def extract(force: bool = False) -> dict:
    """ downloads dataset, extracts files and returns manifest. """
    if not force and not _source_changed():
        logger.info('Kaggle source has not changed, skipping extraction')
        return json.loads(latest_manifest_path().read_text())
    
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    # download dataset
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=RAW_DIR, unzip=False)

    # get the last updated timestamp for the manifest
    dataset_list = api.dataset_list(search=KAGGLE_DATASET)
    dataset = next((d for d in dataset_list if str(d) == KAGGLE_DATASET), None)
    kaggle_last_updated = str(dataset.lastUpdated) if dataset else None

    # hash the zip
    zip_path = next(RAW_DIR.glob('*.zip'))
    snapshot_id = compute_hash(zip_path)[:16]

    # extract
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(RAW_DIR)
    zip_path.unlink()

    # manifest
    manifest = { 
        'snapshot_id': snapshot_id,
        'extracted_at': datetime.now(timezone.utc).isoformat(),
        'kaggle_last_updated': kaggle_last_updated,
        'files': [
            {'filename': f, 'hash': compute_hash(RAW_DIR / f), 'size': (RAW_DIR / f).stat().st_size}
            for f in FILE_TO_TABLE.keys()
            if (RAW_DIR / f).exists()
        ],
    }

    manifest_path(snapshot_id).write_text(json.dumps(manifest, indent=2))
    return manifest

if __name__ == "__main__":
    manifest = extract()
    print(f"Extracted snapshot: {manifest['snapshot_id']}")