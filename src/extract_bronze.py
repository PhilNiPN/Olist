"""
Script to extract raw dataset from the kaggle API
"""

import json
import hashlib
import zipfile
from datetime import datetime, timezone
from kaggle.api.kaggle_api_extended import KaggleApi

from config import KAGGLE_DATASET, RAW_DIR, MANIFEST_PATH, FILE_TO_TABLE

def compute_hash(filepath, algorithm='sha256'):
    """ compute file hash for deterministic snapshot ID. """
    hasher = hashlib.new(algorithm)
    with open(filepath, 'rb') as f: 
        for chunk in iter(lambda:f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def extract() -> dict:
    """ downloads dataset, extracts files and returns manifest. """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # download dataset
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=RAW_DIR, unzip=False)

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
        'files': [
            {'filename': f, 'hash': compute_hash(RAW_DIR / f), 'size': (RAW_DIR / f).stat().st_size}
            for f in FILE_TO_TABLE.keys()
            if (RAW_DIR / f).exists()
        ],
    }

    MANIFEST_PATH.write_text(json.dumps(manifest,indent=2))
    return manifest

if __name__ == "__main__":
    manifest = extract()
    print(f"Extracted snapshot: {manifest['snapshot_id']}")