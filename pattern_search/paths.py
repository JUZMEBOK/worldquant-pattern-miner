import os
from pathlib import Path

# Package lives one level deep, so go up two parents to reach project root.
_PROJECT_DIR = str(Path(__file__).resolve().parent.parent)
_CRED_DIR = os.path.join(_PROJECT_DIR, "credentials")
_DATAFIELDS_DIR = os.path.join(_PROJECT_DIR, "datafields")
_DATA_DIR = os.path.join(_PROJECT_DIR, "data")

PROJECT_DIR = _PROJECT_DIR
