import os
import tarfile
import tempfile
from pathlib import Path

import pytest
import requests

TESTDATA_URL = "http://gws-access.jasmin.ac.uk/public/eurec4auk/testdata/uclales.testdata.tar.gz"  # noqa

if os.environ.get("UCLALES_TESTDATA_DIR", None):
    TESTDATA_DIR = Path(os.environ["UCLALES_TESTDATA_DIR"])
else:
    tempdir = tempfile.TemporaryDirectory()
    TESTDATA_DIR = Path(tempdir.name)


def _download_testdata():
    fhtar = tempfile.NamedTemporaryFile(delete=False, suffix=".tar.gz")

    r = requests.get(TESTDATA_URL)
    fhtar.write(r.content)
    fhtar.close()

    tarfile.open(fhtar.name, "r:gz").extractall(TESTDATA_DIR)


def ensure_testdata_available():
    if not TESTDATA_DIR.exists():
        raise Exception(f"Couldn't find test-data directory {TESTDATA_DIR}")
    # Download testdata if it is not there yet
    if len(list(TESTDATA_DIR.glob("**/*.nc"))) == 0:
        print("Downloading testdata...")
        _download_testdata()


@pytest.fixture
def testdata_path():
    """
    These are used for the CLI tests. We might want to add input definitions to
    the testdata (see `make_test_data.py`) and test more CLI calls in future.
    """
    ensure_testdata_available()
    return Path(TESTDATA_DIR)
