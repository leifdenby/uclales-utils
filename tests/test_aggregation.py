import os
import tempfile
from pathlib import Path

import luigi
import pytest

import uclales

USE_CDO = os.environ.get("CDO_VERSION", "") != ""
EXTRACTION_MODES = ["blocks", "x_strips", "y_strips"]


@pytest.mark.parametrize("extraction_mode", EXTRACTION_MODES)
def test_extract_3d(testdata_path, extraction_mode):
    if extraction_mode == "blocks" and USE_CDO:
        # skip this test since we can't currently extract-by-blocks with cdo
        # and so the extraction will fail
        return True

    tmpdir = tempfile.TemporaryDirectory()
    output_path = Path(tmpdir.name)

    task = uclales.output.Extract(
        var_name="w",
        tn=0,
        kind="3d",
        file_prefix="rico",
        source_path=testdata_path,
        use_cdo=USE_CDO,
        mode=extraction_mode,
        dest_path=output_path,
    )

    luigi.build([task], local_scheduler=True)
    assert task.output().exists()


@pytest.mark.parametrize("extraction_mode", EXTRACTION_MODES)
def test_extract_2d(testdata_path, extraction_mode):
    if extraction_mode == "blocks" and USE_CDO:
        # skip this test since we can't currently extract-by-blocks with cdo
        # and so the extraction will fail
        return True

    tmpdir = tempfile.TemporaryDirectory()
    output_path = Path(tmpdir.name)

    task = uclales.output.Extract(
        var_name="lwp",
        tn=0,
        kind="2d",
        orientation="xy",
        file_prefix="rico",
        source_path=testdata_path,
        use_cdo=USE_CDO,
        mode=extraction_mode,
        dest_path=output_path,
    )

    luigi.build([task], local_scheduler=True)
    assert task.output().exists()
