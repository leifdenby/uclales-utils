import luigi

import uclales


def test_extract_3d(testdata_path):
    task = uclales.output.Extract(
        var_name="w",
        tn=0,
        kind="3d",
        file_prefix="rico",
        source_path=testdata_path,
    )

    luigi.build([task], local_scheduler=True)


def test_extract_2d(testdata_path):
    task = uclales.output.Extract(
        var_name="lwp",
        tn=0,
        kind="2d",
        orientation="xy",
        file_prefix="rico",
        source_path=testdata_path,
    )

    luigi.build([task], local_scheduler=True)
