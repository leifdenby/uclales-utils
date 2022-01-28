import luigi

import uclales


def test_extract_3d(testdata_path):
    task = uclales.output.Extract3D(
        var_name="w",
        tn=0,
        file_prefix="rico",
    )

    luigi.build([task], local_scheduler=True)
