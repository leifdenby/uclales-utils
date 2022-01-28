import uclales


def test_timeseries_plot(testdata_path):
    uclales.plots.timeseries_statistics.main(data_path=testdata_path)
