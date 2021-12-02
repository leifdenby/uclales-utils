# coding: utf-8
"""
Plot time-series statistics for UCLALES output
"""

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plot  # noqa
import netCDF4  # noqa
from matplotlib.gridspec import GridSpec  # noqa


def comparison_plot(var_name, *datasets):
    for dataset in datasets:
        fh, label = dataset
        plot.plot(fh.variables["time"], fh.variables[var_name], label=label)

    plot.xlabel(
        "{} [{}]".format(fh.variables["time"].longname, fh.variables["time"].units)
    )
    plot.ylabel(
        "{} [{}]".format(fh.variables[var_name].longname, fh.variables[var_name].units)
    )
    plot.legend()
    plot.grid(True)


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser(__doc__)
    argparser.add_argument("dataset_name", type=str)
    argparser.add_argument("--vars", default=["cfrac", "shf_bar", "lhf_bar", "sfcbflx"])
    argparser.add_argument("--tmax", default=None)

    args = argparser.parse_args()

    dataset_name = args.dataset_name

    n_vars = len(args.vars)
    plot_grids = iter(GridSpec(n_vars, 1))

    fig = plot.figure(figsize=(10, 3 * n_vars))
    for var_name in args.vars:
        print(var_name)
        plot.subplot(next(plot_grids))
        fn = "{}.ts.nc".format(dataset_name)
        fh = netCDF4.Dataset(fn)

        comparison_plot(var_name, (fh, dataset_name))

    plot.xlim(0, args.tmax)
    plot.savefig("timeseries_statistics.pdf")
