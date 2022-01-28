# coding: utf-8
"""
Plot time-series statistics for UCLALES output
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa
import netCDF4  # noqa
from matplotlib.gridspec import GridSpec  # noqa

DEFAULT_VARS = ["cfrac", "shf_bar", "lhf_bar", "sfcbflx"]


def comparison_plot(var_name, ax, *datasets):
    for dataset in datasets:
        fh, label = dataset
        ax.plot(fh.variables["time"], fh.variables[var_name], label=label)

    ax.xlabel(
        "{} [{}]".format(fh.variables["time"].longname, fh.variables["time"].units)
    )
    ax.ylabel(
        "{} [{}]".format(fh.variables[var_name].longname, fh.variables[var_name].units)
    )
    ax.legend()
    ax.grid(True)


def main(data_path, vars=DEFAULT_VARS):
    dataset_name = Path(data_path).absolute().name
    n_vars = len(vars)

    fig, axes = plt.subplots(nrows=n_vars, figsize=(10, 3 * n_vars))
    for (ax, var_name) in zip(axes, args.vars):
        fn = Path(data_path) / "{}.ts.nc".format(dataset_name)
        fh = netCDF4.Dataset(fn)

        comparison_plot(var_name=var_name, ax=ax, datasets=[(fh, dataset_name)])

        ax.set_xlim(0, args.tmax)

    return fig, axes


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser(__doc__)
    argparser.add_argument("dataset_name", type=str)
    argparser.add_argument("--vars", default=DEFAULT_VARS)
    argparser.add_argument("--tmax", default=None)

    args = argparser.parse_args()

    dataset_name = args.dataset_name
    vars = args.vars

    fig, axes = main(dataset_name=dataset_name, vars=vars)
    fig.savefig("timeseries_statistics.pdf")
