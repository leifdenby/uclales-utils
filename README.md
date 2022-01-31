# UCLALES utils in python

This package contains utilities for working with output from the [UCLALES
model](https://github.com/uclales/uclales).

# Installation

`uclales-utils` can be installed with `pip` directly from github

```bash
pip install git+https://github.com/leifdenby/uclales-utils#egg=uclales-utils
```

## Usage

### Extracting 2D cross-sections and 3D fields from UCLALES output

Because UCLALES creates a netCDF for each individual core (when running
multi-core simulations using MPI) these files must be aggregated together to
extract the full 3D field (or a 2D cross-section) for a variable.
`uclales-utils` has functionality implemented to extract the 3D field for a
single timestep or 2D cross-section of a specific variable. To make the
extraction faster, and to break the extraction down into individual steps that
can be checked, this is implemented using the
[luigi](https://github.com/spotify/luigi) pipeline package. Executing the
pipeline may either be done using a single worker, or if your computer has
multiple CPUs you may speed up the extraction process by using multiple
workers.

For serial executing of the extraction run

```bash
python -m luigi --module uclales.output Extract --kind <3d or 2d> --file-prefix <file-prefix> --var-name <variable> [--tn <timestep>] [--orientation <cross-section-orientation>] --local-scheduler
```

For example, to extract the 3D vertical velocity (`w`) field at the 5th timestep
(counting the initial time at `0`) from a collecting of output files prefixed
by `rico` in the filename (i.e. the 3D files are called `rico.########.nc`)

```bash
python -m luigi --module uclales.output Extract --kind 3d --file-prefix rico --tn 5 --var-name w --local-scheduler
```

Or to extract say the 2D field liquid-water path (`lwp`) you would run

```bash
python -m luigi --module uclales.output Extract --kind 2d --file-prefix rico --var-name lwp --orientation xy --local-scheduler
```

You can optionally provide the arguments `--source-path` and `--dest-path` to
set which paths to search for input from and where the output will be stored
(the default is the current working path by default). Intermediate files will
be stored in `partials`.

To run the extraction across multiple workers in parallel you must start
`luigid` in a separate process, and then run the above command replacing
`--local-scheduler` with `--workers <number-of-workers>`

For example if you have 8 cores on your machine you might run

```bash
python -m luigi --module uclales.output Extract --kind 3d --file-prefix rico --tn 5 --var-name w --workers 8
```

While `luigid` is running you can check the progress on the extraction process
by using luigi's web-interface and opening the URL http://localhost:8082/ in your
browser.
