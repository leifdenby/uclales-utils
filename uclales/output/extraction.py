"""
luigi-based pipeline for extracting either:

1. full-domain 3D fields for single variables at single timestep, or
2. full-domain 2D cross-setion fields

from per-core column output from the UCLALES model
"""
import functools
import pprint
import signal
import subprocess
from pathlib import Path

import luigi
import xarray as xr

from .common import _fix_time_units as fix_time_units

PARTIALS_3D_PATH = Path("partials/3d")
PARTIALS_2D_PATH = Path("partials/2d")

SOURCE_BLOCK_FILENAME_FORMAT_3D = "{file_prefix}.{i:04d}{j:04d}.nc"
SINGLE_VAR_BLOCK_FILENAME_FORMAT_3D = (
    "{file_prefix}.{i:04d}{j:04d}.{var_name}.tn{tn}.nc"
)
SINGLE_VAR_STRIP_FILENAME_FORMAT_3D = (
    "{file_prefix}.{dim}.{idx:04d}.{var_name}.tn{tn}.nc"
)
SINGLE_VAR_FILENAME_FORMAT_3D = "{file_prefix}.{var_name}.tn{tn}.nc"

# rico_gcss.out.xy.0000.0000.nc
SOURCE_BLOCK_FILENAME_FORMAT_2D = "{file_prefix}.out.{orientation}.{i:04d}.{j:04d}.nc"
SINGLE_VAR_BLOCK_FILENAME_FORMAT_2D = (
    "{file_prefix}.out.{orientation}.{i:04d}.{j:04d}.{var_name}.nc"
)
SINGLE_VAR_STRIP_FILENAME_FORMAT_2D = (
    "{file_prefix}.out.{orientation}.{dim}.{idx:04d}.{var_name}.nc"
)
SINGLE_VAR_FILENAME_FORMAT_2D = "{file_prefix}.out.{orientation}.{var_name}.nc"

STORE_PARTIALS_LOCALLY = False


class XArrayTarget(luigi.target.FileSystemTarget):
    fs = luigi.local_target.LocalFileSystem()

    def __init__(self, path, *args, **kwargs):
        super(XArrayTarget, self).__init__(path, *args, **kwargs)
        self.path = path

    def open(self, *args, **kwargs):
        # ds = xr.open_dataset(self.path, engine='h5netcdf', *args, **kwargs)
        ds = xr.open_dataset(self.path, *args, **kwargs)

        if len(ds.data_vars) == 1:
            name = list(ds.data_vars)[0]
            da = ds[name]
            da.name = name
            return da
        else:
            return ds


def _execute(cmd):
    print(" ".join(cmd))
    # https://stackoverflow.com/a/4417735
    popen = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd, popen.stderr.read())


def _call_cdo(args, verbose=True, return_output_on_error=False):
    try:
        cmd = ["cdo"] + args
        for output in _execute(cmd):
            if verbose:
                print((output.strip()))

    except subprocess.CalledProcessError as ex:
        if return_output_on_error:
            return vars(ex)["output"]
        return_code = ex.returncode
        error_extra = ""
        if -return_code == signal.SIGSEGV:
            error_extra = ", the utility segfaulted "

        raise Exception(
            "There was a problem when calling the tracking "
            "utility (errno={}): {} {}".format(error_extra, return_code, ex)
        )


@functools.lru_cache(10)
def _cdo_has_command(cmd):
    output = _call_cdo([cmd], verbose=False, return_output_on_error=True)
    has_op = "Operator >gather< not found!" not in output
    return has_op


class XArrayTargetUCLALES(XArrayTarget):
    def open(self, *args, **kwargs):
        kwargs["decode_times"] = False
        da = super().open(*args, **kwargs)
        da["time"], _ = fix_time_units(da["time"])
        if hasattr(da, "to_dataset"):
            return xr.decode_cf(da.to_dataset())
        else:
            return xr.decode_cf(da)


def _build_filename(data_stage, data_kind, **kwargs):
    if data_kind == "3d":
        if kwargs.get("tn") is None and data_stage != "source_block":
            raise Exception("`tn` must be given for 3D output")

        if data_stage == "source_block":
            filename_format = SOURCE_BLOCK_FILENAME_FORMAT_3D
        elif data_stage == "block_variable":
            filename_format = SINGLE_VAR_BLOCK_FILENAME_FORMAT_3D
        elif data_stage == "strip_variable":
            filename_format = SINGLE_VAR_STRIP_FILENAME_FORMAT_3D
        elif data_stage == "full_domain":
            filename_format = SINGLE_VAR_FILENAME_FORMAT_3D
        else:
            raise NotImplementedError(data_stage)
    elif data_kind == "2d":
        if kwargs.get("orientation") is None:
            raise Exception("`orientation` must be given for 2D output")
        if data_stage == "source_block":
            filename_format = SOURCE_BLOCK_FILENAME_FORMAT_2D
        elif data_stage == "block_variable":
            filename_format = SINGLE_VAR_BLOCK_FILENAME_FORMAT_2D
        elif data_stage == "strip_variable":
            filename_format = SINGLE_VAR_STRIP_FILENAME_FORMAT_2D
        elif data_stage == "full_domain":
            filename_format = SINGLE_VAR_FILENAME_FORMAT_2D
        else:
            raise NotImplementedError(data_stage)
    else:
        raise NotImplementedError(data_kind)

    try:
        return filename_format.format(**kwargs)
    except KeyError as e:
        raise Exception(
            f"The {e} parameter is missing for {data_kind} of {data_stage}, "
            f"the provided parameters are: {pprint.pformat(kwargs)}"
        )


def _build_path(data_stage, data_kind, source_path=None, **kwargs):
    fn = _build_filename(data_stage=data_stage, data_kind=data_kind, **kwargs)

    if data_stage == "source_block":
        assert source_path is not None
        path = source_path
    else:
        path = Path(kwargs.get("dest_path", "."))
        if data_stage != "full_domain":
            if data_kind == "3d":
                path = path / PARTIALS_3D_PATH
            elif data_kind == "2d":
                path = path / PARTIALS_2D_PATH
            else:
                raise NotImplementedError(data_kind)

    return Path(path) / fn


def _find_number_of_blocks(source_path, file_prefix, kind, orientation=None):
    kwargs = dict(
        file_prefix=file_prefix,
        data_stage="source_block",
        data_kind=kind,
        orientation=orientation,
    )

    x_filename_pattern = _build_filename(i=9999, j=0, **kwargs).replace("9999", "????")
    y_filename_pattern = _build_filename(j=9999, i=0, **kwargs).replace("9999", "????")

    nx = len(list(Path(source_path).glob(x_filename_pattern)))
    ny = len(list(Path(source_path).glob(y_filename_pattern)))

    if nx == 0 or ny == 0:
        raise Exception(
            f"Didn't find any source files in `{source_path}` "
            f"(nx={nx} and ny={ny} found). Tried `{x_filename_pattern}` "
            f"and `{y_filename_pattern}` patterns"
        )

    return nx, ny


class UCLALESOutputBlock(luigi.ExternalTask):
    """
    Represents 2D or 3D output from model simulations (depending on the value of `kind`)
    """

    file_prefix = luigi.Parameter()
    source_path = luigi.Parameter()
    i = luigi.IntParameter()
    j = luigi.IntParameter()
    kind = luigi.Parameter()
    orientation = luigi.OptionalParameter(default=None)
    dest_path = luigi.OptionalParameter(default=".")

    def output(self):
        p = _build_path(
            file_prefix=self.file_prefix,
            data_stage="source_block",
            data_kind=self.kind,
            orientation=self.orientation,
            i=self.i,
            j=self.j,
            source_path=self.source_path,
            dest_path=self.dest_path,
        )

        if not p.exists():
            raise Exception(f"Missing input file `{p.name}` for `{self.file_prefix}`")

        return XArrayTargetUCLALES(str(p))


class UCLALESBlockSelectVariable(luigi.Task):
    """
    Extracts a single variable at a single timestep from one 3D output block

    3D:
    {file_prefix}.{j:04d}{i:04d}.nc -> {file_prefix}.{j:04d}{i:04d}.{var_name}.tn{tn}.nc
    rico_gcss.00010002.nc -> rico_gcss.00010002.q.tn4.nc
    for var q and timestep 4

    2D:
    {file_prefix}.{i:04d}.{j:04d}.out.{orientation}.nc -> {file_prefix}.{i:04d}.{j:04d}.out.{var_name}.{orientation}.nc
    rico_gcss.0001.0002.out.xy.nc -> rico_gcss.0001.0002.out.cldbase.xy.nc
    for the cloudbase variable
    """

    file_prefix = luigi.Parameter()
    source_path = luigi.Parameter()
    var_name = luigi.Parameter()
    i = luigi.IntParameter()
    j = luigi.IntParameter()
    tn = luigi.OptionalParameter(default=None)
    kind = luigi.Parameter()
    orientation = luigi.OptionalParameter(default=None)
    dest_path = luigi.OptionalParameter(default=".")

    use_cdo = luigi.BoolParameter(default=True)

    def requires(self):
        return UCLALESOutputBlock(
            file_prefix=self.file_prefix,
            i=self.i,
            j=self.j,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )

    def _run_xarray(self):
        ds_block = self.input().open()
        try:
            da_block_var = ds_block[self.var_name]
        except KeyError as ex:
            raise KeyError(
                f"The variable `{self.var_name}` wasn't found, the following"
                " variables are available: "
                f"{', '.join(ds_block.data_vars.keys())}"
            ) from ex

        if self.kind == "2d":
            if self.var_name == "lcl":
                # lifting-condensation levels is computed per-block, but we
                # want to stack on it anyway, so create a xy coord for the
                # center of the block and expand the dims
                da_block_var["xt"] = ds_block.xt.mean()
                da_block_var["yt"] = ds_block.yt.mean()
                da_block_var = da_block_var.expand_dims(["xt", "yt"])
        elif self.kind == "3d":
            # ensure we cast to int here, `luigi.OptionalParameter` is always a
            # string, but indexing by strings can lead to strange behaviour...
            da_block_var = da_block_var.isel(time=int(self.tn)).expand_dims("time")
        else:
            raise NotImplementedError(self.kind)

        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        da_block_var.to_netcdf(self.output().path)

    def run(self):
        if self.use_cdo:
            self._run_cdo()
        else:
            self._run_xarray()

    def _run_cdo(self):
        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        args = []
        if self.kind == "3d":
            # we're chaining selecting a variable and picking a timestep when
            # extracting from 3D files. This can lead to segfaults because the
            # underlyding HDF5 library might not be thread safe
            # https://code.mpimet.mpg.de/projects/cdo/wiki/CDO#Segfault-with-netcdf4-files
            # try to avoid segfaults with hdf5 lib by adding the "-L" flag
            args.append("-L")
        args.append(f"selname,{self.var_name}")

        if self.kind == "3d":
            args.append(f"-seltimestep,{self.tn+1}")

        args += [
            self.input().path,
            self.output().path,
        ]
        _call_cdo(args)

    def output(self):
        p = _build_path(
            file_prefix=self.file_prefix,
            data_stage="block_variable",
            data_kind=self.kind,
            orientation=self.orientation,
            i=self.i,
            j=self.j,
            var_name=self.var_name,
            tn=self.tn,
            dest_path=self.dest_path,
        )

        return XArrayTargetUCLALES(str(p))


class UCLALESStripSelectVariable(luigi.Task):
    """
    Extracts a single variable at a single timestep as a strip of blocks along
    the `dim` dimension at index `idx` in the perpendicular dimension

    3D:
    {file_prefix}.{j:04d}{i:04d}.nc -> {file_prefix}.{idx:04d}.{var_name}.tn{tn}.nc
    rico_gcss.00010002.nc -> rico_gcss.00010002.q.tn4.nc
    for var q and timestep 4

    2D:
    {file_prefix}.{j:04d}.{i:04d}.out.{orientation}.nc -> {file_prefix}.{idx:04d}.{var_name}.tn{tn}.nc
    rico_gcss.00010002.nc -> rico_gcss.00010002.q.tn4.nc
    for var q and timestep 4
    """

    file_prefix = luigi.Parameter()
    source_path = luigi.Parameter()
    var_name = luigi.Parameter()
    idx = luigi.IntParameter()
    dim = luigi.Parameter()
    tn = luigi.OptionalParameter(default=None)
    kind = luigi.Parameter()
    orientation = luigi.OptionalParameter(default=None)
    dest_path = luigi.OptionalParameter(default=".")

    use_cdo = luigi.BoolParameter(default=True)

    def requires(self):
        nx_b, ny_b = _find_number_of_blocks(
            file_prefix=self.file_prefix,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )

        if self.dim == "x":
            make_kws = lambda n: dict(i=self.idx, j=n)  # noqa
            nidx = ny_b
        elif self.dim == "y":
            make_kws = lambda n: dict(i=n, j=self.idx)  # noqa
            nidx = nx_b
        else:
            raise NotImplementedError(self.dim)

        return [
            UCLALESBlockSelectVariable(
                file_prefix=self.file_prefix,
                tn=self.tn,
                var_name=self.var_name,
                source_path=self.source_path,
                kind=self.kind,
                orientation=self.orientation,
                dest_path=self.dest_path,
                use_cdo=self.use_cdo,
                **make_kws(n=n),
            )
            for n in range(nidx)
        ]

    def _run_xarray(self):
        ortho_dim = "x" if self.dim == "y" else "y"

        dataarrays = [inp.open() for inp in self.input()]
        # x -> `xt` or `xm` mapping, similar for other dims
        da = dataarrays[0]
        dims = dict([(d.replace("t", "").replace("m", ""), d) for d in da.dims])

        ds_strip = xr.concat(dataarrays, dim=dims[ortho_dim])
        da_strip_var = ds_strip[self.var_name]
        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        da_strip_var.to_netcdf(self.output().path)

    def run(self):
        if self.use_cdo:
            if _cdo_has_command("gather"):
                cdo_command = "gather"
            else:
                cdo_command = "collgrid"

            # if we're concatenating in the x-direction we need to tell cdo to
            # add an extra dimension for y
            if self.dim == "x":
                cdo_command += ",1"

            args = (
                [cdo_command]
                + [inp.path for inp in self.input()]
                + [self.output().path]
            )
            _call_cdo(args)
        else:
            self._run_xarray()

    def output(self):
        p = _build_path(
            file_prefix=self.file_prefix,
            data_stage="strip_variable",
            data_kind=self.kind,
            orientation=self.orientation,
            idx=self.idx,
            dim=self.dim,
            var_name=self.var_name,
            tn=self.tn,
            dest_path=self.dest_path,
        )

        return XArrayTargetUCLALES(str(p))


class _Merge3DBaseTask(luigi.Task):
    """
    Common functionality for task that merge either strips or blocks together
    to construct datafile for whole domain
    """

    def requires(self):
        return dict(
            first_block=UCLALESBlockSelectVariable(
                file_prefix=self.file_prefix,
                i=0,
                j=0,
                var_name=self.var_name,
                tn=self.tn,
                source_path=self.source_path,
                kind=self.kind,
                orientation=self.orientation,
                dest_path=self.dest_path,
                use_cdo=self.use_cdo,
            )
        )

    def _check_output(self, da):
        # x -> `xt` or `xm` mapping, similar for other dims
        dims = dict([(d.replace("t", "").replace("m", ""), d) for d in da.dims])

        # check that we've aggregated enough bits and have the expected shape
        nx_b, ny_b = _find_number_of_blocks(
            file_prefix=self.file_prefix,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )
        da_first_block = self.input()["first_block"].open()
        b_nx = int(da_first_block[dims["x"]].count())
        b_ny = int(da_first_block[dims["y"]].count())

        nx_da = int(da.coords[dims["x"]].count())
        ny_da = int(da.coords[dims["y"]].count())

        if nx_da != (b_nx * nx_b):
            raise Exception(
                "Resulting data is the the wrong size " f"( {nx_da} != {b_nx} x {nx_b})"
            )

        if ny_da != (b_ny * ny_b):
            raise Exception(
                "Resulting data is the the wrong size " f"( {ny_da} != {b_ny} x {ny_b})"
            )

    def run(self):
        opened_inputs = dict([(inp, inp.open()) for inp in self.input()["parts"]])
        self._check_inputs(opened_inputs)

        class_name = self.__class__.__name__
        if class_name == "ExtractByStrips":
            # when extracting by strips we need to use `xr.concat` instead of
            # `xr.merge`, and so we need to know which dimension to concatenate
            # along
            concat_dim = None
            da_first = next(iter(opened_inputs.values()))
            for d in da_first.dims:
                if d.startswith(self.dim):
                    concat_dim = d
                    break

            # couldn't find dim to concat along
            if concat_dim is None:
                raise NotImplementedError(da_first.dims)
            da = xr.concat(opened_inputs.values(), dim=concat_dim)
        elif class_name == "ExtractByBlocks":
            da_first = self.input()["first_block"].open()[self.var_name]
            # ensure we retain the same coordinate ordering as in the source blocks
            da = xr.merge(opened_inputs.values())[self.var_name].transpose(
                *da_first.dims
            )
        else:
            raise NotImplementedError(class_name)

        self._check_output(da=da)

        Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
        da.to_netcdf(self.output().path)

    def _check_inputs(self, opened_inputs):
        pass

    def output(self):
        p = _build_path(
            file_prefix=self.file_prefix,
            data_stage="full_domain",
            data_kind=self.kind,
            orientation=self.orientation,
            source_path=self.source_path,
            var_name=self.var_name,
            dest_path=self.dest_path,
            tn=self.tn,
        )

        return XArrayTarget(str(p))


class ExtractByBlocks(_Merge3DBaseTask):
    """
    Aggregate all nx*nx blocks for variable `var_name` at timestep `tn` into a
    single file
    """

    file_prefix = luigi.Parameter()
    source_path = luigi.Parameter()
    var_name = luigi.Parameter()
    tn = luigi.OptionalParameter(default=None)
    kind = luigi.Parameter()
    orientation = luigi.OptionalParameter(default=None)
    dest_path = luigi.OptionalParameter(default=".")

    use_cdo = False

    def requires(self):
        tasks = super().requires()
        nx, ny = _find_number_of_blocks(
            file_prefix=self.file_prefix,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )

        tasks_parts = []
        for i in range(nx):
            for j in range(ny):
                t = UCLALESBlockSelectVariable(
                    file_prefix=self.file_prefix,
                    var_name=self.var_name,
                    i=i,
                    j=j,
                    tn=self.tn,
                    kind=self.kind,
                    orientation=self.orientation,
                    source_path=self.source_path,
                    use_cdo=self.use_cdo,
                    dest_path=self.dest_path,
                )
                tasks_parts.append(t)

        tasks["parts"] = tasks_parts
        return tasks


class ExtractByStrips(_Merge3DBaseTask):
    """
    Aggregate all strips along `dim` dimension for `var_name` at timestep `tn` into a
    single file
    """

    file_prefix = luigi.Parameter()
    source_path = luigi.Parameter()
    var_name = luigi.Parameter()
    tn = luigi.OptionalParameter(default=None)
    kind = luigi.Parameter()
    orientation = luigi.OptionalParameter(default=None)
    dim = luigi.Parameter(default="x")
    use_cdo = luigi.BoolParameter(default=True)
    dest_path = luigi.OptionalParameter(default=".")

    def _check_inputs(self, opened_inputs):
        nx_b, ny_b = _find_number_of_blocks(
            file_prefix=self.file_prefix,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )

        # find block size
        da_first_block = self.input()["first_block"].open()
        dims = dict(
            [(d.replace("t", "").replace("m", ""), d) for d in da_first_block.dims]
        )
        b_nx = int(da_first_block[dims["x"]].count())
        b_ny = int(da_first_block[dims["y"]].count())

        if self.dim == "x":
            expected_shape = (b_nx, b_ny * ny_b)
            expected_shape_calc_str = f"({b_nx}, {b_ny} * {ny_b})"
        elif self.dim == "y":
            expected_shape = (b_nx * nx_b, b_ny)
            expected_shape_calc_str = f"({b_nx} * {nx_b}, {b_ny}"

        invalid_shape = {}
        for inp, da_strip in opened_inputs.items():
            strip_shape = (
                int(da_strip[dims["x"]].count()),
                int(da_strip[dims["y"]].count()),
            )
            if strip_shape != expected_shape:
                invalid_shape[inp.path] = strip_shape

        if len(invalid_shape) > 0:
            err_str = (
                "The following input strip files don't have the expected shape "
                f"{expected_shape_calc_str} = {expected_shape}:\n\t"
            )

            err_str += "\n\t".join(
                [f"{shape}: {fn}" for (fn, shape) in invalid_shape.items()]
            )
            raise Exception(err_str)

    def run(self):
        if self.use_cdo:
            if _cdo_has_command("gather"):
                cdo_command = "gather"
            else:
                cdo_command = "collgrid"

            # if we're concatenating in the y-direction we need to tell cdo to
            # add an extra dimension for x
            if self.dim == "y":
                cdo_command += ",1"

            args = (
                [cdo_command]
                + [inp.path for inp in self.input()["parts"]]
                + [self.output().path]
            )
            Path(self.output().path).parent.mkdir(exist_ok=True, parents=True)
            _call_cdo(args)
            # after running cdo we need to check it has the expected content
            da = self.output().open()
            try:
                self._check_output(da=da)
            except Exception:
                Path(self.output().path).unlink()
                raise
        else:
            super(ExtractByStrips, self).run()

    def requires(self):
        nx, ny = _find_number_of_blocks(
            file_prefix=self.file_prefix,
            source_path=self.source_path,
            kind=self.kind,
            orientation=self.orientation,
        )

        if self.dim == "x":
            nidx = nx
        elif self.dim == "y":
            nidx = ny
        else:
            raise NotImplementedError(self.dim)

        tasks = super().requires()

        tasks["parts"] = [
            UCLALESStripSelectVariable(
                file_prefix=self.file_prefix,
                dim=self.dim,
                idx=i,
                tn=self.tn,
                kind=self.kind,
                orientation=self.orientation,
                var_name=self.var_name,
                source_path=self.source_path,
                dest_path=self.dest_path,
                use_cdo=self.use_cdo,
            )
            for i in range(nidx)
        ]
        return tasks


class Extract(luigi.Task):
    """
    Extract a single variable from UCLALES column-based output. `kind` should
    be either `3d` or `2d` indicating whether 3D fields or 2D cross-sections
    are to be extracted. For 3D extraction you must provide a timestep `tn` and
    for 2D extraction the orientation of the extraction (for example `xy`) must
    be given
    """

    file_prefix = luigi.Parameter()
    var_name = luigi.Parameter()
    tn = luigi.OptionalParameter(default=None)
    kind = luigi.Parameter()
    mode = luigi.Parameter(default="y_strips")
    source_path = luigi.Parameter(default=".")
    dest_path = luigi.OptionalParameter(default=".")
    # orientation for 2D cross-sections
    orientation = luigi.OptionalParameter(default=None)
    use_cdo = luigi.BoolParameter(default=True)

    def requires(self):
        if self.mode == "blocks":
            if self.use_cdo:
                raise NotImplementedError(
                    "It isn't currently possible to use cdo to extract-by-blocks"
                    " to avoid creating intermediate strips"
                )
            return ExtractByBlocks(
                file_prefix=self.file_prefix,
                var_name=self.var_name,
                tn=self.tn,
                kind=self.kind,
                orientation=self.orientation,
                source_path=self.source_path,
                dest_path=self.dest_path,
            )
        elif self.mode.endswith("_strips"):
            return ExtractByStrips(
                file_prefix=self.file_prefix,
                use_cdo=self.use_cdo,
                var_name=self.var_name,
                tn=self.tn,
                kind=self.kind,
                orientation=self.orientation,
                dim=self.mode[0],
                source_path=self.source_path,
                dest_path=self.dest_path,
            )
        else:
            raise NotImplementedError(self.mode)

    def output(self):
        return self.input()
