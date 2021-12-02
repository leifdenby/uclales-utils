"""
Taking the slices of output data from UCLALES for the RICO test case generate
fields that have fields for temperature and density
"""

import os
import sys
import warnings

import numpy as np

# import netCDF4
import scipy.io

from uclales.loader import UCLALES_NetCDFHandler

FILL_VALUE = -1.0e33
fname_base = "rico_gcss.out.xy.k{k}.{var}.nc"


def run(k_indecies):

    timesteps = np.array([240, 480, 720]) / 60 / 2

    print("extracting data for the following timesteps:", timesteps)

    for k in k_indecies:
        print("New slice:", k)
        sys.stdout.flush()

        fh_base = scipy.io.netcdf_file(fname_base.format(k=k, var="t"), mode="r")

        Nt, Nx, Ny, _ = fh_base.variables["t"].shape

        # create a new file to fill the values into
        copy_dimensions = ["time", "zt", "x", "y"]

        temperature_filename = fname_base.format(k=k, var="T")
        density_filename = fname_base.format(k=k, var="rho")

        add_temperature_info = False
        add_density_info = False

        if not os.path.exists(temperature_filename):
            add_temperature_info = True

            fh_T = scipy.io.netcdf_file(temperature_filename, mode="w")
            for d in copy_dimensions:
                fh_T.createDimension(d, fh_base.dimensions[d])

            T_var__handle = fh_T.createVariable(
                name="T", type="float", dimensions=fh_base.variables["t"].dimensions
            )
            T_var__handle.units = "K"
            T_var__handle.longname = "Absolute temperature"
            T_var__handle._FillValue = FILL_VALUE
            T_var__handle.missing_value = FILL_VALUE
            T_var__handle[:] = FILL_VALUE * np.ones((Nt, Nx, Ny, 1))
        else:
            print(temperature_filename)
            fh_T = scipy.io.netcdf_file(temperature_filename, mode="a")

        if not os.path.exists(density_filename):
            add_density_info = True

            fh_rho = scipy.io.netcdf_file(density_filename, mode="w")
            for d in copy_dimensions:
                fh_rho.createDimension(d, fh_base.dimensions[d])

            rho_var__handle = fh_rho.createVariable(
                name="rho", type="float", dimensions=fh_base.variables["t"].dimensions
            )
            rho_var__handle.units = "kg/m^3"
            rho_var__handle.longname = "density"
            rho_var__handle._FillValue = FILL_VALUE
            rho_var__handle.missing_value = FILL_VALUE
            rho_var__handle[:] = FILL_VALUE * np.ones((Nt, Nx, Ny, 1))
        else:
            fh_rho = scipy.io.netcdf_file(density_filename, mode="a")

        # add missing variables
        f_handles = [fh_T, fh_rho]
        vars = ["zt", "time"]
        for var_name in vars:
            for fh in f_handles:
                print("adding", var_name)
                var_handle = fh_base.variables[var_name]
                if var_name not in fh.variables:
                    var_handle__new = fh.createVariable(
                        name=var_name,
                        type=var_handle.typecode(),
                        dimensions=(var_name,),
                    )
                else:
                    var_handle__new = fh.variables[var_name]
                var_handle__new.units = var_handle.units
                var_handle__new[:] = var_handle[:]

        if add_density_info or add_temperature_info:
            for tn in timesteps:
                print("timestep:", tn)

                fhs = {}

                def get_data(var_name, expected_units):
                    # fh = netCDF4.Dataset(fname_base.format(k=k, var=var_name))
                    if var_name not in fhs:
                        fhs[var_name] = scipy.io.netcdf_file(
                            fname_base.format(k=k, var=var_name)
                        )

                    fh = fhs[var_name]

                    var_handle = fh.variables[var_name]

                    assert var_handle.units.startswith(expected_units)

                    return var_handle[tn, :, :, :]

                theta_l = get_data("t", "K")
                p = get_data("p", "Pa")

                # XXX: OBS! The output from UCLALES is actually stored as kg/kg
                # even though the units say g/kg
                r_t = get_data("q", "g/kg")  # /1000
                r_l = get_data("l", "g/kg")  # /1000
                r_r = get_data("r", "g/kg")  # /1000

                # sanity check that we don't have g/kg data
                assert r_t.max() < 1.0
                assert r_l.max() < 1.0
                assert r_r.max() < 1.0

                q_t = r_t / (r_t + 1.0)
                q_l = r_l / (r_l + 1.0)
                q_r = r_r / (r_r + 1.0)

                warnings.warn("Assuming no ice")
                q_i = np.zeros_like(q_r)

                for i in range(Nx):
                    print(i, end=" ")
                    sys.stdout.flush()
                    s = i, slice(None), 0

                    T = UCLALES_NetCDFHandler.calc_temperature(
                        q_l=q_l[s], p=p[s], theta_l=theta_l[s]
                    )

                    if add_temperature_info:
                        T_var__handle[tn, i, :, 0] = T

                    if add_density_info:
                        # XXX: according to Axel Seifert rain is currently not considered
                        # as part of the "total water" mixing ratio
                        q_v = q_t - q_l - q_i
                        q_d = 1.0 - q_t - q_r

                        rho = UCLALES_NetCDFHandler.calc_density(
                            q_d=q_d[s],
                            q_v=q_v[s],
                            q_l=q_l[s],
                            q_r=q_r[s],
                            q_i=q_i[s],
                            T=T,
                            p=p[s],
                        )
                        rho_var__handle[tn, i, :, 0] = rho


def parseNumList(string):
    """
    https://stackoverflow.com/a/6512463/271776
    """
    import argparse
    import re

    m = re.match(r"(\d+)(?:-(\d+))?$", string)
    # ^ (or use .split('-'). anyway you like.)
    if not m:
        if len(string.split(",")) > 1:
            return [int(s) for s in string.split(",")]
        else:
            raise argparse.ArgumentTypeError(
                "'"
                + string
                + "' is not a range of number. Expected forms like '0-5', '1,2,5' or '2'."
            )
    else:
        start = m.group(1)
        end = m.group(2) or start
        return list(range(int(start, 10), int(end, 10) + 1))


if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser(__doc__)
    argparser.add_argument("k_indecies", type=parseNumList)

    args = argparser.parse_args()

    run(**dict(args._get_kwargs()))
