"""
Utility for loading data from UCALES 3D datafiles with routines for calculating
derived fields
"""
import warnings

import netCDF4
import numpy as np
import scipy.optimize


def load_data_and_get_grid(fname, var_name, timestep=0):
    fh = UCLALES_NetCDFHandler(fname=fname)

    return fh.get_data_and_grid(var_name=var_name, timestep=timestep)


class UCLALES_NetCDFHandler:
    def __init__(self, fname):
        self.fname = fname
        self.fhandle = netCDF4.Dataset(self.fname)

    def get_data_and_grid(self, var_name, timestep):
        derived_field = False

        if var_name in ["rho", "T", "q_v", "q_l"]:
            derived_field = True
            tn_ = timestep
            theta_l = self.fhandle.variables["t"][tn_, :]
            p = self.fhandle.variables["p"][tn_, :]
            assert p.shape == theta_l.shape
            r_t = self.fhandle.variables["q"][tn_, :]
            r_l = self.fhandle.variables["l"][tn_, :]
            r_r = self.fhandle.variables["r"][tn_, :]
            if "i" in self.fhandle.variables:
                r_i = self.fhandle.variables["i"][tn_, :]
            else:
                r_i = np.zeros_like(r_r)

            if 1000 * r_t.max() < 1.0:
                warnings.warn(
                    "The `r_t` may actually be in g/kg, but we're assuming "
                    "that the bug in UCLALES where the mixing ratios are mislabelled"
                )
            q_t = r_t / (r_t + 1.0)
            q_l = r_l / (r_l + 1.0)
            q_r = r_r / (r_r + 1.0)
            q_i = r_i / (r_i + 1.0)
            # XXX: according to Axel Seifert rain is currently not considered
            # as part of the "total water" mixing ratio
            q_v = q_t - q_l - q_i
            q_d = 1.0 - q_t - q_r

            T = self.calc_temperature(q_l=q_l, theta_l=theta_l, p=p)
            if var_name == "rho":
                q_d = 1.0 - q_t
                data = self.calc_density(
                    q_d=q_d, q_v=q_v, T=T, p=p, q_l=q_l, q_i=q_i, q_r=q_r
                )
            elif var_name == "T":
                data = T
            elif var_name == "q_v":
                data = q_v
            elif var_name == "q_l":
                data = q_l
            else:
                raise NotImplementedError()
        else:
            data = self.fhandle.variables[var_name][timestep]

        data = np.swapaxes(data, 0, 1)
        # XXX: quick hack, masked arrays let us set attributes (like `units`)
        data = np.ma.masked_array(data)

        x_ = self.fhandle.variables["xt"][:]
        y_ = self.fhandle.variables["yt"][:]
        z_ = self.fhandle.variables["zt"][:]

        if not derived_field:
            data.units = self.fhandle.variables[var_name].units
            data.long_name = self.fhandle.variables[var_name].longname
        elif var_name == "rho":
            data.units = "kg/m3"
            data.long_name = "mixture density"
        elif var_name == "rho_g":
            data.units = "kg/m3"
            data.long_name = "gas density"
        elif var_name == "T":
            data.units = "K"
            data.long_name = "absolute temperature"
        elif var_name == "q_v":
            data.units = "kg/kg"
            data.long_name = "water vapour specific concentration"
        elif var_name == "q_l":
            data.units = "kg/kg"
            data.long_name = "cloud water specific concentration"
        else:
            raise NotImplementedError()

        data.time = "0"  # TODO: fix this
        data.time_units = "seconds"  # TODO: fix this

        grid = np.meshgrid(x_, y_, z_, indexing="ij")

        return (data, grid)

    @np.vectorize
    def calc_density(q_d, q_v, q_l, q_r, q_i, T, p):
        # constants from UCLALES
        R_d = 287.04  # [J/kg/K]
        R_v = 461.5  # [J/kg/K]
        rho_l = 1000.0  # [kg/m^3]
        rho_i = 900.0  # [kg/m^3]

        rho_inv = (q_d * R_d + q_v * R_v) * T / p + (q_l + q_r) / rho_l + q_i / rho_i

        return 1.0 / rho_inv

    @np.vectorize
    def __get_gas_density(q_d, q_v, T, p):
        # constants from UCLALES
        R_d = 287.04  # [J/kg/K]
        R_v = 461.5  # [J/kg/K]
        # rho_l = 1000.0  # [kg/m^3]

        # XXX: in the equation of state used internally in UCLALES the
        # condensate phases are not included, i.e. the gas density is relative
        # to the volume that the gas takes up
        rho_gas_inv = (q_d * R_d + q_v * R_v) * T / p

        return 1.0 / rho_gas_inv

    @np.vectorize
    def calc_temperature(q_l, p, theta_l):
        # constants from UCLALES
        cp_d = 1.004 * 1.0e3  # [J/kg/K]
        R_d = 287.04  # [J/kg/K]
        L_v = 2.5 * 1.0e6  # [J/kg]
        p_theta = 1.0e5

        # XXX: this is *not* the *actual* liquid potential temperature (as
        # given in B. Steven's notes on moist thermodynamics), but instead
        # reflects the form used in UCLALES where in place of the mixture
        # heat-capacity the dry-air heat capacity is used
        def temp_func(T):
            return theta_l - T * (p_theta / p) ** (R_d / cp_d) * np.exp(
                -L_v * q_l / (cp_d * T)
            )

        if np.all(q_l == 0.0):
            # no need for root finding
            return theta_l / ((p_theta / p) ** (R_d / cp_d))

        # XXX: brentq solver requires bounds, I don't expect we'll get below -100C
        T_min = -100.0 + 273.0
        T_max = 50.0 + 273.0
        T = scipy.optimize.brentq(f=temp_func, a=T_min, b=T_max)

        # check that we're within 1.0e-4
        assert np.all(np.abs(temp_func(T)) < 1.0e-4)

        return T

    @property
    def variables(self):
        return list(self.fhandle.variables.keys()) + [
            "rho",
            "T",
            "q_v",
            "q_l",
        ]
