"""
Simple utility for check which cross sections (from 3D datasets) are available
"""
import shutil
import os
import glob
import netCDF4


FN_BASE = "rico_gcss.out.xy.k{k}.{var}.nc"

k_levels = set()
variables = set()

for filename in glob.glob(FN_BASE.format(var="*", k="*")):
    fname_parts = filename.split('.')
    k_levels.add(int(fname_parts[-3][1:]))
    variables.add(fname_parts[-2])

print("k:   " + " ".join(variables))
print "--------------------------------"

for k in k_levels:
    print "{:02d}: ".format(k),
    for var in variables:
        if len(var) > 2:
            print " "*(len(var)-2),

        try:
            fh = netCDF4.Dataset(FN_BASE.format(var=var, k=k))
            print "*",
        except:
            print "-",

    print
