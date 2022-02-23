# Changelog


## [Unreleased](https://github.com/leifdenby/uclales-utils/tree/HEAD)

[Full Changelog](https://github.com/convml/convml_tt/compare/...v0.1.1)

*new features*

- Add support cdo version `>= 1.7.0`. In cdo `1.7.0` the `gather` command was
  renamed as `collgrid`, the extraction routines now check for which command is
  available. Continuous integration now checks multiple cdo version and without
  cdo for extraction (using only xarray)
  [\#7](https://github.com/leifdenby/uclales-utils/pull/7)

*bugfixes*

- Fix to ensure that extracting using y-strips works with cdo.
  [\#7](https://github.com/leifdenby/uclales-utils/pull/7)

- Fix numerous bugs, specifically: 1) ensure `use_cdo` is correctly passed to
  child-tasks. 2) 3D source-files don't have a timestep (`tn`) in the filename,
  3) fix concatenation when all extraction is done with xarray, 4) ensure all
  possible methods of extraction (`blocks`, `x_strips` and `y_strips`) are all
  tested [\#8](https://github.com/leifdenby/uclales-utils/pull/8)


## [v0.1.1](https://github.com/leifdenby/uclales-utils/tree/v0.1.1) (2022-01-31)

[Full Changelog](https://github.com/convml/convml_tt/compare/v0.1.1...v0.1.0)

*maintance*

- Update instructions in the README [\#6](https://github.com/leifdenby/uclales-utils/pull/6)


## [v0.1.0](https://github.com/leifdenby/uclales-utils/tree/v0.1.0) (2022-01-31)

[Full Changelog](https://github.com/convml/convml_tt/compare/...v0.1.0)

First tagged version. Main functionality is extraction of individual 3D and 2D
fields from data-files created per-core for parallel UCLALES runs
[\#2](https://github.com/leifdenby/uclales-utils/pull/2),
[\#4](https://github.com/leifdenby/uclales-utils/pull/4).
