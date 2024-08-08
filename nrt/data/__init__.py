# Copyright (C) 2024 European Union (Joint Research Centre)
#
# Licensed under the EUPL, Version 1.2 or - as soon they will be approved by
# the European Commission - subsequent versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the Licence.
# You may obtain a copy of the Licence at:
#
#   https://joinup.ec.europa.eu/software/page/eupl
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Licence is distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Licence for the specific language governing permissions and
# limitations under the Licence.

import os
import json
import warning

import xarray as xr
import rasterio
import pooch

# Import for backward compatibility and deprecation warnings
from .simulate import make_ts as _make_ts
from .simulate import make_cube_parameters as _make_cube_parameters
from .simulate import make_cube as _make_cube


DATA_DIR = os.path.abspath(os.path.dirname(__file__))

GOODBOY = pooch.create(
    path=pooch.os_cache("nrt-validate"),
    base_url="https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/FOREST/NRT/NRT-DATA/VER1-0/",
    registry={
        "sentinel2_cube_subset_romania_10m.nc": None,
        "sentinel2_cube_subset_romania_20m.nc": None,
        "tree_cover_density_2018_romania.tif": None
    }
)


def _load(f, **kwargs):
    """Load a file using Pooch

    Args:
        f (str): File basename
        **kwargs: Keyword arguments for xarray or rasterio

    Returns:
        Dataset or array depending on file type
    """
    file_path = GOODBOY.fetch(f)
    if f.endswith('.nc'):
        return xr.open_dataset(file_path, **kwargs)
    elif f.endswith('.tif'):
        with rasterio.open(file_path) as src:
            return src.read(1)


def romania_10m(**kwargs):
    """Sentinel 2 datacube of a small forested area in Romania at 10 m resolution

    Examples:
        >>> from nrt import data

        >>> s2_cube = data.romania_10m()
        >>> # Compute NDVI
        >>> s2_cube['ndvi'] = (s2_cube.B8 - s2_cube.B4) / (s2_cube.B8 + s2_cube.B4)
        >>> # Filter clouds
        >>> s2_cube = s2_cube.where(s2_cube.SCL.isin([4,5,7]))
    """
    return _load('sentinel2_cube_subset_romania_10m.nc', **kwargs)


def romania_20m(**kwargs):
    """Sentinel 2 datacube of a small forested area in Romania at 20 m resolution

    Examples:
        >>> from nrt import data

        >>> s2_cube = data.romania_20m()
        >>> # Compute NDVI
        >>> s2_cube['ndvi'] = (s2_cube.B8A - s2_cube.B4) / (s2_cube.B8A + s2_cube.B4)
        >>> # Filter clouds
        >>> s2_cube = s2_cube.where(s2_cube.SCL.isin([4,5,7]))
    """
    return _load('sentinel2_cube_subset_romania_20m.nc', **kwargs)


def romania_forest_cover_percentage():
    """Subset of Copernicus HR layer tree cover percentage - 20 m - Romania
    """
    return _load('tree_cover_density_2018_romania.tif')


def mre_crit_table():
    """Contains a dictionary equivalent to strucchange's ``mreCritValTable``
    The key 'sig_level' is a list of the available pre-computed significance
    (1-alpha) values.

    The other keys contain nested dictionaries, where the keys are the
    available relative window sizes (0.25, 0.5, 1), the second keys are the
    available periods (2, 4, 6, 8, 10) and the third keys are the functional
    types ("max", "range").

    Example:
        >>> from nrt import data
        >>> crit_table = data.mre_crit_table()
        >>> win_size = 0.5
        >>> period = 10
        >>> functional = "max"
        >>> alpha=0.025
        >>> crit_values = crit_table.get(str(win_size))\
                                    .get(str(period))\
                                    .get(functional)
        >>> sig_level = crit_table.get('sig_levels')
        >>> crit_level = np.interp(1-alpha, sig_level, crit_values)
    """
    with open(os.path.join(DATA_DIR, "mreCritValTable.json")) as crit:
        crit_table = json.load(crit)
    return crit_table


def make_ts(*args, **kwargs):
    warnings.warn(
        "The function 'make_ts' has been moved to 'nrt.data.simulate'. "
        "Please update your imports to 'from nrt.data.simulate import make_ts'. "
        "This import path will be deprecated in future versions.",
        DeprecationWarning,
        stacklevel=2
    )
    return _make_ts(*args, **kwargs)


def make_cube_parameters(*args, **kwargs):
    warnings.warn(
        "The function 'make_cube_parameters' has been moved to 'nrt.data.simulate'. "
        "Please update your imports to 'from nrt.data.simulate import make_cube_parameters'. "
        "This import path will be deprecated in future versions.",
        DeprecationWarning,
        stacklevel=2
    )
    return _make_cube_parameters(*args, **kwargs)


def make_cube(*args, **kwargs):
    warnings.warn(
        "The function 'make_cube' has been moved to 'nrt.data.simulate'. "
        "Please update your imports to 'from nrt.data.simulate import make_cube'. "
        "This import path will be deprecated in future versions.",
        DeprecationWarning,
        stacklevel=2
    )
    return _make_cube(*args, **kwargs)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
