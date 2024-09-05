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

__path__ = __import__('pkgutil').extend_path(__path__, __name__)

import os
import json
import warnings
import sqlite3

import pandas as pd
import xarray as xr
import rasterio
import fiona
import pooch

# Import for backward compatibility and deprecation warnings
from .simulate import make_ts as _make_ts
from .simulate import make_cube_parameters as _make_cube_parameters
from .simulate import make_cube as _make_cube


DATA_DIR = os.path.abspath(os.path.dirname(__file__))

GOODBOY = pooch.create(
    path=pooch.os_cache("nrt-data"),
    base_url="https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/FOREST/NRT/NRT-DATA/VER1-0/",
    registry={
        "sentinel2_cube_subset_romania_10m.nc": "sha256:f88716ff11353fa46990c59c57f31b4c9a8dfd4a950a845e073e41f6beb0ac07",
        "sentinel2_cube_subset_romania_20m.nc": "sha256:5e6858fc0e31555e8de44bac57b989bb9a3c10117f4fddead943eb45b8c1be17",
        "tree_cover_density_2018_romania.tif": "sha256:0d6a445b112de8ba44729df26c063650198e6214bc9d9349c3b4c86ee43894bb",
        "germany_stratification.tif": "sha256:149d0c36b9f5d933ca12c4ea976f866e74c205ab708ac3bc4dd7062c74c4968c",
        "germany_sample_points.fgb": "sha256:068cbda19fcfbd2dd1cf9a1d18f032685c971d6d22cb7bef1415109030753ace",
        "germany_temporal_segments.sqlite": "sha256:248fc9ffd020764b4a5a1ece40976dc5f0622c68de6e9ae3005ad192d0233a14"
    }
)


def _load(f, return_meta=False, **kwargs):
    """Load a file using Pooch

    Args:
        f (str): File basename
        return_meta (bool): If True, return metadata along with the
            array (for .tif and .fgb files). Default is False.
        **kwargs: Keyword arguments for xarray when loading nc files

    Returns:
        Dataset, array, dictionary, sqlite3 connection, or tuple depending on
        the file type and options.
    """
    file_path = GOODBOY.fetch(f)
    if f.endswith('.nc'):
        return xr.open_dataset(file_path, **kwargs)
    elif f.endswith('.tif'):
        with rasterio.open(file_path) as src:
            array = src.read(1)
            if return_meta:
                meta = src.meta
                return array, meta
            return array
    elif f.endswith('.fgb'):
        with fiona.open(file_path) as src:
            data = list(src)  # Read all features into a list
            if return_meta:
                meta = src.meta
                return data, meta
            return data
    elif f.endswith('.sqlite'):
        return sqlite3.connect(file_path)


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


def germany_zarr(**kwargs):
    """Sentinel 2 datacube of a forest area affected by Bark Beetle in Germany

    The data covers an area of 400 km\ :sup:`2` located East of the city Cologne
    in the North Rhine-Westphalia state of Germany. It spans a five year period
    from 2018 to 2022 and includes the sentinel bands B02, B03, B04, B08, B11,
    B12 and SCL. Native 10m resolution is preserved for the visible and near-infrared
    bands while B11, B12 and SCL were resampled to 10m from their original 20m resolution.
    The data is organized as an online zarr store on the JRC's Open Data Repository,
    so that running the ``germany_zarr()`` function only creates a symbolic representation
    of the dataset (dask based). Lazy chunks can be eagerly loaded by invoking the
    ``.compute()`` method. Beware that loading the entire dataset may take a fairly
    long time due to its size (4.8 GB).
    Note that although data is stored as int16, the scaling factor is automatically
    applied to convert spectral channels to unscaled surface reflectance values
    and the corresponding data variables are casted to float64. No data pixels are
    also automatically converted to ``np.nan``

    Args:
        **kwargs: Additional keyword arguments passed to ``xarray.open_zarr()``

    Examples:
        >>> import sys
        >>> from nrt import data
        >>> ds = data.germany_zarr()
        >>> print(ds)
        <xarray.Dataset>
        Dimensions:      (time: 172, y: 2000, x: 2000)
        Coordinates:
            spatial_ref  int32 ...
          * time         (time) datetime64[ns] 2018-02-14T10:31:31.026000 ... 2022-12...
          * x            (x) float64 4.133e+06 4.133e+06 ... 4.153e+06 4.153e+06
          * y            (y) float64 3.113e+06 3.113e+06 ... 3.093e+06 3.093e+06
        Data variables:
            B02          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            B03          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            B04          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            B08          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            B11          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            B12          (time, y, x) float32 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
            SCL          (time, y, x) uint8 dask.array<chunksize=(10, 100, 100), meta=np.ndarray>
        >>> # Load a subset in memory
        >>> print(sys.getsizeof(ds.B02.data))
        80
        >>> ds_sub = ds.isel(x=slice(100,110), y=slice(200,210), time=slice(10,20)).compute()
        >>> print(sys.getsizeof(ds_sub.B02.data))
        2144

    Returns:
        xarray.Dataset: A dask based Dataset of dimension ``(time: 172, y: 2000, x: 2000)``
        with 7 data variables.
    """
    ds = xr.open_zarr('https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/FOREST/NRT/NRT-DATA/VER1-0/germany.zarr',
                      **kwargs)
    return ds


def germany_stratification(return_meta=False):
    """Simple rule based classification of forest dynamics for germany_zarr

    Raster layer in the EPSG:3035 CRS, spatially aligned with the
    ``germany_zarr`` data cube. The classification is based on a simple
    rule-based approach applied to a pair of manually selected cloud-free time
    steps (2019-06-27 and 2022-06-16). NDVI and the second SWIR band are used to
    distinguish various tree cover dynamics, based on the assumption that healthy
    forests are generally green in summer (indicated by high NDVI values) and dark
    in the SWIR region. "Unhealthy" forests, such as standing dead trees following
    a bark beetle attack, may still be dark in the SWIR region but with lower
    greenness. Non-forested land lacks these combined characteristics.

    Note that this approach is relatively simple and, as a result, produces an
    imperfect classification. Although there may be some confusion between land 
    cover elements that are difficult to discriminate solely based on NDVI and
    SWIR values, the layer is well-suited for purposes such as stratification in a
    stratified sampling design.

    Classes are encoded as follows::

        1: Non-forested land
        2: Stable Forest (Constantly green and dark in the SWIR region)
        3: Forest Cover Loss (Conversion from healthy forest to non-forested land)
        4: Forest Canopy Dieback (Loss of greenness but still dark in SWIR)
        5: Salvage Logging (From 'unhealthy' forest to non-forested land)

    Returns:
        numpy.ndarray or (numpy.ndarray, dict): 2D array with encoded 2019-2022
        tree cover dynamics or (data, metadata) if ``return_meta`` is True.
    """
    return _load('germany_stratification.tif', return_meta=return_meta)


def germany_sample_points(return_meta=False):
    """Feature collection of 300 sample points from the Germany test area.

    These points represent pixel centers sampled using a reclassified version
    of the ``germany_stratification`` layer. In this reclassification, all
    disturbance strata were combined into a single stratum, resulting in
    stratified random sampling. An equal allocation method was employed,
    with 100 samples per stratum.

    The feature collection includes a ``stratum`` property with values 1, 2,
    or 3, representing different strata:

        - 1: Non-forested stratum (2,333,767 pixels)
        - 2: Stable forest stratum (972,184 pixels)
        - 3: Disturbed forest stratum (694,049 pixels)

    Args:
        return_meta (bool, optional): If True, returns both the feature collection
            and associated metadata. Defaults to False.

    Examples:
        >>> from nrt import data
        >>> fc = data.germany_sample_points()
        >>> set([feat['properties']['stratum'] for feat in fc])
        {1,2,3}
        >>> len(fc)
        300

    Returns:
        list or tuple: If `return_meta` is False, returns a list containing the 
        feature collection of sample points. If `return_meta` is True, returns
        a tuple of (feature collection, metadata).
    """
    return _load('germany_sample_points.fgb', return_meta=return_meta)


def germany_temporal_segments():
    """Visually interpreted temporal segments for 300 sample locations in Germany.

    This function loads temporal segmentation data, which has been visually
    interpreted using the ``SegmentLabellingInterface`` of the ``nrt-data`` package.
    The data corresponds to the sample locations from ``nrt.data.germany_sample_points()``
    and can be joined with it using the ``fid`` or ``feature_id`` keys.

    Each segment is labeled with one of three possible categories:

        - Non-treed
        - Stable tree cover
        - Dieback

    A common disturbance trajectory in this region, which has been heavily
    affected by bark beetle activity, follows the pattern "Stable tree cover",
    "Dieback", and then "Non-treed." For some sample locations, no label could be
    confidently assigned, and these are represented with a single segment labeled `None`.

    Additional information about the dataset:

        - Temporal segmentation is valid for the period between 2019-01-01 and 2021-12-31.
        - Each segment has a ``begin`` and ``end`` time represented as days since epoch.
        - The segmentation data may contain errors for ambiguous samples, particularly
          near edges, in mixed or sparse forests, or for shrub-like vegetation easily
          mistaken for trees.
        - Temporal precision may vary, especially in cases where gradual processes like
          canopy dieback are difficult to date accurately.

    Examples:
        >>> from nrt import data
        >>> data.germany_temporal_segments()
              id  begin    end              label  feature_id
        0      1  17916  18981          Non-treed           0
        1      2  17916  18981          Non-treed           1
        2      3  17916  18981          Non-treed           2
        3      4  17916  18981          Non-treed           3
        4      5  17916  18981  Stable tree cover           4
        ..   ...    ...    ...                ...         ...
        413  414  17916  18981          Non-treed         295
        414  415  17916  18981  Stable tree cover         296
        415  416  17916  18981          Non-treed         297
        416  417  17916  18981          Non-treed         298
        417  418  17916  18981  Stable tree cover         299

        [418 rows x 5 columns]

    Returns:
        pandas.DataFrame: A data-frame containing 418 rows and 5 columns, with each
        row representing a temporal segment for a sample location.
    """
    con = _load('germany_temporal_segments.sqlite')
    df = pd.read_sql('SELECT * FROM segments;', con=con)
    con.close()
    return df


def romania_forest_cover_percentage(return_meta=False):
    """Subset of Copernicus HR layer tree cover percentage - 20 m - Romania

    Returns:
        numpy.ndarray or (numpy.ndarray, dict): The data or (data, metadata)
        if ``return_meta`` is True.
    """
    return _load('tree_cover_density_2018_romania.tif', return_meta=return_meta)


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
