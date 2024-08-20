import datetime

import xarray as xr
import numpy as np
from pystac_client import Client as psClient
from odc.stac import stac_load, configure_rio
from odc.geo.geobox import GeoBox
from dask.distributed import Client, LocalCluster
from shapely.geometry import Point
from shapely.ops import transform
from pyproj import CRS, Transformer
import zarr


if __name__ == '__main__':
    # Define centroid as a shapely point (in EPSG:4326)
    centroid = Point(7.45643, 51.01124)
    # Custom equidistant CRS centered on the centroid
    local_crs = CRS.from_epsg(3035)
    transformer = Transformer.from_crs(CRS.from_epsg(4326), local_crs, always_xy=True)
    # Transform centroid to custom CRS, expand to bbox and transform back to EPSG:4326
    centroid_proj = transform(transformer.transform, centroid)
    bbox_proj = centroid_proj.buffer(10000).bounds
    bbox_proj = tuple(round(num / 10) * 10 for num in bbox_proj)
    gbox = GeoBox.from_bbox(bbox=bbox_proj, crs=local_crs, resolution=10)
    print(gbox)
    # bbox = transformer.transform_bounds(*bbox_proj, direction='INVERSE')

    # Set up a local dask cluster and configure stac-odc to efficiently deal with cloud hosted data
    # while taking advantage of the dask cluster
    # Different types of configuration will be re
    cluster = LocalCluster(n_workers=5, threads_per_worker=2,
                           local_directory='/scratch/dutrilo')
    client = Client(cluster)
    configure_rio(client=client)


    # Open catalogue connection and query data
    catalog = psClient.open('https://jeodpp.jrc.ec.europa.eu/services/stac-api/')
    # Query all sentinel2 data that instersect the spatio-temporal extent and have a
    # scene level cloud cover < 30%
    collection_id = 'EO.Copernicus.S2.L2A'
    bands = ['B02', 'B03', 'B04', 'B08', 'B11', 'B12', 'SCL']
    resampling = {band: 'nearest' if band == 'SCL' else 'cubic' for band in bands}
    dt = [datetime.datetime(2018, 1, 1), datetime.datetime(2022,12,31)]
    query = catalog.search(collections=[collection_id],
                           bbox=gbox.geographic_extent.boundingbox.bbox,
                           datetime=dt,
                           query={"eo:cloud_cover": {"lt": 30}},
                           method='POST')

    items = list(query.items())
    items_no_offset = filter(lambda x: float(x.properties['sentinel:processing_baseline']) < 4,
                             items)
    items_offset = filter(lambda x: float(x.properties['sentinel:processing_baseline']) >= 4,
                          items)

    # Load the required bands as a lazy (dask based) Dataset
    ds_old = stac_load(items_no_offset,
                       bands=bands,
                       groupby='solar_day',
                       chunks={'time': 1, 'x': 1000, 'y': 1000},
                       geobox=gbox,
                       resampling=resampling,
                       fail_on_error=False)
    ds_old = ds_old.where(ds_old != 0)

    ds_new = stac_load(items_offset,
                       bands=bands,
                       groupby='solar_day',
                       chunks={'time': 1, 'x': 1000, 'y': 1000},
                       geobox=gbox,
                       resampling=resampling,
                       fail_on_error=False)
    ds_new = ds_new.where(ds_new != 0)

    ds_new = ds_new.apply(lambda x: x - 1000 if x.name in
                          ['B02', 'B03', 'B04', 'B08', 'B11', 'B12'] else x)

    # Combine, giving priority to new baselines and put back zeros instead of nans
    ds = ds_new.combine_first(ds_old).fillna(0)
    ds = ds.apply(lambda x: x.astype(np.int16) if x.name in
                  ['B02', 'B03', 'B04', 'B08', 'B11', 'B12'] else x)
    ds['SCL'] = ds['SCL'].astype(np.uint8)

    # Trigger computation to bring the data in memory (depending on network speed, this step may
    # take up to a few minutes)
    ds = ds.compute()
    # Dump to netcdf to be safe in case zarr writing fails
    ds.to_netcdf('/scratch/dutrilo/germany.nc')

    """Anything after that failed on the fly and was done manually with the following
    code

    >>> import xarray as xr
    >>> import zarr
    >>> ds = xr.open_dataset('/scratch/dutrilo/germany.nc')
    >>> ds = ds.chunk({'time': 10, 'y': 100, 'x': 100})
    >>> compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)
    >>> encoding = {var: {'compressor': compressor,
    ...                       'scale_factor': 0.0001,
    ...                       '_FillValue': 0,
    ...                      'dtype': 'int16'} for var in ['B02', 'B03', 'B04', 'B08', 'B11', 'B12']}
    >>> encoding.update(SCL={'compressor': compressor,
    ...                      'dtype': 'uint8'})
    # Convert zeros back to nan
    >>> ds['B02'] = ds['B02'].where(ds['B02'] != 0)
    >>> ds['B03'] = ds['B03'].where(ds['B03'] != 0)
    >>> ds['B04'] = ds['B04'].where(ds['B04'] != 0)
    >>> ds['B08'] = ds['B08'].where(ds['B08'] != 0)
    >>> ds['B11'] = ds['B11'].where(ds['B11'] != 0)
    >>> ds['B12'] = ds['B12'].where(ds['B12'] != 0)
    # apply scaling
    >>> ds['B02'] = ds['B02']/10000
    >>> ds['B03'] = ds['B03']/10000
    >>> ds['B04'] = ds['B04']/10000
    >>> ds['B08'] = ds['B08']/10000
    >>> ds['B11'] = ds['B11']/10000
    >>> ds['B12'] = ds['B12']/10000
    >>> ds.to_zarr('/scratch/dutrilo/germany.zarr',
    ...            encoding=encoding, consolidated=True)
    """
