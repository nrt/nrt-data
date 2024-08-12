"""Script to prepare demo data of nrt-data package
20m sentinel2 data cube over a small area in Romania, spanning August 2015 to December 2020
Ran on August 12 2024 on BDAP
"""
import os
import datetime

import rasterio
import rioxarray
import xarray as xr
import numpy as np
from pystac_client import Client as psClient
from odc.stac import stac_load, configure_rio
from odc.geo.geobox import GeoBox
from dask.distributed import Client, LocalCluster


if __name__ == '__main__':
    con = rasterio.open('.virtualenvs/stac/lib/python3.10/site-packages/nrt/data/tree_cover_density_2018_romania.tif')
    gbox = GeoBox.from_rio(con)

    # Set up a local dask cluster and configure stac-odc
    cluster = LocalCluster(n_workers=5, threads_per_worker=2,
                           local_directory='/scratch/dutrilo')
    client = Client(cluster)
    configure_rio(client=client)


    # Open catalogue connection and query data
    catalog = psClient.open('https://jeodpp.jrc.ec.europa.eu/services/stac-api/')
    # Query all sentinel2 data with tile level cloud cover < 50%
    collection_id = 'EO.Copernicus.S2.L2A'
    bands = ['B03_20', 'B04_20', 'B8A', 'B11', 'SCL']
    resampling = {band: 'nearest' if band == 'SCL' else 'cubic' for band in bands}
    dt = [datetime.datetime(2015,7,31), datetime.datetime(2021,1,2)]
    query = catalog.search(collections=[collection_id],
                           bbox=gbox.geographic_extent.boundingbox.bbox,
                           datetime=dt,
                           query={"eo:cloud_cover": {"lt": 50}},
                           method='POST')


    # Visual check, one baseline 5.0 item in that item list
    items_no_offset = filter(lambda x: float(x.properties['sentinel:processing_baseline']) < 4,
                             query.items())

    # Load the required bands as a lazy (dask based) Dataset
    ds = stac_load(items_no_offset,
                   bands=bands,
                   groupby='solar_day',
                   chunks={'time': 1},
                   geobox=gbox,
                   resampling=resampling,
                   fail_on_error=False)


    ds = ds.apply(lambda x: x.astype(np.uint16) if x.name in
                  ['B03_20', 'B04_20', 'B8A', 'B11'] else x)
    ds['SCL'] = ds['SCL'].astype(np.uint8)

    ds = ds.compute()
    # Rename 20m visible bands
    ds = ds.rename({'B03_20': 'B03',
                    'B04_20': 'B04'})
    # Save as netcdf with right compression
    comp = dict(zlib=True, complevel=5, shuffle=True)
    encoding = {var: comp for var in ds.data_vars}
    ds.to_netcdf('/scratch/dutrilo/sentinel2_cube_subset_romania_20m.nc',
                 encoding=encoding)
