import rasterio
from rasterio import features
import rioxarray
import numpy as np

from nrt import data


ds = data.germany_zarr()

# Two manually selected summer cloud free observations
ds_t1 = ds.isel(time=39) # 2019-06-27
ds_t2 = ds.isel(time=150) # 2022-06-16

def calculate_ndvi(ds):
    return (ds['B08'] - ds['B04']) / (ds['B08'] + ds['B04'])

# Get the four variables as numpy arrays
# Assign NDVI for t1 and t2
ndvi_t1 = calculate_ndvi(ds_t1).values
ndvi_t2 = calculate_ndvi(ds_t2).values

# Retrieve SWIR bands
swir_t1 = ds_t1['B12'].values
swir_t2 = ds_t2['B12'].values

##################
## Rule based algorithm

# Define the two thresholds
ndvi_threshold = 0.82
swir_threshold = 0.09

# Initialize classification with zeros (Unclassified)
classification = np.zeros_like(ndvi_t1, dtype=np.uint8)

# Stable Forest (SF) - Constantly green and dark in swir region
sf_condition = (
    (ndvi_t1 > ndvi_threshold) & (ndvi_t2 > ndvi_threshold) &
    (swir_t1 <= swir_threshold) & (swir_t2 <= swir_threshold)
)
classification[sf_condition] = 2

# Forest Cover Loss (FCL) - Conversion from healthy forest to no forest
fcl_condition = (
    (ndvi_t1 > ndvi_threshold) & (ndvi_t2 <= ndvi_threshold) &
    (swir_t1 <= swir_threshold) & (swir_t2 > swir_threshold)
)
classification[fcl_condition] = 3

# Forest Canopy Dieback (FCD) - Loss of greeness but dark in SWIR
fcd_condition = (
    (ndvi_t1 > ndvi_threshold) & (ndvi_t2 <= ndvi_threshold) &
    (swir_t1 <= swir_threshold) & (swir_t2 <= swir_threshold)
)
classification[fcd_condition] = 4

# Salvage Logging (SVL) - From unhealthy forest (possibly standing dead trees)
# to no trees
svl_condition = (
    (ndvi_t1 <= ndvi_threshold) & (ndvi_t2 <= ndvi_threshold) &
    (swir_t1 <= swir_threshold) & (swir_t2 > swir_threshold)
)
classification[svl_condition] = 5

# Assign all remaining pixels to Persistent Non-Forest (PNF)
classification[classification == 0] = 1  # Assign PNF (class 1) to all unclassified pixels


# Clean out isolated pixel using sieve
classification_filterd = features.sieve(classification, size=3)

# Write to file
cmap = {
    0: (0, 0, 0),          # Unclassified: Black (This will no longer be used after the update)
    1: (255, 255, 0),      # Persistent Non-Forest: Yellow
    2: (0, 100, 0),        # Stable Forest: Dark Green
    3: (255, 0, 255),      # Forest Cover Loss: Magenta
    4: (128, 128, 128),    # Forest Canopy Dieback: Grey
    5: (165, 42, 42)       # Salvage Logging: Brown
}

# Trying to create a valid COG
with rasterio.open('/tmp/germany_stratification.tif', 'w',
                   crs=ds.rio.crs,
                   width=classification.shape[1],
                   height=classification.shape[0],
                   count=1,
                   transform=ds.rio.transform(),
                   driver='GTiff',
                   tiled=True,
                   blockxsize=256,
                   blockysize=256,
                   compress='deflate',
                   interleave='band',
                   dtype=np.uint8) as dst:
    dst.write(classification_filterd.astype(np.uint8), 1)
    dst.write_colormap(1, cmap)
