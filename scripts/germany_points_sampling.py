import rasterio
import numpy as np
from shapely.geometry import Point
import fiona
import random

from nrt import data

random.seed(36)
np.random.seed(36)

stratification, meta = data.germany_stratification(return_meta=True)

# Reclassify the 3 disturbance classes into one
stratification = np.where(np.isin(stratification, [4, 5]), 3, stratification)

# Function to calculate the center coordinates of a pixel
def pixel_to_coords(pixel_row, pixel_col, transform):
    x, y = rasterio.transform.xy(transform, pixel_row, pixel_col, offset='center')
    return x, y

# Identify the unique strata
strata = np.unique(stratification)

samples = []
# Perform stratified random sampling
n_samples = 100
for stratum in strata:
    # Get all the pixel indices for the current stratum
    indices = np.argwhere(stratification == stratum)
    # Randomly sample n_samples from these indices
    sampled_indices = indices[np.random.choice(indices.shape[0], n_samples,
                                               replace=False)]
    # Convert sampled indices to coordinates and store them
    for row, col in sampled_indices:
        x, y = pixel_to_coords(row, col, meta['transform'])
        samples.append({'geometry': Point(x,y),
                        'properties': {'stratum': stratum.item()}})

random.shuffle(samples)

for idx, feature in enumerate(samples):
    feature['properties']['fid'] = idx

schema = {'geometry': 'Point',
          'properties': {'stratum': 'int',
                         'fid': 'int'}}

with fiona.open('/tmp/germany_sample_points.fgb', 'w',
                crs=meta['crs'],
                schema=schema,
                driver='FlatGeobuf') as dst:
    dst.writerecords(samples)

print(np.unique(stratification, return_counts=True))
