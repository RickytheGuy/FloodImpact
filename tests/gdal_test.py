import os
import sys
# Add the project_root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from python.fld_impact_ricky import impact
from osgeo import gdal

# Load up croplands raster dataset path 
croplands_path = 'tests/croplands.tif'

# Load up population raster dataset path 
pop_path = 'tests/worldpop.tif'

# Load up the OpenSteetMap point shapefile dataset path 
osm_file = 'tests/Chazuta_osm_points.gpkg'

# Upload the flood map  path (either shortened as so or absolute). 
flood_file = 'tests/chazuta_floodmap.tif'

# This is the output flood impact map
impact_file = 'tests/flood_impact.tif'

# Optional cost map, can let it be None if not desired
cost_file = 'tests/flood_cost.tif'

impact_validation = 'tests/impact_validation.tif'
impact_cost = 'tests/cost_validation.tif'

impact(croplands_path,pop_path,osm_file,flood_file,impact_file,cost_file)

ds = gdal.Open(impact_file, gdal.GA_ReadOnly)
impact_arr = ds.GetRasterBand(1).ReadAsArray(0)
ds = None
ds = gdal.Open(impact_validation, gdal.GA_ReadOnly)
impact_val_arr = ds.GetRasterBand(1).ReadAsArray(0)
ds = None

ds = gdal.Open(cost_file, gdal.GA_ReadOnly)
cost_arr = ds.GetRasterBand(1).ReadAsArray(0)
ds = None
ds = gdal.Open(impact_cost, gdal.GA_ReadOnly)
cost_val_arr = ds.GetRasterBand(1).ReadAsArray(0)
ds = None
try:
    assert (impact_arr.shape == impact_val_arr.shape), "Impact array and validation shapes do not match"
    assert (cost_arr.shape == cost_val_arr.shape), "Cost array and validation shapes do not match"

    assert (impact_arr == impact_val_arr).all(), "Impact array and validation do not match"
    assert (cost_arr == cost_val_arr).all(), "Cost array and validation do not match"

    print('ALL TESTS PASS')
except Exception as e:
    print(e)

