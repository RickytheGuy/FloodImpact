import os
import sys
# Add the project_root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from python.fld_impact_ricky import impact

############################################ INPUTS #########################################################

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

impact(croplands_path,pop_path,osm_file,flood_file,impact_file,cost_file)
