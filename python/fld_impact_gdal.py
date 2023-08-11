#####################################################################################################
# Based off Evan's work, optimized by Ricky Rosas
#####################################################################################################
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple

import geopandas as gpd
import numpy as np
from osgeo import gdal

def impact(croplands_path: str,
           pop_path: str,
           osm_file: str,
           flood_file: str,
           impact_file: str,
           cost_file: str = None):
    """
    Main function for creating impact maps. 

    Parameters
    ----------
    croplands_path : str
        Path to a file of Global Food Security-Support Analysis Data at 30 m -> https://www.usgs.gov/centers/western-geographic-science-center/science/global-food-security-support-analysis-data-30-m
        This dataset contains a number of values, but the value that corresponds to "croplands" is 2: this is the value measured. 
    pop_path : str
        Path to a file of population counts -> https://hub.worldpop.org/geodata/listing?id=78
        This dataset contains the estimated population of a region of a given cell
    osm_file: str
        Path to a file of Open Street Map points (downloaded here, converted to shp or geopackage) -> https://download.geofabrik.de/
        This dataset contains a field called 'amenity' or 'other_tags'
    flood_file : str
        Path to rasterized flood inundation map
    impact_file : str,
        Path to output flood impact geotiff
    cost_file : str, optional
        If specified, path to output cost map
    """
    floodmap_dataset = gdal.Open(flood_file, gdal.GA_ReadOnly)
    floodmap_array = floodmap_dataset.ReadAsArray()
    flood_geo = floodmap_dataset.GetGeoTransform()
    pixel_area = abs(flood_geo[5] * flood_geo[1]) * 12348543360 / 10000 # This number comes from conversion of degrees to meters (111128*111120). Divide by 10000 for hectares

    # Allocate the impact array
    impact_array = np.zeros((floodmap_array.shape[0], floodmap_array.shape[1], 4), dtype=np.uint8)

    with ThreadPoolExecutor(max_workers=3) as executor: 
        # Run the functions concurrently in parallel
        future_pop = executor.submit(get_population, floodmap_dataset, floodmap_array, pop_path)
        future_crop = executor.submit(get_crop, floodmap_dataset, floodmap_array, croplands_path, pixel_area)
        future_amenities = executor.submit(get_osm_table, osm_file, floodmap_dataset, floodmap_array)

        # Get the results from the futures
        pop_count, impact_array[:,:,0], max_pop = future_pop.result()
        crop_hectares, impact_array[:,:,1] = future_crop.result()
        amenities_df, location_df = future_amenities.result()

    indices = location_df[['x','y']].values.T
    max_osm_cost = location_df['cost'].max()
    impact_array[indices[1],indices[0],2] = location_df['cost'].values * 255 / max_osm_cost

    # The alpha channel should be set to opaque (255) iff any of the other three channels have any value above 0
    impact_array[(np.count_nonzero(impact_array, axis=2) != 0 ), 3] = 255

    # assume average size of home is 1600 sqft, people per home is 4.9, so 326.5 sqft per person (8.25 is cost per sqft)
    pop_impact_cost = 326.5 * 8.25

    # assume that croplands are $2648 per acre -> $1071.6 per hectare * reduction in price due to flood 
    # -> https://le.uwpress.org/content/early/2021/08/18/wple.97.1.061019-0075R1
    cost_per_hectare = 1071.6 * (1 - 0.243)
    crop_impact_cost = crop_hectares * cost_per_hectare

    # save impact map
    output_ds = create_disk_dataset(floodmap_dataset, impact_file, floodmap_array.shape[1], floodmap_array.shape[0], 4, gdal.GDT_Byte)

    for i in range(4):
        output_ds.GetRasterBand(i+1).WriteArray(impact_array[:,:,i])
 
    building_cost = location_df['cost'].sum()
    residential_cost = pop_count*pop_impact_cost

    # save cost map
    if cost_file is not None:
        cost_ds = create_disk_dataset(floodmap_dataset, cost_file, floodmap_array.shape[1], floodmap_array.shape[0], 1, gdal.GDT_Float32)
        cost_ds.GetRasterBand(1).WriteArray(((impact_array[:,:,0] * max_pop * pop_impact_cost) + (impact_array[:,:,1] * cost_per_hectare) + (impact_array[:,:,2] * max_osm_cost)) / 255)
        cost_ds.GetRasterBand(1).SetNoDataValue(0)

    print(f"The amenity group table is:\n{amenities_df}")

    print(f"\nArea of agriculture in the flood extent is {crop_hectares} hectares")

    print(f"\nEstimated population in the flood extent is {pop_count}")

    print(f"\nThe estimated infrastructure (excluding residences) repair cost is {format_money(building_cost)}")

    print(f"\nThe estimated residential repair cost is {format_money(residential_cost)}")

    print(f"\nThe estimated loss of farmlands is {format_money(crop_impact_cost)}")

    print(f"\nTotal flood impact losses: {format_money(building_cost+residential_cost+crop_impact_cost)}")
    
def get_population(floodmap_dataset: gdal.Dataset, floodmap_array: np.ndarray, pop_path: str) -> Tuple[int, np.ndarray, np.float64]:
        """
        
        """
        population_dataset = gdal.Open(pop_path, gdal.GA_ReadOnly)

        pop_reprojected = create_mem_dataset(floodmap_dataset, gdal.GDT_Float32)

        #gdal.ReprojectImage(population_dataset, pop_reprojected, None, None, gdal.GRA_NearestNeighbour)
        gdal.Warp(pop_reprojected, population_dataset, options=gdal.WarpOptions(resampleAlg='near'))

        # Calculate the sum of population in the intersection
        pop_array = pop_reprojected.GetRasterBand(1).ReadAsArray()
        
        # Set any values outside of the flood extent to 0
        pop_array[np.logical_not(np.isin(floodmap_array , 1))] = 0
        pop_count = int(pop_array.sum())

        # Min-max normalization to scale values between 0 and 255
        return pop_count, normalize_array(pop_array), np.max(pop_array)

def get_crop(flood_dataset, flood_array, crop_raster, pixel_area) -> Tuple[int, np.ndarray]:
    # Open the crop raster with gdal
    crop_dataset = gdal.Open(crop_raster)
    crop_reprojected = create_mem_dataset(flood_dataset, gdal.GDT_Byte)

    #gdal.ReprojectImage(crop_dataset, crop_reprojected, None, None, gdal.GRA_NearestNeighbour)
    gdal.Warp(crop_reprojected, crop_dataset, options=gdal.WarpOptions(resampleAlg='near'))

    # Calculate the intersection between the two rasters
    crop_array = crop_reprojected.GetRasterBand(1).ReadAsArray()
    intersection_array = np.where((flood_array == 1) & (crop_array == 2), 1, 0)

    # Calculate the area in hectares of the intersection
    return round(np.count_nonzero(intersection_array) * pixel_area, 2), intersection_array * 255 # Convert to hectares, count_nonzero may be faster for larger datasets

def get_osm_table(osm_file: str, floodmap_dataset: gdal.Dataset, floodmap_array: np.ndarray) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    osm_gdf = gpd.read_file(osm_file)
    flood_geo = floodmap_dataset.GetGeoTransform()

    # Create x and y coordinates that are in regards to the floodmap dataset
    osm_gdf['x'] = ((osm_gdf.geometry.x - flood_geo[0]) / flood_geo[1]).round().astype(int)
    osm_gdf['y'] = ((osm_gdf.geometry.y - flood_geo[3]) / flood_geo[5]).round().astype(int)

    # Select the osm points that are within the raster
    osm_gdf = osm_gdf[(osm_gdf.x >= 0) & (osm_gdf.x < floodmap_array.shape[1]) & (osm_gdf.y >= 0) & (osm_gdf.y < floodmap_array.shape[0])]

    if 'amenity' in osm_gdf:
        osm_gdf['amenity'] = osm_gdf['amenity'].apply(amen_group)
    elif 'other_tags' in osm_gdf:
        osm_gdf['amenity'] = osm_gdf['other_tags'].apply(old_amen_group)
    else:
        osm_gdf['amenity'] = osm_gdf['fclass'].apply(amen_group)

    # We assume average restoration cost per sq. ft as $8.25 => https://www.fixr.com/costs/water-damage-restoration
    sq_ft_cost = 8.25
    amenity_cost = {
        'food': 3500.0 * sq_ft_cost, # https://www.statista.com/statistics/587130/average-floor-space-qrs-us/
        'education': 173727.0 * sq_ft_cost, # median High school size
        'transportation': 360.0 * sq_ft_cost, #
        'financial': 3400.0 * sq_ft_cost, # https://bancography.com/wp-content/uploads/2022/03/Bancology0819.pdf
        'healthcare': 300000.0 *  sq_ft_cost, # https://www.fixr.com/costs/build-hospital
        'entertainment': 40000.0 * sq_ft_cost, # https://forum-theatre.com/how-many-square-feet-is-a-movie-theatre/
        'others': 3000.0 * sq_ft_cost, # https://gymdesk.com/blog/how-much-does-it-cost-to-open-a-gym/,
        'public_service': 11000.0 * sq_ft_cost, # https://www.rsmeans.com/model-pages/police-station
        'facilities': 10.0 * sq_ft_cost, # No idea
        'waste_management': 4000.0 * sq_ft_cost, # https://wasteadvantagemag.com/determining-transfer-station-size-and-capacity/
        None: 0.0
    }

    osm_gdf['cost'] = osm_gdf['amenity'].apply(lambda x: amenity_cost[x])

    return (osm_gdf
        .loc[floodmap_array[osm_gdf['y'], osm_gdf['x']] == 1, 'amenity']
        .value_counts()
        .to_frame(name='number_of')
        .T
    ), osm_gdf.loc[floodmap_array[osm_gdf['y'], osm_gdf['x']] == 1, ['amenity','x','y','cost']]

def amen_group(string: str) -> str or None:
    """
    This will organize the different amenities into a group
    """
    amenity_groups = {
        'food': ['bar', 'biergarten', 'cafe', 'drinking_water', 'fast_food',
                 'food_court', 'ice_cream', 'pub', 'restaurant'],
        'education': ['college', 'driving_school', 'kindergarten', 'language_school',
                      'library', 'toy_library', 'music_school', 'school', 'university'],
        'transportation': ['bicycle_parking', 'bicycle_repair_station', 'bicycle_rental',
                           'boat_rental', 'boat_sharing', 'bus_station', 'car_rental',
                           'car_sharing', 'car_wash', 'vehicle_inspection', 'charging_station',
                           'ferry_terminal', 'fuel', 'grit_bin', 'motorcycle_parking',
                           'parking', 'parking_entrance', 'parking_space', 'taxi', 'kick-scooter_rental'],
        'financial': ['atm', 'bank', 'bureau_de_change'],
        'healthcare': ['baby_hatch', 'clinic', 'dentist', 'doctors', 'hospital', 'nursing_home',
                       'pharmacy', 'social_facility', 'veterinary'],
        'entertainment': ['arts_centre', 'brothel', 'casino', 'cinema', 'community_centre',
                          'conference_centre', 'events_venue', 'fountain', 'gambling',
                          'love_hotel', 'nightclub', 'planetarium', 'public_bookcase',
                          'social_centre', 'stripclub', 'studio', 'swingerclub', 'theatre'],
        'others': ['animal_boarding', 'animal_breeding', 'animal_shelter', 'baking_oven',
                   'childcare', 'clock', 'crematorium', 'dive_centre',
                   'funeral_hall', 'grave_yard', 'gym', 'hunting_stand',
                   'internet_cafe', 'kitchen', 'kneipp_water_cure', 'lounger', 'marketplace',
                   'monastery', 'photo_booth', 'place_of_mourning', 'place_of_worship', 'public_bath',
                   'public_building', 'refugee_site', 'vending_machine', 'user defined'],
        'public_service': ['courthouse', 'embassy', 'fire_station', 'police', 'post_box', 'post_depot',
                           'post_office', 'prison', 'ranger_station', 'townhall'],
        'facilities': ['bbq', 'bench', 'dog_toilet', 'give_box', 'shelter', 'shower', 'telephone',
                       'toilets', 'water_point', 'watering_place'],
        'waste_management': ['sanitary_dump_station', 'recycling', 'waste_basket', 'waste_disposal',
                             'waste_transfer_station']
    }

    for group, amenities in amenity_groups.items():
        if string in amenities:
            return group

    return None

def old_amen_group(x: str or None) -> str or None:
    """
    Used to parse older osm files
    """
    if isinstance(x, str):
        match = re.findall(r'"amenity"=>"(.*?)"', x)
        if match:
            return amen_group(match[0])
        
    return None

def normalize_array(array: np.ndarray, high: int = 255) -> np.ndarray:
    min_val = np.min(array)
    return (array - min_val) * (high / (np.max(array) - min_val))

def format_money(amount: int or float) -> str:
    thousands_sep = ","
    decimal_point = "."

    # Convert the amount to a string
    formatted_amount = f"{amount:.2f}"

    # Split the formatted amount into integer and fractional parts
    integer_part, _ = formatted_amount.split(".")

    # Add thousands separators
    integer_part_with_sep = ""
    for i, digit in enumerate(reversed(integer_part)):
        if i > 0 and i % 3 == 0:
            integer_part_with_sep = thousands_sep + integer_part_with_sep
        integer_part_with_sep = digit + integer_part_with_sep

    # Reconstruct the formatted amount
    formatted_currency = "$" + integer_part_with_sep
    return formatted_currency

def create_mem_dataset(ref_dataset: gdal.Dataset, dtype) -> gdal.Dataset:
    """
    Create a new dataset in memory that is the same shape, extent, and projection of an input dataset, of a certain type.
    """
    new_ds = gdal.GetDriverByName('MEM').Create('', ref_dataset.RasterXSize, ref_dataset.RasterYSize, 1, dtype)
    new_ds.SetProjection(ref_dataset.GetProjection())
    new_ds.SetGeoTransform(ref_dataset.GetGeoTransform())

    return new_ds

def create_disk_dataset(ref_dataset: gdal.Dataset, out_path: str, width: int, height: int, bands: int, dtype):
    """
    Create a new dataset that is the same shape, extent, and projection of an input dataset, of a certain type.
    """
    output_ds = gdal.GetDriverByName("GTiff").Create(out_path, width, height, bands, dtype)
    output_ds.SetGeoTransform(ref_dataset.GetGeoTransform())
    output_ds.SetProjection(ref_dataset.GetProjection())
    
    return output_ds
