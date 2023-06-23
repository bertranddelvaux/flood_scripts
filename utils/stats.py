import numpy as np

from utils.depth import calculate_total_water

def array_2_stats(array: np.ndarray, pixel_size_x_m: float, pixel_size_y_m: float):
    """Return a dictionary with the stats of an array"""

    # only account for non-zero values
    array = array[array != 0]

    # Calculate the area of a single pixel
    pixel_area_m2 = pixel_size_x_m * abs(pixel_size_y_m)

    # calculate flooded area
    flooded_area_px = np.sum(array.astype(bool))
    flooded_area_m2 = flooded_area_px * pixel_area_m2 #TODO: recalculate with the actual pixel size!!!
    flooded_area_km2 = flooded_area_m2 / 1000000

    # calculate severity indices
    total_water_m3 = calculate_total_water(array, './constants/range_classes.csv', pixel_area_m2)
    total_water_km3 = total_water_m3 / 1e9
    severity_index_1m = total_water_m3 / flooded_area_m2
    severity_index_1km2 = total_water_m3 / 1e6
    #severity_index_1m = np.round(np.mean(array) / 20.0, 2) # number of times a meter of water would be filled with the water from the flood, band 20 = 1m
    severity_index_median = np.round(np.mean(array) / np.median(array), 2)

    return {
        'min': int(np.min(array)),
        'max': int(np.max(array)),
        'mean': np.round(np.mean(array),2),
        'std': np.round(np.std(array),2),
        'median': np.round(np.median(array),2),
        'flooded_area_px': int(flooded_area_px),
        'flooded_area_m2': np.round(flooded_area_m2,2),
        'flooded_area_km2': np.round(flooded_area_km2, 2),
        'severity_index_1m': np.round(severity_index_1m, 2),
        'severity_index_1km2': np.round(severity_index_1km2, 2),
        'total_water_km3': np.round(total_water_km3, 2),
        'severity_index_median': severity_index_median,
    }