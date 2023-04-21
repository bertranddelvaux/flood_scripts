import numpy as np

def array_2_stats(array: np.ndarray, pixel_size_x_m: float, pixel_size_y_m: float):
    """Return a dictionary with the stats of an array"""

    # only account for non-zero values
    array = array[array != 0]

    # Calculate the area of a single pixel
    pixel_area_m2 = pixel_size_x_m * abs(pixel_size_y_m)

    # calculate flooded area
    flooded_area_px = np.sum(array.astype(bool))
    flooded_area_m2 = flooded_area_px * pixel_area_m2 #TODO: recalculate with the actual pixel size!!!

    # calculate severity indices
    severity_index_1m = np.round(np.mean(array) / 20.0, 2) # number of times a meter of water would be filled with the water from the flood, band 20 = 1m
    severity_index_median = np.round(np.mean(array) / np.median(array), 2)

    return {
        'min': int(np.min(array)),
        'max': int(np.max(array)),
        'mean': np.round(np.mean(array),2),
        'std': np.round(np.std(array),2),
        'median': np.round(np.median(array),2),
        'flooded_area_px': int(flooded_area_px),
        'flooded_area_m2': flooded_area_m2,
        'severity_index_1m': severity_index_1m,
        'severity_index_median': severity_index_median,
    }