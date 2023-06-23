import numpy as np
import pandas as pd

def create_band_depth_mapping(csv_file):
    range_data = {}
    df = pd.read_csv(csv_file)
    for row in df.itertuples(index=False):
        band = getattr(row, 'band')
        depth_m = getattr(row, 'depth_m')
        range_data[band] = depth_m
    return range_data

def calculate_total_water(numpy_array, csv_file, pixel_area_m2):
    range_data = {}
    df = pd.read_csv(csv_file)
    for row in df.itertuples(index=False):
        band = getattr(row, 'band')
        depth_m = getattr(row, 'depth_m')
        range_data[band] = depth_m

    depth_array = np.zeros_like(numpy_array, dtype=float)
    for band, depth in range_data.items():
        depth_array[numpy_array == band] = depth

    total_water = np.sum(depth_array * pixel_area_m2)
    return total_water