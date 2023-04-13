import os.path

import rasterio
import copy
import numpy as np

from rasterio.merge import merge

from scipy.stats import mode

from utils.files import get_file_stem_until_post

def tifs2tif_depth(folder_path: str, tifs_list: list[str], postfix: str, post_stem: str ='ens', threshold: float = 0.8, n_bands: int = 211) -> str:
    """
    Create a depth map from a list of tifs
    :param folder_path:
    :param tifs_list:
    :param postfix:
    :param post_stem:
    :param threshold:
    :param n_bands:
    :return:
    """

    # Check that the tifs_list all have the same stem
    stem_set = set(get_file_stem_until_post(tif_file, post_stem) for tif_file in tifs_list)
    assert len(stem_set) == 1, f"Stems of tifs are not the same: {stem_set}"

    # Initialize reference metadata
    meta_ref = None

    # Extract the pixel values from each dataset and store them in a numpy array:
    arrays = []
    for tif_file in tifs_list:
        with rasterio.open(os.path.join(folder_path, tif_file)) as src:
            arrays.append(src.read(1))

            # if reference values are not set, set them
            meta = src.meta
            if meta_ref is None:
                meta_ref = copy.deepcopy(meta)
            if meta != meta_ref:
                raise Exception(f'The TIF file that you are trying to combine come from different resolution and/or regions:\n{meta_ref}\n{meta}')

    # Stack the arrays into a single numpy array
    stacked = np.stack(arrays)

    # Clip values above n_bands
    stacked = np.clip(stacked, 0, n_bands)

    # Get the count of each depth value for each pixel
    counts = np.apply_along_axis(lambda x: np.bincount(x, minlength=n_bands+1), axis=0, arr=stacked)

    # Get the most common depth value for each pixel and its count
    most_common_depth = np.argmax(counts, axis=0)
    most_common_depth_count = np.max(counts, axis=0)

    # Calculate the probability of the most common depth value for each pixel
    probability = most_common_depth_count / stacked.shape[0]

    # Keep only the most common depth values with a probability above the threshold
    ensemble_agreement = np.where(probability >= threshold, most_common_depth, 0)

    # update meta to compress and tile
    meta_ref.update({
        'compress': 'lzw',
        'tiled': True,
    })

    # Write the resulting raster to a new geotiff file
    output_file = os.path.join(folder_path, f'{stem_set.pop()}{postfix}')
    with rasterio.open(output_file, 'w', **meta_ref) as dst:
        dst.write(ensemble_agreement, 1)

    return output_file

