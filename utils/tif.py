import os.path

import shutil
import rasterio
import copy
import numpy as np

from rasterio.warp import calculate_default_transform, reproject, Resampling

from utils.files import get_file_stem_until_post

def convert_tif_2_crs():
    pass

def tifs_2_tif_depth(folder_path: str, tifs_list: list[str], postfix: str, post_stem: str = 'ens', threshold: float = 0.8, n_bands: int = 211) -> str:
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
    assert len(stem_set) == 1, f'\033[31mStems of tifs are not the same: {stem_set}\033[0m'

    # Initialize reference metadata
    meta_ref = None
    crs_ref = None
    array_ref = None

    # Initialize message for different resolutions
    msg_different_resolutions = None

    # Extract the pixel values from each dataset and store them in a numpy array:
    arrays = []
    for tif_file in tifs_list:
        with rasterio.open(os.path.join(folder_path, tif_file)) as src:
            #TODO: check if same resolution, if not transform first then append

            array = src.read(1)

            # check if it's empty
            if np.any(array != 0):

                # if reference values are not set, set them
                meta = src.meta
                if meta_ref is None:
                    meta_ref = copy.deepcopy(meta)
                    crs_ref = copy.deepcopy(src.crs)
                    array_ref = copy.deepcopy(array)

                # Check if the resolution, coordinates and affine transformation coefficients are different from the reference
                elif src.meta['transform'] != meta_ref['transform'] or src.meta['crs'] != meta_ref['crs'] or src.meta['height'] != meta_ref['height'] or src.meta['width'] != meta_ref['width']:

                    if msg_different_resolutions is None:
                        msg_different_resolutions = f'\t\t\t\t\033[31mException: The TIF files that you are trying to combine come from different resolutions and/or regions. ✘' # \n{meta_ref}\n{meta}
                        print(msg_different_resolutions, end='')
                    else:
                        # Add ✘ for every file that has a different resolution
                        print('✘', end='')

                    # Reproject the array to the reference system
                    dest = np.empty_like(array_ref)
                    transform, width, height = calculate_default_transform(src.crs, crs_ref, src.width, src.height,
                                                                           *src.bounds)
                    kwargs = src.meta.copy()
                    kwargs.update({
                        'crs': crs_ref,
                        'transform': transform,
                        'width': width,
                        'height': height
                    })
                    reproject(source=rasterio.band(src, 1),
                              destination=dest,
                              src_transform=src.transform,
                              src_crs=src.crs,
                              dst_transform=transform,
                              dst_crs=crs_ref,
                              resampling=Resampling.nearest)

                    # Add the reprojected array to the list
                    arrays.append(dest)

                # Otherwise, add the array to the list without transforming it
                else:
                    arrays.append(array)

    # print empty line to close the message in case of different resolutions
    if msg_different_resolutions is not None:
        print('\033[0m')

    # Create the output file name
    output_file = os.path.join(folder_path, f'{stem_set.pop()}{postfix}')

    # if arrays is empty, copy the first ensemble file to the output file
    if not arrays:
        print(f'\t\t\t\tAll files are empty, copying first file to output file: {tifs_list[0]}')
        shutil.copy(os.path.join(folder_path, tifs_list[0]), output_file)
        return output_file

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
    with rasterio.open(output_file, 'w', **meta_ref) as dst:
        dst.write(ensemble_agreement, 1)

    return output_file
