import os.path

import shutil
import rasterio
import copy
import numpy as np

from rasterio.warp import calculate_default_transform, reproject, Resampling

from utils.files import get_file_stem_until_post

def convert_tif_2_crs():
    pass

def tifs_2_tif_depth(folder_path: str, tifs_list: list[str], postfix: str, post_stem: str = 'ens', threshold: float = 0.8, n_bands: int = 211, max_block_process_size: int = 1000) -> str:
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
    transform_ref = None

    # Initialize message for different resolutions
    msg_different_resolutions = None

    # Read the metadata of all the tifs and store the reference metadata for the one with the highest resolution
    for tif_file in tifs_list:
        with rasterio.open(os.path.join(folder_path, tif_file)) as src:
            meta = src.meta

            array = src.read(1)

            # check if it's empty
            if np.any(array != 0):

                # Find the indices of the non-zero values
                nonzero_indices = np.nonzero(array)

                # Extract the minimum and maximum values for each dimension
                min_x, max_x = np.min(nonzero_indices[1]), np.max(nonzero_indices[1])
                min_y, max_y = np.min(nonzero_indices[0]), np.max(nonzero_indices[0])

                # Get the transform
                transform_c = meta['transform'].c + min_x * meta['transform'].a
                transform_f = meta['transform'].f + min_y * meta['transform'].e
                transform = rasterio.Affine(meta['transform'].a, meta['transform'].b, transform_c, meta['transform'].d, meta['transform'].e, transform_f)

                # Get the width and height
                width = max_x - min_x
                height = max_y - min_y

                # Update the metadata
                meta.update({
                    'transform': transform,
                    'width': width,
                    'height': height,
                    'crs': crs_ref
                })

                if meta_ref is None:
                    meta_ref = copy.deepcopy(meta)
                    crs_ref = copy.deepcopy(src.crs)
                    transform_ref = copy.deepcopy(transform)
                    array_ref = np.empty((height, width), dtype=meta_ref['dtype'])
                else:
                    # take the maximum extent
                    transform_ref_c = min(transform_ref.c, transform.c)
                    transform_ref_f = max(transform_ref.f, transform.f)
                    transform_ref = rasterio.Affine(transform_ref.a, transform_ref.b, transform_ref_c, transform_ref.d, transform_ref.e, transform_ref_f)
                    width_ref = max(meta_ref['width'], meta['width'])
                    height_ref = max(meta_ref['height'], meta['height'])
                    array_ref = np.empty((height_ref, width_ref), dtype=meta_ref['dtype'])
                    # update the reference metadata
                    meta_ref.update({
                        'transform': transform_ref,
                        'width': width_ref,
                        'height': height_ref,
                        'crs': crs_ref
                    })

    print(f'\t\t\tReference resolution: , {meta_ref["width"]}x{meta_ref["height"]}')

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

                # Check if the resolution, coordinates and affine transformation coefficients are different from the reference
                elif src.meta['transform'] != meta_ref['transform'] or src.meta['crs'] != meta_ref['crs'] or src.meta['height'] != meta_ref['height'] or src.meta['width'] != meta_ref['width']:

                    if msg_different_resolutions is None:
                        msg_different_resolutions = f'\t\t\t\t\033[31mWarning: The TIF files that you are trying to combine come from different resolutions and/or regions. ✘' # \n{meta_ref}\n{meta}
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

    # initialize the ensemble agreement array
    ensemble_agreement = np.zeros_like(stacked[0])

    for i in range(int(np.ceil(stacked.shape[1] / max_block_process_size))):
        for j in range(int(np.ceil(stacked.shape[2] / max_block_process_size))):
            print(f'\t\t\t\tProcessing block ({i + 1}/{int(np.ceil(stacked.shape[1] / max_block_process_size))} ; {j+1}/{int(np.ceil(stacked.shape[2] / max_block_process_size))}) ({(i + 1) * max_block_process_size}/{stacked.shape[1]}) ; ({(j+1)*max_block_process_size}/{stacked.shape[2]})')
            stacked_partition = stacked[:, i*max_block_process_size:(i+1)*max_block_process_size, j*max_block_process_size:(j+1)*max_block_process_size]


            # Get the count of each depth value for each pixel
            counts = np.apply_along_axis(lambda x: np.bincount(x[x != 0], minlength=n_bands + 1), axis=0, arr=stacked_partition)

            # Get the most common depth value for each pixel and its count
            most_common_depth = np.argmax(counts, axis=0)
            most_common_depth_count = np.max(counts, axis=0)

            # Calculate the probability of the most common depth value for each pixel
            probability = most_common_depth_count / stacked_partition.shape[0]

            # Keep only the most common depth values with a probability above the threshold
            ensemble_agreement_partition = np.where(probability >= threshold, most_common_depth, 0)

            # Write the array sub_section to ensemble_agreement
            ensemble_agreement[i*max_block_process_size:(i+1)*max_block_process_size, j*max_block_process_size:(j+1)*max_block_process_size] = ensemble_agreement_partition

    # update meta to compress and tile
    meta_ref.update({
        'compress': 'lzw',
        'tiled': True,
    })

    # Write the resulting raster to a new geotiff file
    with rasterio.open(output_file, 'w', **meta_ref) as dst:
        print(f'\t\t\t\tWrite the resulting raster to a new geotiff file: {output_file}')
        dst.write(ensemble_agreement, 1)

    return output_file
