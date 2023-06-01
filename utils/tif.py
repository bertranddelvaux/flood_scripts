#TODO: next implementation: for the reference resolution, check the non-null pixels that overlap at at least threshold (0.8)

import os.path

import shutil
import rasterio
import copy
import numpy as np

from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.crs import CRS

from utils.files import get_file_stem_until_post

def tif_2_array(tif_file: str) -> tuple[np.ndarray, dict]:
    """
    Get a tif file and return an array and the metadata
    :param tif_file:
    :return:
    """

    with rasterio.open(tif_file) as src:
        array = src.read()
        meta = src.meta

    return array, meta

def reproject_tif(tif_file: str, to_crs: str | CRS | dict) -> tuple:
    """
    Convert a tif file to a CRS
    :param to_crs:
    :param tif_file:
    :param output_file:
    :return:
    """

    # copy the file to a temporary file
    tmp_file = f'{tif_file}.tmp'
    shutil.copyfile(tif_file, tmp_file)

    with rasterio.open(tmp_file) as src:

        transform_ref = src.transform
        width_ref = src.width
        height_ref = src.height
        left, bottom, right, top = transform_ref.c, transform_ref.f + transform_ref.e * height_ref, transform_ref.c + transform_ref.a * width_ref, transform_ref.f

        transform, width, height = calculate_default_transform(src.crs, to_crs, width_ref, height_ref, left, bottom, right, top)
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': to_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'compress': 'lzw',
            'tiled': True,
        })

        with rasterio.open(tif_file, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=to_crs,
                    resampling=Resampling.bilinear
                )

    # remove the temporary file
    os.remove(tmp_file)

    bbox = dst.bounds

    return bbox


def reproject_geotiff(tif_file: str, max_resolution: int = 16000, msg_max_resolution: str =''):
    """
    Reproject a GeoTIFF file to a maximum resolution
    :param input_path:
    :param output_path:
    :param max_resolution:
    :return:
    """

    # copy the file to a temporary file
    tmp_file = f'{tif_file}.tmp'
    shutil.copyfile(tif_file, tmp_file)

    with rasterio.open(tmp_file) as src:
        # Calculate the target resolution
        src_transform, src_width, src_height = src.transform, src.width, src.height
        max_dimension = max(src_width, src_height)

        if max_dimension > max_resolution:
            if msg_max_resolution is None:
                msg_max_resolution = 'Resolution too high, it needs to be downsized to a maximum of {max_resolution} px per dimension ... '
                print(f'\t\t\t\033[31m{msg_max_resolution} \033[0m', end='')
            else:
                print(f'\033[32m' + '✔' + '\033[0m', end='')
            target_resolution = max_dimension / max_resolution
            dst_transform = rasterio.Affine(src_transform.a*target_resolution, src_transform.b, src_transform.c, src_transform.d, src_transform.e*target_resolution, src_transform.f)
            dst_width = src_width // target_resolution
            dst_height = src_height // target_resolution
        else:
            dst_transform = src_transform
            dst_width = src_width
            dst_height = src_height

        # Prepare the output dataset
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': src.crs,
            'transform': dst_transform,
            'width': dst_width,
            'height': dst_height,
            'compress': 'lzw',
            'tiled': True,
        })

        # Reproject the dataset
        with rasterio.open(tif_file, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src_transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=src.crs,  # Use the same CRS for the output
                    resampling = Resampling.bilinear
                )

    # remove the temporary file
    os.remove(tmp_file)

    return msg_max_resolution


def crop_array_tif_meta(array: np.ndarray, meta: dict) -> tuple[dict, rasterio.Affine, int, int]:
    """
    Crop the array
    :param array:
    :param meta:
    :return:
    """

    # Find the indices of the non-zero values
    nonzero_indices = np.nonzero(array)

    # Extract the minimum and maximum values for each dimension
    min_x, max_x = np.min(nonzero_indices[1]), np.max(nonzero_indices[1])
    min_y, max_y = np.min(nonzero_indices[0]), np.max(nonzero_indices[0])

    # Get the transform
    transform_c = meta['transform'].c + min_x * meta['transform'].a
    transform_f = meta['transform'].f + min_y * meta['transform'].e
    transform = rasterio.Affine(meta['transform'].a, meta['transform'].b, transform_c, meta['transform'].d,
                                meta['transform'].e, transform_f)

    # Get the width and height
    width = max_x - min_x
    height = max_y - min_y

    # Update the metadata
    meta.update({
        'transform': transform,
        'width': width,
        'height': height,
    })

    return meta, transform, width, height

def reproject_and_maximize_tifs(tifs_list: list[str], output_file: str, to_epsg_3857: bool = True):
    # Initialize variables to store the spatial extent
    min_x, min_y, max_x, max_y = float('inf'), float('inf'), float('-inf'), float('-inf')

    # Initialize reference metadata
    meta_ref = None
    array_ref = None
    transform_ref = None
    width_ref = 0
    height_ref = 0

    # Iterate over the files to find the common extent
    for tif_file in tifs_list:
        with rasterio.open(tif_file) as src:
            bounds = src.bounds
            meta = src.meta
            array = src.read(1)
            transform = src.transform
            height, width = array.shape
            #meta, transform, width, height = crop_array_tif_meta(array, meta)

            # min_x = min(min_x, bounds.left)
            # min_y = min(min_y, bounds.bottom)
            # max_x = max(max_x, bounds.right)
            # max_y = max(max_y, bounds.top)

            min_x = min(min_x, transform.c)
            min_y = min(min_y, transform.f + transform.e * height)
            max_x = max(max_x, transform.c + transform.a * width)
            max_y = max(max_y, transform.f)

            dtype = src.dtypes[0]

            if meta_ref is None:
                meta_ref = copy.deepcopy(meta)
                transform_ref = copy.deepcopy(transform)
                crs_ref = copy.deepcopy(src.crs)
                array_ref = np.empty((height, width), dtype=meta_ref['dtype'])
                width_ref = width
                height_ref = height
            else:
                # take the maximum extent
                transform_ref_c = min(transform_ref.c, transform.c)
                transform_ref_f = max(transform_ref.f, transform.f)
                transform_ref = rasterio.Affine(transform_ref.a, transform_ref.b, transform_ref_c, transform_ref.d,
                                                transform_ref.e, transform_ref_f)
                width_ref = max(width_ref, width)
                height_ref = max(height_ref, height)
                array_ref = np.empty((height_ref, width_ref), dtype=meta_ref['dtype'])
                # update the reference metadata
                meta_ref.update({
                    'transform': transform_ref,
                    'width': width_ref,
                    'height': height_ref,
                    'crs': crs_ref,
                })

    # Create a bounding box with the common extent
    bbox = (min_x, min_y, max_x, max_y)
    left, bottom, right, top = transform_ref.c, transform_ref.f + transform_ref.e * height_ref, transform_ref.c + transform_ref.a * width_ref, transform_ref.f

    # Reprojection parameters
    if to_epsg_3857:
        dst_crs = 'EPSG:3857'
    else:
        dst_crs = 'EPSG:4326'  # Specify the desired CRS
    dst_transform, dst_width, dst_height = calculate_default_transform(src.crs, dst_crs, width_ref, height_ref, left, bottom, right, top)

    # Initialize the array of max values
    array_dst = np.empty((dst_height, dst_width), dtype=dtype)
    array_max = np.empty((dst_height, dst_width), dtype=dtype)

    # Output folder for the reprojected files
    output_folder = ""

    # Iterate over the files
    for tif_file in tifs_list:
        with rasterio.open(tif_file) as src:
            # Reproject the raster to the common extent
            reprojected = rasterio.open(
                output_folder + output_file,
                'w',
                driver='GTiff',
                height=dst_height,
                width=dst_width,
                count=1,
                dtype=src.dtypes[0],
                crs=dst_crs,
                transform=dst_transform
            )
            reproject(
                source=rasterio.band(src, 1),
                destination=array_dst,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=Resampling.nearest
            )

            # Read the reprojected data
            array_max = np.maximum(array_max, array_dst)

            reprojected.close()


    # Write the output file with the maximum pixel values
    with rasterio.open(output_file, 'w', driver='GTiff', height=dst_height, width=dst_width, count=1,
                       dtype=dtype, crs=dst_crs, transform=dst_transform, compress='lzw', tiled=True) as dst:
        dst.write(array_max, 1)

    return bbox



def tifs_2_tif_depth(folder_path: str, tifs_list: list[str], postfix: str, post_stem: str = 'ens', threshold: float = 0.8, n_bands: int = 211, max_block_process_size: int = 1000, max_resolution: int = 16000, to_epsg_3857: bool = True) -> tuple[str, bool, tuple]:
    """
    Get a list of tifs and return a tif with the depth
    :param folder_path:
    :param tifs_list:
    :param postfix:
    :param post_stem:
    :param threshold:
    :param n_bands:
    :param max_block_process_size:
    :param to_epsg_3857:
    :return:
    """

    # Check that the tifs_list all have the same stem
    stem_set = set(get_file_stem_until_post(tif_file, post_stem) for tif_file in tifs_list)
    assert len(stem_set) == 1, f'\033[31mStems of tifs are not the same: {stem_set}\033[0m'

    # Initialize reference metadata
    meta_ref = None
    array_ref = None
    transform_ref = None

    # Initialize boolean empty
    empty = True

    # Initialize message for different resolutions
    msg_different_resolutions = None
    msg_max_resolution = None

    # Read the metadata of all the tifs and store the reference metadata for the one with the highest resolution
    for tif_file in tifs_list:

        if len(tifs_list) > 1:
            # sanity check: compress and max_resolution
            msg_max_resolution = reproject_geotiff(os.path.join(folder_path, tif_file), max_resolution=max_resolution, msg_max_resolution=msg_max_resolution)

        with rasterio.open(os.path.join(folder_path, tif_file)) as src:
            meta = src.meta
            crs = src.crs

            array = src.read(1)

            # check if it's empty
            if np.any(array != 0):
                #meta, transform, width, height = crop_array_tif_meta(array, meta)
                transform = src.transform
                width = src.width
                height = src.height

                if meta_ref is None:
                    meta_ref = copy.deepcopy(meta)
                    transform_ref = copy.deepcopy(transform)
                    crs_ref = copy.deepcopy(src.crs)
                    array_ref = np.empty((height, width), dtype=meta_ref['dtype'])
                    width_ref = width
                    height_ref = height
                else:
                    # take the maximum extent
                    transform_ref_c = min(transform_ref.c, transform.c)
                    transform_ref_f = max(transform_ref.f, transform.f)
                    transform_ref = rasterio.Affine(transform_ref.a, transform_ref.b, transform_ref_c, transform_ref.d, transform_ref.e, transform_ref_f)
                    width_ref = max(width_ref, width)
                    height_ref = max(height_ref, height)
                    array_ref = np.empty((height_ref, width_ref), dtype=meta_ref['dtype'])
                    # update the reference metadata
                    meta_ref.update({
                        'transform': transform_ref,
                        'width': width_ref,
                        'height': height_ref,
                        'crs': crs_ref,
                    })

    # close the msg_max_resolution printing message
    if msg_max_resolution is not None:
        print()

    if meta_ref is None:
        meta_ref = copy.deepcopy(meta)
        crs_ref = copy.deepcopy(src.crs)
        array_ref = np.empty((meta_ref['height'], meta_ref['width']), dtype=meta_ref['dtype'])

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
                elif src.meta['transform'] != meta_ref['transform'] or src.meta['height'] != meta_ref['height'] or src.meta['width'] != meta_ref['width']:

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
                              resampling=Resampling.bilinear)

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
    if not arrays or len(arrays) == 1:
        if not arrays:
            print(f'\t\t\t\tAll files are empty, copying first file to output file: {tifs_list[0]}')
        elif len(arrays) == 1:
            print(f'\t\t\t\tOnly one file is not empty, copying it to output file: {tifs_list[0]}')
            empty = False

        shutil.copy(os.path.join(folder_path, tifs_list[0]), output_file)

        # open the file to get the bbox
        with rasterio.open(output_file) as src:
            bbox = src.bounds

        if to_epsg_3857:
            bbox = reproject_tif(output_file, to_crs='EPSG:3857')
        return output_file, empty, bbox

    # Stack the arrays into a single numpy array
    stacked = np.stack(arrays)

    # Clip values above n_bands
    stacked = np.clip(stacked, 0, n_bands)

    # initialize the ensemble agreement array
    ensemble_agreement = np.zeros_like(stacked[0])

    # initialize max count
    max_count = 0

    for i in range(int(np.ceil(stacked.shape[1] / max_block_process_size))):
        for j in range(int(np.ceil(stacked.shape[2] / max_block_process_size))):
            print(f'\t\t\t\tProcessing block ({i + 1}/{int(np.ceil(stacked.shape[1] / max_block_process_size))} ; {j+1}/{int(np.ceil(stacked.shape[2] / max_block_process_size))}) ({(i + 1) * max_block_process_size}/{stacked.shape[1]}) ; ({(j+1)*max_block_process_size}/{stacked.shape[2]})')
            stacked_partition = stacked[:, i*max_block_process_size:(i+1)*max_block_process_size, j*max_block_process_size:(j+1)*max_block_process_size]


            # Get the count of each depth value for each pixel
            counts = np.apply_along_axis(lambda x: np.bincount(x[x != 0], minlength=n_bands + 1), axis=0, arr=stacked_partition)

            # Get the most common depth value for each pixel and its count
            most_common_depth = np.argmax(counts, axis=0)
            most_common_depth_count = np.max(counts, axis=0)

            # Update max count
            max_count = np.max([max_count, np.max(most_common_depth_count)])

            # Calculate the probability of the most common depth value for each pixel
            probability = most_common_depth_count / stacked_partition.shape[0]

            # Keep only the most common depth values with a probability above the threshold
            ensemble_agreement_partition = np.where(probability >= threshold, most_common_depth, 0)

            # Write the array sub_section to ensemble_agreement
            ensemble_agreement[i*max_block_process_size:(i+1)*max_block_process_size, j*max_block_process_size:(j+1)*max_block_process_size] = ensemble_agreement_partition

    # Print max agreement
    print(f'\t\t\t\tMax agreement: {max_count}/{stacked.shape[0]} ({max_count/stacked.shape[0]*100:.2f}%), ', end='')
    if max_count / stacked.shape[0] * 100 < threshold * 100:
        print(f"\033[31m{'below the threshold'}\033[0m")  # 31 for red color
    else:
        print(f"\033[32m{'above the threshold'}\033[0m")  # 32 for green color


    # Check if the ensemble agreement array is empty
    if np.any(ensemble_agreement != 0):

        empty = False

        # Crop the array to the reference resolution
        #TODO: correct the following line to crop appropriately
        #meta_ref, transform, width, height = crop_array_tif_meta(ensemble_agreement, meta_ref)

        print(f'\t\t\t\tCropping the array to the reference resolution: {width}x{height}')

        # Reproject the array to the reference system
        dest = np.empty((height, width), dtype=meta_ref['dtype'])
        reproject(source=ensemble_agreement,
                  destination=dest,
                  src_transform=transform,
                  src_crs=crs_ref,
                  dst_transform=transform,
                  dst_crs=crs_ref,
                  resampling=Resampling.bilinear)

        ensemble_agreement = dest

    # update meta to compress and tile
    meta_ref.update({
        'compress': 'lzw',
        'tiled': True,
    })

    # Write the resulting raster to a new geotiff file
    with rasterio.open(output_file, 'w', **meta_ref) as dst:
        print(f'\t\t\t\tWrite the resulting raster to a new geotiff file: {output_file}')
        dst.write(ensemble_agreement, 1)

    bbox = dst.bounds

    if to_epsg_3857:
        bbox = reproject_tif(output_file, to_crs='EPSG:3857')

    # Return the output file name, and a boolean indicating if the array is empty
    return output_file, empty, bbox
