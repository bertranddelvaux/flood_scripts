#####################################################
# Author: Bertrand Delvaux (2023)                   #
#                                                   #
# Script to populate ARC's Flood Explorer buffer    #
#                                                   #
#####################################################

import os
import time
import argparse
import pysftp


from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, TMP_FOLDER, N_DAYS

from utils.files import createFolderIfNotExists, createDataTreeStructure, DICT_DATA_TREE

from utils.date import year, month, day, increment_day

# TODO: REMOVE set date!!
year, month, day = '2023', '03', '31'
# TODO: REMOVE

from utils.tif import tifs_2_tif_depth


def pipeline_tifs():
    pass

def process_files_include_exclude(include_str:str, exclude_str:str, sftp:pysftp.Connection, path_sftp:str, tmp_path:str, postfix='_depth.tif',
                                 n_bands=211, threshold=0.8) -> bool | None:

    # get list of files
    list_files = [tif.filename for tif in sftp.listdir_attr(path_sftp) if
                        include_str in tif.filename and exclude_str not in tif.filename]

    # download files to temp folder
    try:
        print(f'\t\t\tDownloading {len(list_files)} from the sftp server ... ', end='')
        for i, file in enumerate(list_files):
            sftp.get(os.path.join(path_sftp, file), os.path.join(tmp_path, file))
        print('\033[32m' + '✔' + '\033[0m')
    except:
        print('\033[31m' + '✘' + '\033[0m')
        return False

    # process depth map
    try:
        output_file = tifs_2_tif_depth(folder_path=tmp_path, tifs_list=list_files, postfix=postfix, n_bands=n_bands, threshold=threshold)
    except:
        return False

    # remove files from temp folder
    for file in list_files:
        os.remove(os.path.join(tmp_path, file))

    print(f'\t\t\tCreated depth map: \033[32m{output_file}\033[0m ', end='')

    return True


def pipeline(n_days: int = N_DAYS):

    # Make sure that the data tree structure exists
    createDataTreeStructure()

    # Get the latest data from JBA's server
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD, cnopts=cnopts) as sftp:

        for country in LIST_COUNTRIES:
            print(f'Fetching data for {country}...')

            for sub_folder in LIST_SUBFOLDERS_BUFFER:
                print(f'\tFetching {sub_folder} data...')

                path_sftp = os.path.join(country, sub_folder, year, month, day)

                if sub_folder == IMPACTS_FOLDER:
                    path = os.path.join(DATA_FOLDER, country, sub_folder)
                    sftp.get_d(path_sftp, path, preserve_mtime=False)

                elif sub_folder == RASTER_FOLDER:
                    tmp_path = os.path.join(DATA_FOLDER, country, sub_folder, TMP_FOLDER)
                    createFolderIfNotExists(tmp_path)

                    for i_day in range(0, n_days):
                        # TODO: REMOVE
                        i_day = 1
                        # TODO: REMOVE
                        year_n, month_n, day_n = increment_day(year, month, day, i_day)

                        # print day
                        print(f'\t\tProcessing \033[1mday {i_day}\033[0m : ({year_n}-{month_n}-{day_n}) ... ')

                        # create depth map
                        success = process_files_include_exclude(
                            include_str=f'fe{year_n}{month_n}{day_n}',
                            exclude_str='Agreement',
                            sftp=sftp,
                            path_sftp=path_sftp,
                            tmp_path=tmp_path,
                            postfix='_depth.tif',
                            n_bands=211,
                            threshold=0.8
                        )
                        if success:
                            print(f'(\033[1mday {i_day}\033[0m)')
                        else:
                            print(f'\t\t\033[31mCould not create depth map for day \033[1m{i_day}\033[0m')

                    exit()

                    ######TODO: START HERE




                    # create depth map of day 0

                    ## get list of files for day 0
                    list_files_day_0 = [tif.filename for tif in sftp.listdir_attr(path_sftp) if
                     f'fe{year}{month}{day}' in tif.filename and 'Agreement' not in tif.filename]

                    ## download files for day 0
                    # for file in list_files_day_0:
                    #     sftp.get(os.path.join(path_sftp, file), os.path.join(tmp_path, file))

                    ## process depth map of day 0
                    output_file = tifs_2_tif_depth(folder_path=tmp_path, tifs_list=list_files_day_0, postfix='_depth.tif', n_bands=211) #TODO: remove [4] when all files are available and n_bands

                    if output_file:
                        print(f'\t\tCreated depth map for day 0: \033[32m{output_file}\033[0m')

                    print()



                    sftp.get_d(path_sftp, tmp_path, preserve_mtime=False)

                    # create depth map of day 0

                    # create depth map (probability >= 08) of day 1-10

                    # check if event needs to be created

                    # does the event already exist?

                    # if not, create it

                    # if yes, update it (json)

                    # add the depth map of the event

                    # append the csv of the event

                    # remove temporary data from buffer folder


                print(f'\t\tDownloaded \033[32m{path_sftp}\033[0m from the sftp server')

                #except:
                    #print(f'\t\tCould not get \033[31m{path_sftp}\033[0m from JBA\'s server')




        # loop through subfolders

            # get latest data from JBA's server








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    #parser.add_argument('-d', '--data_folder', help='Path folder for the data', default='data')
    args = parser.parse_args()

    pipeline()#args.data_folder)
