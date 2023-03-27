#####################################################
# Author: Bertrand Delvaux (2023)                   #
#                                                   #
# Script to populate ARC's Flood Explorer buffer    #
#                                                   #
#####################################################

import os
import argparse
import pysftp


from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, TMP_FOLDER

from utils.files import createFolderIfNotExists, createDataTreeStructure, DICT_DATA_TREE

from utils.date import year, month, day


def pipeline_tifs():
    pass

#TODO: example of meta-script:
def pipeline():

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
                    print(f'\t\tDownloaded \033[32m{path_sftp}\033[0m from JBA\'s server')
                elif sub_folder == RASTER_FOLDER:

                    try:
                        # TODO: copy temporary data to buffer folder
                        tmp_path = os.path.join(DATA_FOLDER, country, sub_folder, TMP_FOLDER)
                        createFolderIfNotExists(tmp_path)
                        sftp.get_d(path_sftp, tmp_path, preserve_mtime=False)
                        print(f'\t\tDownloaded \033[32m{path_sftp}\033[0m from JBA\'s server')
                    except:
                        print(f'\t\tCould not get \033[31m{path_sftp}\033[0m from JBA\'s server')

                    pass
                    # create depth map of day 0

                    # create depth map (probability >= 08) of day 1-10

                    # check if event needs to be created

                    # does the event already exist?

                    # if not, create it

                    # if yes, update it (json)

                    # add the depth map of the event

                    # append the csv of the event

                    # remove temporary data from buffer folder




        # loop through subfolders

            # get latest data from JBA's server








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    #parser.add_argument('-d', '--data_folder', help='Path folder for the data', default='data')
    args = parser.parse_args()

    pipeline()#args.data_folder)
