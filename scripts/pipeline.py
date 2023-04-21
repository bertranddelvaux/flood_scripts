#####################################################
# Author: Bertrand Delvaux (2023)                   #
#                                                   #
# Script to populate ARC's Flood Explorer buffer    #
#                                                   #
#####################################################

import os
import shutil
import time
import datetime as dt
import argparse
import pysftp


from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, TMP_FOLDER, N_DAYS

from utils.files import createFolderIfNotExists, createDataTreeStructure, DICT_DATA_TREE

from utils.date import year, month, day, increment_day

from utils.json import createJSONifNotExists, jsonFileToDict, dictToJSONFile

# TODO: REMOVE set date!!
year, month, day = '2023', '03', '31'
# TODO: REMOVE

from utils.tif import tifs_2_tif_depth


def pipeline_tifs():
    pass

def process_files_include_exclude(include_str:str, exclude_str:str, sftp:pysftp.Connection, path_sftp:str, tmp_path:str, postfix='_depth.tif',
                                 n_bands=211, threshold=0.8) -> tuple[bool, bool]:

    # initialize success flag
    success = False
    empty = False

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
        return success, empty

    # process depth map
    #try:
    raster_depth_file, empty = tifs_2_tif_depth(folder_path=tmp_path, tifs_list=list_files, postfix=postfix, n_bands=n_bands, threshold=threshold)
    success = True
    #except:
        #return False

    # remove files from temp folder
    for file in list_files:
        os.remove(os.path.join(tmp_path, file))

    print(f'\t\t\tCreated depth map: \033[32m{raster_depth_file}\033[0m ', end='')

    return success, empty


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
                        year_n, month_n, day_n = increment_day(year, month, day, i_day)

                        # print day
                        print(f'\t\tProcessing \033[1mday {i_day}\033[0m : ({year_n}-{month_n}-{day_n}) ... ')

                        # create depth map
                        # success, empty = process_files_include_exclude(
                        #     include_str=f'fe{year_n}{month_n}{day_n}',
                        #     exclude_str='Agreement',
                        #     sftp=sftp,
                        #     path_sftp=path_sftp,
                        #     tmp_path=tmp_path,
                        #     postfix='_depth.tif',
                        #     n_bands=211,
                        #     threshold=0.8
                        # )
                        # if success:
                        #     print(f'(\033[1mday {i_day}\033[0m)')
                        # else:
                        #     print(f'\t\t\033[31mCould not create depth map for day \033[1m{i_day}\033[0m')

                        #TODO: REMOVE
                        import random
                        empty = random.choice([True, False])
                        #empty = False
                        #TODO: REMOVE

                        # for day 0, check if empty (no depth map)
                        if i_day == 0:

                            # json file for country
                            json_path_country = os.path.join(DATA_FOLDER, country)
                            json_file_country = f'{country}.json'

                            # json file for year
                            json_path_year = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_n)
                            json_file_year = f'{country}_{year_n}.json'

                            # json file for event
                            json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_n, f'{month:02}',
                                                           f'{day:02}')
                            json_file_event = f'{year_n}_{month_n:02}_{day_n:02}.json'

                            # intialize json files

                            ## Create JSON files if they don't exist
                            dict_default_values = {
                                'created': str(dt.datetime.utcnow()),
                                'last_edited': str(dt.datetime.utcnow()),
                                'ongoing': True,
                                'ongoing_event_year': year_n,
                                'ongoing_event_month': month_n,
                                'ongoing_event_day': day_n,
                                'stats': {}
                            }

                            # create json at country level
                            createJSONifNotExists(
                                json_path=json_path_country,
                                json_file=json_file_country,
                                json_dict={**dict_default_values, **{
                                    'total_events_country': 0,
                                    'total_days_country': 0,
                                    'peak_year': {},
                                    'year_by_year': []
                                }}
                            )

                            # create json at year level
                            createJSONifNotExists(
                                json_path=json_path_year,
                                json_file=json_file_year,
                                json_dict={**dict_default_values, **{
                                    'total_events_year': 0,
                                    'total_days_year': 0,
                                    'peak_event': {},
                                    'event_by_event': []
                                }}
                            )

                            # create json at event level
                            createJSONifNotExists(
                                json_path=json_path_event,
                                json_file=json_file_event,
                                json_dict={**dict_default_values, **{
                                    'total_days': 0,
                                    'peak_day': {},
                                    'day_by_day': []
                                }}
                            )

                            # if not empty, check if event already exists
                            if not empty:

                                # load the jsons at country level, year level and event level
                                dict_country = jsonFileToDict(json_path_country, json_file_country)

                                # check if ongoing event exists
                                if dict_country['ongoing']:

                                    # get the json year of the ongoing event
                                    year_ongoing = dict_country['ongoing_event_year']
                                    month_ongoing = dict_country['ongoing_event_month']
                                    day_ongoing = dict_country['ongoing_event_day']

                                    # get the json year of the ongoing event
                                    json_path_year = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing)
                                    json_file_year = f'{country}_{year_ongoing}.json'
                                    dict_year = jsonFileToDict(json_path_year, json_file_year)

                                    # get the json event of the ongoing event
                                    json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing, month_ongoing, day_ongoing)
                                    json_file_event = f'{year_ongoing}_{month_ongoing}_{day_ongoing}.json'
                                    dict_event = jsonFileToDict(json_path_event, json_file_event)

                                    ## Copy files

                                    # copy the depth file
                                    depth_file = [os.path.join(DATA_FOLDER, country, RASTER_FOLDER, TMP_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, RASTER_FOLDER, TMP_FOLDER)) if f'fe{year_ongoing}{month_ongoing}{day_ongoing}' in f and 'depth.tif' in f][0]
                                    shutil.copy(depth_file, os.path.join(json_path_event, os.path.basename(depth_file)))

                                    # copy the impact file
                                    impact_file = [os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER)) if f'rd{year_ongoing}{month_ongoing}{day_ongoing}' in f and '.csv' in f][0]
                                    shutil.copy(impact_file, os.path.join(json_path_event, os.path.basename(impact_file)))



                                    # update the json event of the ongoing event
                                    dict_event['total_days'] += 1
                                    dict_event['day_by_day'].append({
                                        'day': day_n, #TODO: add stats here
                                    })
                                    #TODO: add logic for peak day
                                    dict_event['last_edited'] = str(dt.datetime.utcnow())

                                    # update the json year of the ongoing event
                                    dict_year['event_by_event'].append({
                                        'event': f'{month_n:02}_{day_n:02}', #TODO: add stats here
                                    })
                                    dict_year['last_edited'] = str(dt.datetime.utcnow())

                                    # update the json country of the ongoing event
                                    dict_country['last_edited'] = str(dt.datetime.utcnow())

                            # if empty, check if ongoing event exists
                            else:

                                # check if ongoing event exists
                                # load the jsons at country level, year level and event level
                                dict_country = jsonFileToDict(json_path_country, json_file_country)
                                if dict_country['ongoing']:

                                    # get the json year of the ongoing event
                                    year_ongoing = dict_country['ongoing_event_year']
                                    month_ongoing = dict_country['ongoing_event_month']
                                    day_ongoing = dict_country['ongoing_event_day']

                                    # get the json year of the ongoing event
                                    json_path_year = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing)
                                    json_file_year = f'{country}_{year_ongoing}.json'
                                    dict_year = jsonFileToDict(json_path_year, json_file_year)

                                    # get the json event of the ongoing event
                                    json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing, month_ongoing, day_ongoing)
                                    json_file_event = f'{year_ongoing}_{month_ongoing}_{day_ongoing}.json'
                                    dict_event = jsonFileToDict(json_path_event, json_file_event)

                                    # if ongoing event exists, set it to False
                                    dict_country['ongoing'] = False
                                    dict_country['ongoing_event_year'] = None
                                    dict_country['ongoing_event_month'] = None
                                    dict_country['ongoing_event_day'] = None
                                    dict_country['total_events_country'] += 1
                                    dict_country['total_days_country'] += dict_event['total_days']
                                    dict_country['last_edited'] = str(dt.datetime.now())

                                    dict_year['ongoing'] = False
                                    dict_year['ongoing_event_year'] = None
                                    dict_year['ongoing_event_month'] = None
                                    dict_year['ongoing_event_day'] = None
                                    dict_year['total_events_year'] += 1
                                    dict_year['total_days_year'] += dict_event['total_days']
                                    dict_year['last_edited'] = str(dt.datetime.now())

                                    dict_event['ongoing'] = False
                                    dict_event['last_edited'] = str(dt.datetime.now())


                            # update the jsons
                            dictToJSONFile(json_path_country, json_file_country, dict_country)
                            dictToJSONFile(json_path_year, json_file_year, dict_year)
                            dictToJSONFile(json_path_event, json_file_event, dict_event)




                    exit()








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    #parser.add_argument('-d', '--data_folder', help='Path folder for the data', default='data')
    args = parser.parse_args()

    pipeline()#args.data_folder)
