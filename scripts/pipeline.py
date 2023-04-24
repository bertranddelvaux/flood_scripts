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

from utils.event import initialize_event, set_ongoing_event, save_json_last_edit

from utils.tif import tifs_2_tif_depth, tif_2_array

from utils.stats import array_2_stats

# TODO: REMOVE set date!!
year, month, day = '2023', '03', '31'
# TODO: REMOVE

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

            # Initialize JSON country file

            # json file for country
            json_path_country = os.path.join(DATA_FOLDER, country)
            json_file_country = f'{country}.json'

            # initialize event for country
            dict_country = initialize_event(
                json_path=json_path_country,
                json_file=json_file_country,
                json_dict_update={
                    'total_events_country': 0,
                    'total_days_country': 0,
                    'peak_year': {},
                    'year_by_year': []
                }
            )

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

                        #TODO: RESTORE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
                        # TODO: RESTORE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                        #TODO: REMOVE ------------------------------------------------------------------------------------------
                        import random
                        #empty = random.choice([True, False])
                        if random.random() > 0.75:
                            empty = True
                        else:
                            empty = False
                        i_day = 0
                        #empty = False
                        #TODO: REMOVE ------------------------------------------------------------------------------------------

                        # for day 0, check if empty (no depth map)
                        if i_day == 0:

                            # Initialize JSON year file
                            # json file for year
                            json_path_year = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_n)
                            json_file_year = f'{country}_{year_n}.json'

                            # initialize event for year
                            dict_year = initialize_event(
                                json_path=json_path_year,
                                json_file=json_file_year,
                                json_dict_update={
                                    'total_events_year': 0,
                                    'total_days_year': 0,
                                    'peak_event': {},
                                    'event_by_event': []
                                }
                            )

                            # check if empty:
                            print('\t\t\tNot empty? ', end='')
                            if empty:
                                print('\033[31m' + '✘' + '\033[0m')

                                # check if ongoing event exists
                                print('\t\t\tOngoing event? ', end='')
                                if dict_country['ongoing']:
                                    print('\033[32m' + '✔' + '\033[0m')

                                    # get the json year of the ongoing event
                                    year_ongoing = dict_country['ongoing_event_year']
                                    month_ongoing = dict_country['ongoing_event_month']
                                    day_ongoing = dict_country['ongoing_event_day']

                                    # get the json year of the ongoing event
                                    json_path_year = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing)
                                    json_file_year = f'{country}_{year_ongoing}.json'
                                    dict_year = jsonFileToDict(json_path_year, json_file_year)

                                    # get the json event of the ongoing event
                                    json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER,
                                                                   year_ongoing, month_ongoing, day_ongoing)
                                    json_file_event = f'{year_ongoing}_{month_ongoing}_{day_ongoing}.json'
                                    dict_event = jsonFileToDict(json_path_event, json_file_event)

                                    # close ongoing event
                                    print(f'\t\t\t\t\033[95mClosing ongoing event that started on {year_ongoing:04}_{month_ongoing:02}_{day_ongoing:02}... \033[0m')
                                    dict_country = set_ongoing_event(json_path_country, json_file_country, False)
                                    dict_year = set_ongoing_event(json_path_year, json_file_year, False)

                                    # update jsons
                                    #TODO: this is where country and year jsons are incremented
                                    dict_country['total_events_country'] += 1
                                    dict_country['total_days_country'] += dict_event['total_days_event']

                                    dict_year['total_events_year'] += 1
                                    dict_year['total_days_year'] += dict_event['total_days_event']

                                    dict_country = save_json_last_edit(json_path_country, json_file_country, dict_country)
                                    dict_year = save_json_last_edit(json_path_year, json_file_year, dict_year)

                                else:
                                    print('\033[31m' + '✘' + '\033[0m')


                            else:
                                print('\033[32m' + '✔' + '\033[0m')

                                # check if ongoing event exists
                                print('\t\t\tOngoing event? ', end='')

                                if dict_country['ongoing']:
                                    print('\033[32m' + '✔' + '\033[0m')



                                else:
                                    print('\033[31m' + '✘' + '\033[0m')

                                    # create new event
                                    print(f'\t\t\t\t\033[95mOpening new event on {year_n:04}_{month_n:02}_{day_n:02}... \033[0m')

                                    # json file for event
                                    json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_n,
                                                                   f'{month_n:02}',
                                                                   f'{day_n:02}')
                                    json_file_event = f'{year_n}_{month_n:02}_{day_n:02}.json'

                                    # initialize event
                                    dict_event = initialize_event(
                                        json_path=json_path_event,
                                        json_file=json_file_event,
                                        json_dict_update={
                                            #'ongoing': True,
                                            'total_days_event': 0,
                                            'day_by_day': [], #TODO: add the first day
                                            #'stats': {}, #TODO: initialize with the stats of the first day
                                            'peak_day': None, #TODO: peak day is the day with the highest stats, so the day of the creation, then the day with the highest stats
                                        },
                                        ongoing_year=year_n,
                                        ongoing_month=month_n,
                                        ongoing_day=day_n
                                    )

                                    # set ongoing event in country and year jsons
                                    dict_country = set_ongoing_event(
                                        json_path=json_path_country,
                                        json_file=json_file_country,
                                        ongoing=True,
                                        ongoing_year=year_n,
                                        ongoing_month=month_n,
                                        ongoing_day=day_n
                                    )

                                    dict_year = set_ongoing_event(
                                        json_path=json_path_year,
                                        json_file=json_file_year,
                                        ongoing=True,
                                        ongoing_year=year_n,
                                        ongoing_month=month_n,
                                        ongoing_day=day_n
                                    )

                                ### Update ongoing event and copy files

                                # get the json year of the ongoing event
                                year_ongoing = dict_country['ongoing_event_year']
                                month_ongoing = dict_country['ongoing_event_month']
                                day_ongoing = dict_country['ongoing_event_day']

                                #print(f'\t\t\t\tOngoing event: {year_ongoing:04}_{month_ongoing:02}_{day_ongoing:02}')

                                # get the json event of the ongoing event
                                json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER,
                                                               year_ongoing, month_ongoing, day_ongoing)
                                json_file_event = f'{year_ongoing}_{month_ongoing}_{day_ongoing}.json'
                                dict_event = jsonFileToDict(json_path_event, json_file_event)

                                ## Copy files

                                #TODO: RESTORE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                                # copy the depth file
                                # depth_file = [os.path.join(DATA_FOLDER, country, RASTER_FOLDER, TMP_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, RASTER_FOLDER, TMP_FOLDER)) if f'fe{year_ongoing}{month_ongoing}{day_ongoing}' in f and 'depth.tif' in f][0]
                                # shutil.copy(depth_file, os.path.join(json_path_event, os.path.basename(depth_file)))
                                #
                                # # copy the impact file
                                # impact_file = [os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER)) if f'rd{year_ongoing}{month_ongoing}{day_ongoing}' in f and '.csv' in f][0]
                                # shutil.copy(impact_file, os.path.join(json_path_event, os.path.basename(impact_file)))
                                #
                                # # update ongoing event
                                # print('\t\t\tUpdating ongoing event... ')
                                # # update the json event of the ongoing event
                                # # open the raster file
                                # array, meta = tif_2_array(
                                #     os.path.join(json_path_event, os.path.basename(depth_file)))
                                # # get the stats
                                # stats = array_2_stats(
                                #     array=array,
                                #     pixel_size_x_m=meta['transform'].a,
                                #     pixel_size_y_m=meta['transform'].e
                                # )
                                # TODO: RESTORE ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                                # TODO: REMOVE ----------------------------------------------------------------------------------------------
                                stats = {}
                                # TODO: REMOVE ----------------------------------------------------------------------------------------------

                                dict_event['total_days_event'] += 1
                                dict_event['day_by_day'].append({
                                    'day': i_day,
                                    'stats': stats
                                })
                                dict_event['stats'] = {**dict_event['stats'],
                                                       **stats}  # TODO: add comparison with previous day and keep the 'best' one
                                # TODO: add logic for peak day
                                dict_event = save_json_last_edit(
                                    json_path=json_path_event,
                                    json_file=json_file_event,
                                    json_dict=dict_event
                                )
                                dict_event['last_edited'] = str(dt.datetime.utcnow())

                    exit()








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    #parser.add_argument('-d', '--data_folder', help='Path folder for the data', default='data')
    args = parser.parse_args()

    pipeline(n_days=100)#args.data_folder) #TODO: remove n_days
