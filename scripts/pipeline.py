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


from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, BUFFER_FOLDER, N_DAYS

from utils.files import createFolderIfNotExists, createDataTreeStructure, DICT_DATA_TREE

from utils.date import year, month, day, increment_day

from utils.json import createJSONifNotExists, jsonFileToDict, dictToJSONFile

from utils.event import initialize_event, set_ongoing_event, save_json_last_edit

from utils.tif import tifs_2_tif_depth, tif_2_array

from utils.stats import array_2_stats


def clean_buffer(year: str, month: str, day: str, list_countries: list[str] = LIST_COUNTRIES, n_days: int = N_DAYS) -> None:
    """
    Remove past files which are not day 0, i.e. depth maps whose file name rdYYYYMMDD and feYYYYMMDD are different
    :param year:
    :param month:
    :param day:
    :param list_countries:
    :param n_days:
    :return:
    """
    # Remove past files which are not day 0, i.e. depth maps whose file name rdYYYYMMDD and feYYYYMMDD are different
    for country in list_countries:
        path = os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER)
        if os.path.exists(path):
            for file in os.listdir(path):
                # extract the 8 characters of file name after 'rd' and after 'fe'
                rd = file.split('rd')[1][:8]
                fe = file.split('fe')[1][:8]

                # get the latest date to keep
                year_last, month_last, day_last = increment_day(year, month, day, -n_days)

                if fe > rd or rd < f'{year_last}{month_last}{day_last}':
                    os.remove(os.path.join(path, file))

def process_files_include_exclude(include_str_list:list[str], exclude_str_list:list[str], buffer_path:str, postfix='_depth.tif',
                                  n_bands=211, threshold=0.8) -> tuple[bool, bool]:

    # get list of files
    list_files = [tif for tif in os.listdir(buffer_path) if all(include_str in tif for include_str in include_str_list) and include_str_list and not any(
        exclude_str in tif for exclude_str in exclude_str_list)]

    # check if list is empty
    if len(list_files) == 0:
        raise ValueError(f'No files found in buffer folder containing {", ".join(include_str_list)}')

    # process depth map
    raster_depth_file, empty = tifs_2_tif_depth(folder_path=buffer_path, tifs_list=list_files, postfix=postfix, n_bands=n_bands, threshold=threshold)
    success = True

    # remove files from temp folder
    for file in list_files:
        os.remove(os.path.join(buffer_path, file))

    print(f'\t\t\tCreated depth map: \033[32m{raster_depth_file}\033[0m ', end='')

    return success, empty


def pipeline(start_date: str = None, end_date: str = None, n_days: int = N_DAYS, list_countries: list[str] = LIST_COUNTRIES):

    # Make sure that the data tree structure exists
    createDataTreeStructure()

    # Get start and end dates
    if start_date is None:
        start_date = dt.datetime.now().strftime('%Y_%m_%d')
    if end_date is None:
        end_date = dt.datetime.now().strftime('%Y_%m_%d')

    # Get year, month, day from start_date
    year, month, day = start_date.split('_')

    # Get year, month, day from end_date
    year_end, month_end, day_end = end_date.split('_')

    # Loop over days between start_date and end_date
    while (dt.datetime(int(year_end), int(month_end), int(day_end)) - dt.datetime(int(year), int(month), int(day))).days >= 0 :

        for country in list_countries:

            print(f'Processing data for {country}...')

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
                print(f'\tProcessing {sub_folder} data...')

                if sub_folder == RASTER_FOLDER:
                    tmp_path = os.path.join(DATA_FOLDER, country, sub_folder, BUFFER_FOLDER)
                    createFolderIfNotExists(tmp_path)

                    for i_day in range(0, n_days):
                        year_n, month_n, day_n = increment_day(year, month, day, i_day)

                        # print day
                        print(f'\t\tProcessing \033[1mday {i_day}\033[0m : ({year_n}-{month_n}-{day_n}) ... ')

                        # create depth map
                        success, empty = process_files_include_exclude(
                            include_str_list=[f'fe{year_n}{month_n}{day_n}', f'rd{year}{month}{day}'],
                            exclude_str_list=['Agreement','_depth'],
                            buffer_path=tmp_path,
                            postfix='_depth.tif',
                            n_bands=211,
                            threshold=0.8
                        )
                        if success:
                            print(f'(\033[1mday {i_day}\033[0m)')
                        else:
                            print(f'\t\t\033[31mCould not create depth map for day \033[1m{i_day}\033[0m')

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

                                    #TODO: pick up the biggest numbers from the ongoing event and put them in the peak event: flooded area, flooded population, losses, severity_index

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

                                # copy the depth file
                                depth_file = [os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER)) if f'fe{year_ongoing}{month_ongoing}{day_ongoing}' in f and 'depth.tif' in f][0]
                                shutil.copy(depth_file, os.path.join(json_path_event, os.path.basename(depth_file)))

                                # copy the impact file
                                impact_file = [os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER, f) for f in os.listdir(os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER)) if f'rd{year_ongoing}{month_ongoing}{day_ongoing}' in f and '.csv' in f][0]
                                shutil.copy(impact_file, os.path.join(json_path_event, os.path.basename(impact_file)))

                                # update ongoing event
                                print('\t\t\tUpdating ongoing event... ')
                                # update the json event of the ongoing event

                                # open the raster file
                                array, meta = tif_2_array(
                                    os.path.join(json_path_event, os.path.basename(depth_file)))
                                # get the stats
                                stats = array_2_stats(
                                    array=array,
                                    pixel_size_x_m=meta['transform'].a,
                                    pixel_size_y_m=meta['transform'].e
                                )

                                # update the json event of the ongoing event
                                dict_event['total_days_event'] += 1
                                dict_event['day_by_day'].append({
                                    'day': i_day,
                                    'stats': stats
                                })
                                dict_event['stats'] = {**dict_event['stats'],
                                                       **stats}

                                # TODO: add comparison with previous day and keep the 'best' one
                                # TODO: add logic for peak day
                                # compare the severity_index_1m of the current day with the peak day, and replace if higher
                                if dict_event['peak_day'] is None:
                                    dict_event['peak_day'] = {
                                        'day': i_day,
                                        'stats': stats
                                    }
                                else:
                                    if stats['severity_index_1m'] > dict_event['peak_day']['stats']['severity_index_1m']:
                                        dict_event['peak_day'] = {
                                            'day': i_day,
                                            'stats': stats
                                        }

                                dict_event = save_json_last_edit(
                                    json_path=json_path_event,
                                    json_file=json_file_event,
                                    json_dict=dict_event
                                )

        # increment day
        year, month, day = increment_day(year, month, day, 1)

    # clean buffer
    clean_buffer(year, month, day, list_countries=LIST_COUNTRIES, n_days=n_days)

    exit()








if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    #parser.add_argument('-d', '--data_folder', help='Path folder for the data', default='data')
    parser.add_argument('-n', '--n_days', help='Number of days to populate', type=int, default=11)
    parser.add_argument('-s', '--start_date', help='Start date (YYYY_MM_DD)', type=str, default=None)
    parser.add_argument('-e', '--end_date', help='End date (YYYY_MM_DD)', type=str, default=None)
    args = parser.parse_args()

    pipeline(n_days=args.n_days, start_date=args.start_date, end_date=args.end_date)#args.data_folder)

    #TODO: Recommendation to run hisotrical data
    # --start_date <first_date> --n_days 1
    # in that way, it won't run and keep/delete unnecessary files (forecast)
