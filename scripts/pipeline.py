#####################################################
# Author: Bertrand Delvaux (2023)                   #
#                                                   #
# Script to populate ARC's Flood Explorer buffer    #
#                                                   #
#####################################################

import os
import shutil
import datetime as dt
import argparse

import pandas as pd
import numpy as np

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, \
    LIST_SUBFOLDERS_BUFFER, BUFFER_FOLDER, N_DAYS, COUNTRIES_FOLDER, HISTORICAL_STARTING_DATES

from utils.files import createFolderIfNotExists, createDataTreeStructure

from utils.date import increment_day

from utils.json import createJSONifNotExists, jsonFileToDict

from utils.event import initialize_event, set_ongoing_event, save_json_last_edit

from utils.tif import tifs_2_tif_depth, tif_2_array, reproject_and_maximize_tifs, merge_tifs

from utils.stats import array_2_stats

from utils.sftp import download_pipeline

from utils.csv2geojson import csv2geojson

from utils.string_format import colorize_text

from utils.dataframe import sum_list_dict


def clean_buffer_impacts(year: str, month: str, day: str, list_countries: list[str] = LIST_COUNTRIES,
                         n_days: int = N_DAYS) -> None:
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

        # clean buffer folder
        path = os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER)
        if os.path.exists(path):
            for file in os.listdir(path):
                try:
                    # extract the 8 characters of file name after 'rd' and after 'fe'
                    rd = file.split('rd')[1][:8]
                    fe = file.split('fe')[1][:8]

                    # get the latest date to keep
                    year_last, month_last, day_last = increment_day(year, month, day, -n_days)

                    if fe > rd or rd < f'{year_last}{month_last}{day_last}':
                        os.remove(os.path.join(path, file))
                except IndexError:
                    print(f'File {file} does not have the right format')

        # clean impacts folder
        path = os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER)
        if os.path.exists(path):
            for file in os.listdir(path):
                # extract the 8 characters of file name after 'rd'
                rd = file.split('rd')[1][:8]

                # get the latest date to keep
                year_last, month_last, day_last = increment_day(year, month, day, -n_days)

                if rd < f'{year_last}{month_last}{day_last}':
                    os.remove(os.path.join(path, file))


def process_files_include_exclude(include_str_list: list[str], exclude_str_list: list[str], buffer_path: str,
                                  postfix: str = '_depth.tif',
                                  n_bands: int = 211, threshold: float = 0.8, to_epsg_3857: bool = True) -> tuple[bool, bool, tuple]:
    """
    Process files in buffer folder
    :param include_str_list:
    :param exclude_str_list:
    :param buffer_path:
    :param postfix:
    :param n_bands:
    :param threshold:
    :return:
    """

    # get list of files
    list_files = [tif for tif in os.listdir(buffer_path) if
                  all(include_str in tif for include_str in include_str_list) and include_str_list and not any(
                      exclude_str in tif for exclude_str in exclude_str_list)]

    # check if list is empty
    if len(list_files) == 0:
        raise ValueError(f'No files found in buffer folder containing {", ".join(include_str_list)}')

    # process depth map
    raster_depth_file, empty, bbox = tifs_2_tif_depth(folder_path=buffer_path, tifs_list=list_files, postfix=postfix,
                                                n_bands=n_bands, threshold=threshold, to_epsg_3857=to_epsg_3857)
    success = True

    # remove files from temp folder
    for file in list_files:
        os.remove(os.path.join(buffer_path, file))

    print(f'\t\t\tCreated depth map: \033[32m{raster_depth_file}\033[0m ', end='')

    return success, empty, bbox


def process_pipeline(start_date: str = None, end_date: str = None, n_days: int = N_DAYS,
                     list_countries: list[str] = LIST_COUNTRIES, to_epsg_3857: bool = True) -> None:
    """
    Pipeline to populate ARC's Flood Explorer buffer
    :param start_date:
    :param end_date:
    :param n_days:
    :param list_countries:
    :return:
    """

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
    while (dt.datetime(int(year_end), int(month_end), int(day_end)) - dt.datetime(int(year), int(month),
                                                                                  int(day))).days >= 0:

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
                    'year_by_year': {}
                }
            )

            for sub_folder in LIST_SUBFOLDERS_BUFFER:
                print(f'\tProcessing {sub_folder} data...')

                if sub_folder == IMPACTS_FOLDER:
                    folder_path = os.path.join(DATA_FOLDER, country, sub_folder)
                    csv_file = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if
                                f'rd{year}{month}{day}' in f and f.endswith('.csv') and not f.endswith(
                                    '_processed.csv')][0]
                    print(f'\t\tProcessing {csv_file} ... ', end='')
                    merged_adm0, merged_adm1, merged_adm2 = csv2geojson(
                        csv_file=csv_file,
                        shp_file=os.path.join(COUNTRIES_FOLDER, f'{country}_adm_shapefile.zip'),
                        output_file=csv_file.replace('.csv', '.geojson'),
                        to_epsg_3857=to_epsg_3857,
                    )
                    print(f'\033[32m' + '✔' + '\033[0m')

                elif sub_folder == RASTER_FOLDER:
                    tmp_path = os.path.join(DATA_FOLDER, country, sub_folder, BUFFER_FOLDER)
                    createFolderIfNotExists(tmp_path)

                    for i_day in range(0, n_days):
                        year_n, month_n, day_n = increment_day(year, month, day, i_day)

                        # print day
                        print(f'\t\tProcessing \033[1mday {i_day}\033[0m : ({year_n}-{month_n}-{day_n}) ... ')

                        # create depth map
                        success, empty, bbox = process_files_include_exclude(
                            include_str_list=[f'fe{year_n}{month_n}{day_n}', f'rd{year}{month}{day}'],
                            exclude_str_list=['Agreement', '_depth'],
                            buffer_path=tmp_path,
                            postfix='_depth.tif',
                            n_bands=211,
                            threshold=0.8,
                            to_epsg_3857=to_epsg_3857,
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
                                    'event_by_event': {}
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
                                    print(
                                        f'\t\t\t\t\033[95mClosing ongoing event that started on {year_ongoing:04}_{month_ongoing:02}_{day_ongoing:02}... \033[0m')
                                    dict_country = set_ongoing_event(json_path_country, json_file_country, False)
                                    dict_year = set_ongoing_event(json_path_year, json_file_year, False)

                                    # update jsons
                                    # TODO: this is where country and year jsons are incremented

                                    # get event start date
                                    start_date = dict_event['start_date']

                                    dict_year['total_events_year'] += 1
                                    dict_year['total_days_year'] += dict_event['total_days_event']
                                    dict_year['event_by_event'][start_date] = {
                                        'path': os.path.join(DATA_FOLDER, country, EVENTS_FOLDER, year_ongoing, month_ongoing, day_ongoing, json_file_event),
                                        'event': dict_event
                                    }

                                    dict_country['total_events_country'] += 1
                                    dict_country['total_days_country'] += dict_event['total_days_event']
                                    dict_country['year_by_year'].setdefault(year_ongoing, {})
                                    dict_country['year_by_year'][year_ongoing][start_date] = dict_year['event_by_event'][start_date]

                                    # TODO: pick up the biggest numbers from the ongoing event and put them in the peak event: flooded area, flooded population, losses, severity_index

                                    dict_country = save_json_last_edit(json_path_country, json_file_country,
                                                                       dict_country)
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
                                    print(
                                        f'\t\t\t\t\033[95mOpening new event on {year_n:04}_{month_n:02}_{day_n:02}... \033[0m')

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
                                            # 'ongoing': True,
                                            'start_date': f'{year_n:04}_{month_n:02}_{day_n:02}',
                                            'total_days_event': 0,
                                            'day_by_day': [],  # TODO: add the first day
                                            # 'stats': {}, #TODO: initialize with the stats of the first day
                                            'peak_flood': None,
                                            'peak_population': None,
                                            'peak_losses': None,
                                            'adm0_max': None,
                                            'adm1_max': None,
                                            'adm2_max': None,
                                            # TODO: peak day is the day with the highest stats, so the day of the creation, then the day with the highest stats
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

                                # print(f'\t\t\t\tOngoing event: {year_ongoing:04}_{month_ongoing:02}_{day_ongoing:02}')

                                # get the json event of the ongoing event
                                json_path_event = os.path.join(DATA_FOLDER, country, EVENTS_FOLDER,
                                                               year_ongoing, month_ongoing, day_ongoing)
                                json_file_event = f'{year_ongoing}_{month_ongoing}_{day_ongoing}.json'
                                dict_event = jsonFileToDict(json_path_event, json_file_event)

                                ## Copy files

                                # copy the depth file
                                depth_file_buffer = \
                                    [os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER, f) for f in
                                     os.listdir(os.path.join(DATA_FOLDER, country, RASTER_FOLDER, BUFFER_FOLDER)) if
                                     f'rd{year}{month}{day}' in f and 'depth.tif' in f][0]
                                depth_file = os.path.join(json_path_event, os.path.basename(depth_file_buffer))
                                shutil.copy(depth_file_buffer, depth_file)
                                print(f'\t\t\t\t\033[34mCopied {os.path.basename(depth_file)}... \033[0m')

                                # if a file named f'{year_ongoing}_{month_ongoing}_{day_ongoing}_max_depth.tif' does not exists, copy the only depth file and rename it, if not, call reproject_and_maximize_tifs with the two files
                                max_depth_file = os.path.join(json_path_event, f'{country}_{year_ongoing}_{month_ongoing}_{day_ongoing}_max_depth.tif')
                                if not os.path.exists(max_depth_file):
                                    shutil.copy(depth_file, max_depth_file)
                                    bbox_max = bbox
                                    print(f'\t\t\t\t\033[34mCreated {os.path.basename(max_depth_file)}... \033[0m')
                                else:
                                    # reproject and maximize the two raster files
                                    #bbox_max = reproject_and_maximize_tifs(tifs_list=[max_depth_file, depth_file], output_file=max_depth_file)
                                    bbox_max = merge_tifs(tifs_list=[max_depth_file, depth_file], output_file=max_depth_file)
                                    print(f'\t\t\t\t\033[34mUpdated {os.path.basename(max_depth_file)}... \033[0m')

                                # copy the impact file
                                impact_files = [os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER, f) for f in
                                                os.listdir(os.path.join(DATA_FOLDER, country, IMPACTS_FOLDER)) if
                                                f'rd{year}{month}{day}' in f and '.csv' in f or '.geojson' in f or '.tif' in f]
                                for impact_file in impact_files:
                                    shutil.copy(impact_file,
                                                os.path.join(json_path_event, os.path.basename(impact_file)))
                                    print(f'\t\t\t\t\033[34mCopied {os.path.basename(impact_file)}... \033[0m')

                                # update ongoing event
                                print('\t\t\tUpdating ongoing event... ')
                                # update the json event of the ongoing event

                                # open the raster file
                                array, meta = tif_2_array(depth_file)
                                # get the stats
                                stats = array_2_stats(
                                    array=array,
                                    pixel_size_x_m=meta['transform'].a,
                                    pixel_size_y_m=meta['transform'].e
                                )

                                # adm breakdown
                                adm0 = merged_adm0.to_dict(orient='records')
                                adm1 = merged_adm1.to_dict(orient='records')
                                adm2 = merged_adm2.to_dict(orient='records')

                                # update the json event of the ongoing event
                                dict_event['total_days_event'] += 1
                                dict_event['max_depth_file'] = os.path.basename(max_depth_file)
                                dict_event['day_by_day'].append({
                                    'day': dict_event['total_days_event'],
                                    'map': os.path.basename(depth_file),
                                    'bbox': bbox,
                                    'stats': stats,
                                    'adm0': adm0,
                                    'adm1': adm1,
                                    'adm2': adm2,
                                })
                                dict_event['bbox_max'] = bbox_max

                                if dict_event['total_days_event'] == 1:
                                    dict_event['peak_flood'] = {
                                        'day': dict_event['total_days_event'],
                                        'stats': stats
                                    }
                                    dict_event['peak_population'] = {
                                        'day': dict_event['total_days_event'],
                                        'population': merged_adm0.to_dict(orient='records')
                                    }
                                    dict_event['stats'] = stats
                                    dict_event['adm0_max'] = merged_adm0.to_dict(orient='records')
                                    dict_event['adm1_max'] = merged_adm1.to_dict(orient='records')
                                    dict_event['adm2_max'] = merged_adm2.to_dict(orient='records')
                                else:
                                    # update the stats of the event: take the maximum value of each stat
                                    for stat in dict_event['stats']:
                                        dict_event['stats'][stat] = max(dict_event['stats'][stat], stats[stat])
                                    # dict_event['stats'] = {**dict_event['stats'],
                                    # **stats}
                                    if stats['severity_index_1m'] > dict_event['stats']['severity_index_1m']:
                                        dict_event['peak_flood'] = {
                                            'day': dict_event['total_days_event'],
                                            'stats': stats
                                        }
                                    # if the sum of the numerical values of adm0 is greater than the previous peak
                                    if sum_list_dict(adm0) > sum_list_dict(dict_event['peak_population']['population']) : #TODO!!!!!!!
                                        dict_event['peak_population'] = {
                                            'day': dict_event['total_days_event'],
                                            'population': adm0
                                        }


                                    dict_event['adm0_max'] = np.maximum(merged_adm0, pd.DataFrame.from_records(dict_event['adm0_max'])).to_dict(orient='records')
                                    dict_event['adm1_max'] = np.maximum(merged_adm1, pd.DataFrame.from_records(
                                        dict_event['adm1_max'])).to_dict(orient='records')
                                    dict_event['adm1_max'] = np.maximum(merged_adm1, pd.DataFrame.from_records(
                                        dict_event['adm1_max'])).to_dict(orient='records')

                                dict_event = save_json_last_edit(
                                    json_path=json_path_event,
                                    json_file=json_file_event,
                                    json_dict=dict_event
                                )

        # increment day
        year, month, day = increment_day(year, month, day, 1)

    # clean buffer
    print('\t\t\tCleaning buffer...')
    clean_buffer_impacts(year, month, day, list_countries=LIST_COUNTRIES, n_days=n_days)

def process_pipeline_historic(start_date: str = None, end_date: str = None,
                     list_countries: list[str] = LIST_COUNTRIES, to_epsg_3857: bool = True) -> None:
    """
    Process the pipeline for historic data
    :param start_date:
    :param end_date:
    :param n_days:
    :param list_countries:
    :param to_epsg_3857:
    :return:
    """

    n_days = 1  # so that the pipeline does not keep forecasts from the past

    for country in list_countries:
        # try:
        json_path = os.path.join(DATA_FOLDER, country)
        json_file = 'latest_date.json'
        path_latest_date = os.path.join(DATA_FOLDER, country, 'latest_date.json')

        # allow to start the historic data collection from a specific date
        if start_date is not None:
            latest_date = [start_date]
        else:
            latest_date = []

        json_dict = {
            'latest_date': latest_date
        }

        json_dict = createJSONifNotExists(
            json_path=json_path,
            json_file=json_file,
            json_dict=json_dict
        )

        if json_dict['latest_date'] == []:
            latest_date = None
            print(f'\n\033[95mNo files in buffer folder\033[0m')

            start_date = HISTORICAL_STARTING_DATES[country]  # first date of data collection from JBA's sftp
            end_date = HISTORICAL_STARTING_DATES[country]

        else:

            latest_date = json_dict['latest_date'][0]
            print(f'\n\033[95mLatest date in buffer folder: {latest_date}\033[0m')

            # download data from sftp and run pipeline from the next day, for 1 day and 1 day of forecast (n_days=1)
            year, month, day = latest_date.split('_')
            if start_date:
                inc_days = 0
            else:
                inc_days = 1
            year_n, month_n, day_n = increment_day(year, month, day, inc_days)
            start_date = f'{year_n}_{month_n}_{day_n}'
            end_date = f'{year_n}_{month_n}_{day_n}'

        download_pipeline(start_date=start_date, end_date=end_date, n_days=n_days,
                          list_countries=[country], include_str='ens00')  # according to JBA, the first day of forecast, all ensembles are the same

        # pipeline to process data
        process_pipeline(start_date=start_date, end_date=end_date,
                         n_days=n_days, list_countries=[country])

        # update json latest date
        json_dict['latest_date'].insert(0, end_date)

        # update json last edited
        json_dict = save_json_last_edit(
            json_path=json_path,
            json_file=json_file,
            json_dict=json_dict
        )
        # except:
        # print(f'{colorize_text("Error in historic data", "red")}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Populate the buffer folder with the latest data from JBA\'s server')
    parser.add_argument('-n', '--n_days', help='Number of days of forecast to populate', type=int, default=11)
    parser.add_argument('-s', '--start_date', help='Start date (YYYY_MM_DD)', type=str, default=None)
    parser.add_argument('-e', '--end_date', help='End date (YYYY_MM_DD)', type=str, default=None)
    parser.add_argument('-c', '--list_countries', help='List of countries to populate', type=str, nargs='+',
                        default=LIST_COUNTRIES)
    parser.add_argument('-hist', '--historic', help='Run historic data', action='store_true', default=False)
    parser.add_argument('-to_now', '--to_now', help='Run historic data to now', action='store_true', default=False)
    args = parser.parse_args()

    if not args.historic:
        process_pipeline(start_date=args.start_date, end_date=args.end_date, n_days=args.n_days, list_countries=args.list_countries)
    else:
        if args.to_now:
            # calculate the number of days to run the historic data to now
            today = dt.datetime.now()
            start_date = dt.datetime.strptime(args.start_date, '%Y_%m_%d')
            n_days_to_run = (today - start_date).days
        else:
            n_days_to_run = 1

        process_pipeline_historic(start_date=args.start_date, end_date=args.end_date, list_countries=args.list_countries)

        for i in range(n_days_to_run-1):
            process_pipeline_historic(list_countries=args.list_countries)
