import os
import pysftp
import json
from datetime import datetime, timedelta

from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, BUFFER_FOLDER, N_DAYS

from utils.decorator import datetree

from utils.files import createFolderIfNotExists

from utils.date import increment_day

from utils.string_format import colorize_text

@datetree
def download_pipeline(
        year,
        month,
        day,
        start_date: str = None,
        end_date: str = None,
        n_days: int = N_DAYS,
        list_countries: list[str] = LIST_COUNTRIES,
        exclude_str: str ='Agreement',
        include_str: str = '',
) -> None:
    """
    Download data from JBA's sftp server for a given date and a given list of countries
    :param year:
    :param month:
    :param day:
    :param start_date:
    :param end_date:
    :param n_days:
    :param list_countries:
    :param exclude_str:
    :return:
    """

    # date message
    date_msg = f'{year}_{month}_{day}'
    print(colorize_text(f'\n{date_msg}\n{"*" * len(date_msg)}\n', 'bold'))

    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD, cnopts=cnopts) as sftp:

        for country in list_countries:
            print(f'Fetching data for {country}...')

            for sub_folder in LIST_SUBFOLDERS_BUFFER:
                print(f'\tFetching {sub_folder} data...')

                path_sftp = os.path.join(country, sub_folder, year, month, day)

                if sub_folder == IMPACTS_FOLDER:
                    path = os.path.join(DATA_FOLDER, country, sub_folder)
                    sftp.get_d(path_sftp, path, preserve_mtime=False)

                elif sub_folder == RASTER_FOLDER:
                    buffer_path = os.path.join(DATA_FOLDER, country, sub_folder, BUFFER_FOLDER)
                    createFolderIfNotExists(buffer_path)

                    for i_day in range(0, n_days):
                        year_n, month_n, day_n = increment_day(year, month, day, i_day)

                        # include string
                        include_str_day = f'fe{year_n}{month_n}{day_n}'

                        # get list of files
                        list_files = [tif.filename for tif in sftp.listdir_attr(path_sftp) if
                                      include_str_day in tif.filename and include_str in tif.filename and exclude_str not in tif.filename]

                        # download files to temp folder
                        print(f'\t\t\tDownloading {len(list_files)} from the sftp server ({colorize_text(include_str_day, "bold")}) ... ', end='')
                        try:
                            for i, file in enumerate(list_files):
                                sftp.get(os.path.join(path_sftp, file), os.path.join(buffer_path, file))
                            print(colorize_text('✔', 'green'))
                        except:
                            print(colorize_text('✘', 'red'))


def generate_json_missing_data(json_file_path):
    report = {"countries": {}, "grand_total_missing": 0}

    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD, cnopts=cnopts) as sftp:
        countries = [f for f in sftp.listdir() if sftp.isdir(f)]

        for country in countries:
            print(f'Dealing with {country} ...')
            raster_base = f"{country}/raster"
            if not sftp.exists(raster_base):
                print(f'\tCould not find "raster" folder inside {country}')
                continue

            start_date = find_earliest_date(sftp, raster_base)
            if not start_date:
                print(f'\tCould not find a starting date for {country}')
                continue

            report["countries"][country] = {
                "continuous_data_from": None,
                "total_missing_days": 0,
                "years": {}
            }

            # This variable tracks the start of the current continuous streak
            streak_start = start_date
            today = datetime.now().date()
            current_date = start_date

            while current_date <= today:
                y = current_date.strftime("%Y")
                m = current_date.strftime("%m")
                d = current_date.strftime("%d")
                day_path = f"{raster_base}/{y}/{m}/{d}"

                if not sftp.exists(day_path):
                    # 1. Log Missing Data
                    if y not in report["countries"][country]["years"]:
                        report["countries"][country]["years"][y] = {"missing_count": 0, "months": {}}
                    if m not in report["countries"][country]["years"][y]["months"]:
                        report["countries"][country]["years"][y]["months"][m] = {"missing_count": 0, "days": []}

                    report["countries"][country]["years"][y]["months"][m]["days"].append(d)
                    report["countries"][country]["years"][y]["months"][m]["missing_count"] += 1
                    report["countries"][country]["years"][y]["missing_count"] += 1
                    report["countries"][country]["total_missing_days"] += 1
                    report["grand_total_missing"] += 1

                    # 2. Reset the continuous streak start to the day after the gap
                    streak_start = current_date + timedelta(days=1)

                current_date += timedelta(days=1)

            # Final check: if streak_start is in the future, it means today was missing
            if streak_start > today:
                report["countries"][country]["continuous_data_from"] = "No current continuous data"
            else:
                report["countries"][country]["continuous_data_from"] = streak_start.strftime("%Y-%m-%d")

    with open(json_file_path, 'w') as f:
        json.dump(report, f, indent=4)

    return report


def find_earliest_date(sftp, base_path):
    """Helper to find the first available YYYY/MM/DD folder."""
    try:
        years = sorted([y for y in sftp.listdir(base_path) if y.isdigit()])
        if not years: return None
        first_year = years[0]
        months = sorted([m for m in sftp.listdir(f"{base_path}/{first_year}") if m.isdigit()])
        if not months: return None
        for month in months:
            if len(sftp.listdir(f"{base_path}/{first_year}/{month}")) > 0:
                first_month = month
                break
        # first_month = months[0]
        days = sorted([d for d in sftp.listdir(f"{base_path}/{first_year}/{first_month}") if d.isdigit()])
        if not days: return None
        return datetime(int(first_year), int(first_month), int(days[0])).date()
    except Exception:
        return None