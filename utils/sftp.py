import os
import pysftp

from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

from constants.constants import DATA_FOLDER, RASTER_FOLDER, IMPACTS_FOLDER, EVENTS_FOLDER, LIST_COUNTRIES, LIST_SUBFOLDERS_BUFFER, BUFFER_FOLDER, N_DAYS

from utils.decorator import datetree

from utils.files import createFolderIfNotExists

from utils.date import increment_day

from utils.string_format import colorize_text

@datetree
def download_data_from_sftp(
        year,
        month,
        day,
        start_date: str = None,
        end_date: str = None,
        n_days: int = N_DAYS,
        list_countries: list[str] = LIST_COUNTRIES,
        exclude_str: str ='Agreement'
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
                        include_str = f'fe{year_n}{month_n}{day_n}'

                        # get list of files
                        list_files = [tif.filename for tif in sftp.listdir_attr(path_sftp) if
                                      include_str in tif.filename and exclude_str not in tif.filename]

                        # download files to temp folder
                        print(f'\t\t\tDownloading {len(list_files)} from the sftp server ({colorize_text(include_str, "bold")}) ... ', end='')
                        try:
                            for i, file in enumerate(list_files):
                                sftp.get(os.path.join(path_sftp, file), os.path.join(buffer_path, file))
                            print(colorize_text('✔', 'green'))
                        except:
                            print(colorize_text('✘', 'red'))