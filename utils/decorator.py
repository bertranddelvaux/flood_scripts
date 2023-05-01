import datetime as dt

from utils.files import createFolderIfNotExists, createDataTreeStructure, DICT_DATA_TREE

from utils.date import year, month, day, increment_day

def datetree(func):
    def wrapper(*args, **kwargs):
        # Make sure that the data tree structure exists
        createDataTreeStructure()

        # Get start and end dates
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)

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
            func(*args, year=year, month=month, day=day, **kwargs)

            # increment day
            year, month, day = increment_day(year, month, day, 1)
    return wrapper
