# directory structure
DATA_FOLDER = 'data'
LIST_COUNTRIES = ['civ', 'mdg', 'moz', 'mwi', 'tgo'] #TODO: there is an issue for 'gha'
IMPACTS_FOLDER = 'impacts'
RASTER_FOLDER = 'raster'
EVENTS_FOLDER = 'events'

# directory structure inferrence
LIST_SUBFOLDERS_BUFFER = [IMPACTS_FOLDER, RASTER_FOLDER]
LIST_SUBFOLDERS_EVENTS = [EVENTS_FOLDER]
LIST_SUBFOLDERS = list(set(LIST_SUBFOLDERS_BUFFER + LIST_SUBFOLDERS_EVENTS))
DICT_DATA_TREE = {DATA_FOLDER: {country: LIST_SUBFOLDERS for country in LIST_COUNTRIES}}

# buffer folder
BUFFER_FOLDER = 'buffer'

# days of forecast
N_DAYS = 11

# number of bands
N_BANDS = 211

# threshold

# max number of days between 2 successive "peaks" of the flood (triggered when above 0.2m)
N_DAYS_SINCE_LAST_THRESHOLD = 3

# band
TRIGGER_BAND_VALUE = 5  # 0.2m

# default dict values
DICT_DEFAULT_VALUES = {
    'created': None,
    'last_edited': None,
    'ongoing': False,
    'ongoing_event_year': None,
    'ongoing_event_month': None,
    'ongoing_event_day': None,
    'stats': {}
}

# Countries folder
COUNTRIES_FOLDER = 'countries'

# Historical starting dates
HISTORICAL_STARTING_DATES = {
    'civ': '2022_08_18',
    'mdg': '2022_07_05',
    'moz': '2022_04_28',
    'mwi': '2022_07_05',
    'tgo': '2022_04_28'
}

# TIF resolution
TIF_RESOLUTION = 0.00027777777999999997

# GeoServer constants
GEOSERVER_WORKSPACE = 'flood_foresight'
