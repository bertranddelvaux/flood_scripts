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