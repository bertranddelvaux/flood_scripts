# directory structure
DATA_FOLDER = 'data'
LIST_COUNTRIES = ['moz', 'civ', 'gha', 'mdg', 'mwi', 'tgo']
IMPACTS_FOLDER = 'impacts'
RASTER_FOLDER = 'raster'
EVENTS_FOLDER = 'events'

# directory structure inferrence
LIST_SUBFOLDERS_BUFFER = [IMPACTS_FOLDER, RASTER_FOLDER]
LIST_SUBFOLDERS_EVENTS = [EVENTS_FOLDER]
LIST_SUBFOLDERS = list(set(LIST_SUBFOLDERS_BUFFER + LIST_SUBFOLDERS_EVENTS))
DICT_DATA_TREE = {DATA_FOLDER: {country: LIST_SUBFOLDERS for country in LIST_COUNTRIES}}

# temporary folder
TMP_FOLDER = 'tmp'