import datetime as dt

import copy

import os

from utils.json import createJSONifNotExists, jsonFileToDict, dictToJSONFile

from constants.constants import DICT_DEFAULT_VALUES, DATA_FOLDER, EVENTS_FOLDER

def initialize_event(json_path: str, json_file: str, json_dict_update: dict, ongoing_year: str = None, ongoing_month: str = None, ongoing_day: str = None) -> dict:
    """
    Initialize an event
    :param json_path:
    :param json_file:
    :param dict_default_values:
    :param ongoing_year:
    :param ongoing_month:
    :param ongoing_day:
    :return:
    """

    dict_default_values = copy.deepcopy(DICT_DEFAULT_VALUES)

    # get utc now
    utc_now = str(dt.datetime.utcnow())

    # update ongoing event
    dict_default_values['created'] = utc_now
    dict_default_values['last_edited'] = utc_now

    if all([ongoing_year, ongoing_month, ongoing_day]):
        dict_default_values['ongoing'] = True
        dict_default_values['ongoing_event_year'] = ongoing_year
        dict_default_values['ongoing_event_month'] = ongoing_month
        dict_default_values['ongoing_event_day'] = ongoing_day

    # create json at event level
    json_dict = createJSONifNotExists(
        json_path=json_path,
        json_file=json_file,
        json_dict={**dict_default_values, **json_dict_update}
    )

    return json_dict

def set_ongoing_event(json_path, json_file, ongoing: bool, ongoing_year: str = None, ongoing_month: str = None, ongoing_day: str = None) -> dict:
    """
    Set an ongoing event
    :param json_path:
    :param json_file:
    :param ongoing_year:
    :param ongoing_month:
    :param ongoing_day:
    :return:
    """

    if ongoing:
        assert all([ongoing_year, ongoing_month, ongoing_day]), 'ongoing_year, ongoing_month, ongoing_day must be provided'

    json_dict = jsonFileToDict(json_path=json_path, json_file=json_file)

    # update ongoing event
    json_dict['ongoing'] = ongoing
    json_dict['ongoing_event_year'] = ongoing_year
    json_dict['ongoing_event_month'] = ongoing_month
    json_dict['ongoing_event_day'] = ongoing_day

    # save json
    json_dict = save_json_last_edit(json_path=json_path, json_file=json_file, json_dict=json_dict)

    return json_dict

def save_json_last_edit(json_path, json_file, json_dict) -> dict:
    """
    Save last edit
    :param json_path:
    :param json_file:
    :param json_dict:
    :return:
    """
    # update last update
    json_dict['last_edited'] = str(dt.datetime.utcnow())

    # write to json
    dictToJSONFile(json_path=json_path, json_file=json_file, json_dict=json_dict)

    return json_dict

def update_event_to_jsons(event, year, country) -> tuple[dict, dict, dict]:
    """
    Update event in jsons
    :param event:
    :param year:
    :param country:
    :return:
    """

    # Getting data from tupples
    json_path_event, json_file_event, dict_event = event
    json_path_year, json_file_year, dict_year = year
    json_path_country, json_file_country, dict_country = country

    # get event information
    start_date = dict_event['start_date']
    year_ongoing = dict_event['ongoing_event_year']
    month_ongoing = dict_event['ongoing_event_month']
    day_ongoing = dict_event['ongoing_event_day']

    # adding self-contained path to dict_event, if needed
    dict_event['path'] = os.path.join(json_path_country, EVENTS_FOLDER, year_ongoing,
                             month_ongoing, day_ongoing, json_file_event)

    # updating event json
    dict_event = save_json_last_edit(
        json_path=json_path_event,
        json_file=json_file_event,
        json_dict=dict_event
    )

    # updating the event in dict_year
    dict_year['event_by_event'][start_date] = {
        'path': os.path.join(json_path_country, EVENTS_FOLDER, year_ongoing,
                             month_ongoing, day_ongoing, json_file_event),
        'event': dict_event
    }
    dict_year['total_events_year'] = len(dict_year['event_by_event'])
    dict_year['total_days_year'] = sum([events['event']['total_days_event'] for events in dict_year['event_by_event'].values()])

    # updating the event in dict_country
    dict_country['year_by_year'].setdefault(year_ongoing, {})
    dict_country['year_by_year'][year_ongoing][start_date] = dict_year['event_by_event'][start_date]

    # computing number of events and of days
    all_events = [
        data['event']['total_days_event']
        for year in dict_country.get('year_by_year', {}).values()
        for data in year.values()
        if 'event' in data
    ]
    dict_country['total_events_country'] = len(all_events)
    dict_country['total_days_country'] = sum(all_events)

    # adding/removing the ongoing event to the country dictionary of ongoing events
    if dict_country['ongoing']:
        dict_country['ongoing_event'] = dict_event
    else:
        dict_country['ongoing_event'] = {}

    # updating country json
    dict_country = save_json_last_edit(json_path_country, json_file_country, dict_country)

    # updating year json
    dict_year = save_json_last_edit(json_path_year, json_file_year, dict_year)

    return dict_event, dict_year, dict_country
