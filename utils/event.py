import datetime as dt

import copy

from utils.json import createJSONifNotExists, jsonFileToDict, dictToJSONFile

from constants.constants import DICT_DEFAULT_VALUES

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
