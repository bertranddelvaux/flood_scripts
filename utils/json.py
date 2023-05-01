import os
import json

def createJSONifNotExists(json_path: str, json_file: str, json_dict: dict) -> dict:
    """
    Create a JSON file if it does not exist
    :param json_path:
    :param json_file:
    :return:
    """
    path = os.path.join(json_path, json_file)
    if not os.path.exists(json_path):
        print(f'\t\t\t\t\033[34mCreating folder {json_path}...\033[0m')
        os.makedirs(json_path, exist_ok=True)
    if not os.path.exists(path):
        with open(path, 'w') as fp:
            json.dump(json_dict, fp)
            print(f'\t\t\t\t\033[34mCreating file {json_file}...\033[0m')
            return json_dict
    else:
        with open(path, 'r') as fp:
            json_dict = json.load(fp)
            return json_dict

def jsonFileToDict(json_path: str, json_file: str) -> dict:
    """
    Get a JSON file and return a dictionary
    :param json_path:
    :param json_file:
    :return:
    """
    path = os.path.join(json_path, json_file)
    with open(path, 'r') as fp:
        return json.load(fp)

def dictToJSONFile(json_path: str, json_file: str, json_dict: dict) -> None:
    """
    Get a dictionary and write it to a JSON file
    :param json_path:
    :param json_file:
    :param json_dict:
    :return:
    """
    path = os.path.join(json_path, json_file)
    with open(path, 'w') as fp:
        json.dump(json_dict, fp)