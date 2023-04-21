import os
import json

def createJSONifNotExists(json_path: str, json_file: str, json_dict: dict) -> None:
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
    with open(path, 'w') as fp:
        json.dump(json_dict, fp)
        print(f'\t\t\t\t\033[34mCreating file {json_file}...\033[0m')