import os

from constants.constants import DICT_DATA_TREE

def createFolderIfNotExists(folder) -> None:
    """
    Create a folder if it does not exist
    :param folder:
    :return:
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    return

def createDataTreeStructure(dict_data_tree=DICT_DATA_TREE) -> None:
    """
    Create the data tree structure
    :param dict_data_tree:
    :return:
    """
    for key, value in dict_data_tree.items():
        createFolderIfNotExists(key)
        if isinstance(value, dict):
            os.chdir(key)
            createDataTreeStructure(value)
            os.chdir('..')
        elif isinstance(value, list):
            for sub_folder in value:
                createFolderIfNotExists(os.path.join(key, sub_folder))
    return
