import os

def createFolderIfNotExists(folder) -> None:
    """
    Create a folder if it does not exist
    :param folder:
    :return:
    """
    if not os.path.exists(folder):
        os.makedirs(folder)
    return
