#####################################################
# Author: Bertrand Delvaux (2023)                   #
#                                                   #
# Script to populate ARC's Flood Explorer buffer    #
#                                                   #
#####################################################

import os
import pysftp
import datetime as dt

from interface.connection import HOSTNAME, USERNAME, PASSWORD, cnopts

# constants
LIST_COUNTRIES = ['civ','gha','mdg','moz','mwi','tgo']
LIST_SUBFOLDERS = ['impacts','raster','events']

# connection parameters


