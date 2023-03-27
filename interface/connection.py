import os
import pysftp

# connection parameters
HOSTNAME = 'sftp.floodforesight.com'
USERNAME = os.environ['JBA_USERNAME']
PASSWORD = os.environ['JBA_PASSWORD']
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

