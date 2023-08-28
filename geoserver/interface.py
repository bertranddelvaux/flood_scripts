import os

from constants.constants import GEOSERVER_WORKSPACE

def uploadToGeoserver(
        path_file: str,
        username: str,
        password: str,
        server: str,
        workspace: str = GEOSERVER_WORKSPACE,
):
    """
    Upload a file to Geoserver
    :param path_file:
    :param filename:
    :return:
    """

    success = False

    try:

        # get filename
        filename = os.path.basename(path_file)

        # get coverage name
        coverage_name = os.path.splitext(os.path.basename(filename))[0]

        # Initiate cURL session
        service = server  # replace with your URL
        request = f'rest/workspaces/{workspace}/coveragestores/{coverage_name}/external.geotiff?configure=first&coverageName={coverage_name}'
        url = service + request

        # Required PUT request settings
        passwordStr = f'{username}:{password}'  # replace with your username:password

        # CURL command
        curl_command = f'curl -v -s -u {passwordStr} -XPUT -H "Content-type: text/plain" -d "file://{path_file}" \'{url}\''

        print(curl_command)
        os.system(curl_command)

        layername = coverage_name
        sld_name = 	'flood_depth_jba'

        service = server  # replace with your URL
        request = f'rest/layers/{workspace}:{layername}'
        url = service + request

        layer_styling = f'<layer><defaultStyle><name>{workspace}:{sld_name}</name></defaultStyle></layer>'
        curl_command = f'curl -v -s -u {passwordStr} -XPUT -H "Content-type: text/xml" -d "{layer_styling}" \'{url}\''

        os.system(curl_command)

        success = True

    except:
        print(f'Error uploading {path_file} to Geoserver')

    return success

def deleteFromGeoserver(
        filename: str,
        username: str,
        password: str,
        server: str,
        workspace: str = GEOSERVER_WORKSPACE,
):
    """
    Delete a file from Geoserver
    :param filename:
    :return:
    """

    success = False

    try:

        # get coverage name
        coverage_name = os.path.splitext(os.path.basename(filename))[0]

        # Initiate cURL session
        service = server  # replace with your URL
        request = f'rest/workspaces/{workspace}/coveragestores/{coverage_name}'
        url = service + request

        # Required PUT request settings
        passwordStr = f'{username}:{password}'  # replace with your username:password

        # CURL command
        curl_command = f'curl -v -s -u {passwordStr} -XDELETE \'{url}\''

        print(curl_command)
        os.system(curl_command)

        success = True

    except:
        print(f'Error removing {filename} from Geoserver')

    return success