import os
import requests
from constants.constants import GEOSERVER_WORKSPACE


def uploadToGeoserver(
        path_file: str,
        username: str,
        password: str,
        server: str,
        workspace: str = GEOSERVER_WORKSPACE,
):
    """
    Upload a file to Geoserver using REST JSON API and requests library
    """
    success = False
    filename = os.path.basename(path_file)
    coverage_name = os.path.splitext(filename)[0]

    # Ensure server URL ends with a slash
    base_url = server.rstrip('/')

    try:
        # 1. Create Coverage Store and Upload file
        # Using external.geotiff to reference the file path
        upload_url = f"{base_url}/rest/workspaces/{workspace}/coveragestores/{coverage_name}/external.geotiff"
        params = {
            "configure": "first",
            "coverageName": coverage_name
        }

        # Geoserver expects the absolute path as a plain text string for 'external.geotiff'
        file_path_body = f"file://{os.path.abspath(path_file)}"

        response = requests.put(
            upload_url,
            auth=(username, password),
            params=params,
            headers={'Content-type': 'text/plain'},
            data=file_path_body
        )

        if response.status_code not in [200, 201]:
            print(f"Failed to upload coverage: {response.text}")
            return False

        # 2. Apply SLD Styling to the Layer
        layer_url = f"{base_url}/rest/layers/{workspace}:{coverage_name}.json"
        sld_name = 'flood_depth_jba'

        layer_data = {
            "layer": {
                "defaultStyle": {
                    "name": f"{workspace}:{sld_name}"
                }
            }
        }

        response_style = requests.put(
            layer_url,
            auth=(username, password),
            json=layer_data
        )

        if response_style.status_code in [200, 201]:
            success = True
        else:
            print(f"Failed to apply style: {response_style.text}")

    except Exception as e:
        print(f"Error uploading {path_file} to Geoserver: {e}")

    return success


def deleteFromGeoserver(
        filename: str,
        username: str,
        password: str,
        server: str,
        workspace: str = GEOSERVER_WORKSPACE,
):
    """
    Delete a coverage store from Geoserver using REST API
    """
    success = False
    coverage_name = os.path.splitext(os.path.basename(filename))[0]
    base_url = server.rstrip('/')

    try:
        # recurse=true deletes the associated layer as well
        delete_url = f"{base_url}/rest/workspaces/{workspace}/coveragestores/{coverage_name}?recurse=true"

        response = requests.delete(
            delete_url,
            auth=(username, password)
        )

        if response.status_code in [200, 204]:
            success = True
    except Exception as e:
        print(f"Error removing {filename} from Geoserver: {e}")

    return success