import logging
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth

from constants.constants import GEOSERVER_WORKSPACE

# Initialize module-level logger
logger = logging.getLogger(__name__)


class GeoServerClient:
    def __init__(self, server_url: str, username: str, password: str):
        if not username or not password:
            raise ValueError("Username and password cannot be None or empty.")

        self.base_url = server_url.rstrip('/')
        self.workspace = GEOSERVER_WORKSPACE

        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)

        # Optionally test connection immediately upon creation:
        self.test_connection()

    def test_connection(self) -> None:
        """Pings GeoServer to verify server URL and credentials."""
        test_url = f"{self.base_url}/rest/about/version.json"
        response = self.session.get(test_url, timeout=5)

        # Raises HTTPError if status code is 401, 404, 500, etc.
        response.raise_for_status()

    def upload_geotiff(
            self,
            path_file: str | Path,
            sld_name: str | None = None,
            is_external: bool = False
    ) -> bool:
        """
        Uploads or references a GeoTIFF to GeoServer and optionally applies a style.

        :param is_external: If True, tells GeoServer to read a local file on its own disk.
                            If False, actually uploads the binary file over the network.
        """
        file_path = Path(path_file)
        coverage_name = file_path.stem  # Gets filename without extension

        try:
            # 1. Create Coverage Store and Upload/Link file
            if is_external:
                url = f"{self.base_url}/rest/workspaces/{self.workspace}/coveragestores/{coverage_name}/external.geotiff"
                params = {"configure": "first", "coverageName": coverage_name}
                body = f"file://{file_path.absolute()}"
                headers = {"Content-type": "text/plain"}

                response = self.session.put(url, params=params, headers=headers, data=body)
            else:
                url = f"{self.base_url}/rest/workspaces/{self.workspace}/coveragestores/{coverage_name}/file.geotiff"
                headers = {"Content-type": "image/tiff"}

                # Upload the actual binary data
                with open(file_path, "rb") as f:
                    response = self.session.put(url, headers=headers, data=f)

            # Throws an exception for 4xx and 5xx status codes
            response.raise_for_status()
            logger.info(f"Successfully created coverage store: {coverage_name}")

            # 2. Apply SLD Styling (if provided)
            if sld_name:
                self._apply_style(coverage_name, sld_name)

            return True

        except requests.HTTPError as e:
            # Dynamically extract detailed request & response information on failure only
            self._log_http_error(e, context_message=f"Uploading {coverage_name}")
        except Exception as e:
            logger.error(f"System Error handling {coverage_name}: {e}")

        return False

    def _apply_style(self, coverage_name: str, sld_name: str) -> None:
        """Internal helper to apply an SLD style to a layer."""
        url = f"{self.base_url}/rest/layers/{self.workspace}:{coverage_name}.json"
        layer_data = {
            "layer": {
                "defaultStyle": {
                    "name": f"{self.workspace}:{sld_name}"
                }
            }
        }

        response = self.session.put(url, json=layer_data)
        response.raise_for_status()
        logger.info(f"Applied style '{sld_name}' to layer '{coverage_name}'")

    def delete_geotiff(self, filename: str | Path) -> bool:
        """Deletes a coverage store and its associated layer."""
        coverage_name = Path(filename).stem
        url = f"{self.base_url}/rest/workspaces/{self.workspace}/coveragestores/{coverage_name}"

        try:
            # recurse=true deletes the layer as well as the store
            response = self.session.delete(url, params={"recurse": "true"})
            response.raise_for_status()
            logger.info(f"Successfully removed {coverage_name} from GeoServer.")
            return True

        except requests.HTTPError as e:
            self._log_http_error(e, context_message=f"Deleting {coverage_name}")
        except Exception as e:
            logger.error(f"Error removing {coverage_name}: {e}")

        return False

    def _log_http_error(self, e: requests.HTTPError, context_message: str) -> None:
        """Helper to format and print detailed HTTP error diagnostics without enabling global DEBUG mode."""
        req = e.request
        res = e.response

        error_details = (
            f"\n================ GeoServer Error ({context_message}) ================"
            f"\nStatus Code : {res.status_code} {res.reason}"
            f"\nTarget URL  : {req.method} {req.url}"
            f"\nReq Headers : {dict(req.headers)}"
            f"\nServer Resp : {res.text.strip()}"
            f"\n====================================================================="
        )
        logger.error(error_details)