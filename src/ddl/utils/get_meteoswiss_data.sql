USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

CREATE OR REPLACE FUNCTION utils.get_meteoswiss_data(url VARCHAR)
RETURNS TABLE (json VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.13
HANDLER = 'MeteoSwissApiData'
EXTERNAL_ACCESS_INTEGRATIONS = (meteoswiss_integration)
PACKAGES = ('requests')
AS
$$
import requests
import json
import time

class MeteoSwissApiData:
    def process(self, url):
        headers = {
            'content-type': "application/json",
            'User-Agent': "Snowflake-MeteoSwiss-Integration/1.0"
        }
        start_time = time.time()
        try:
            response = requests.get(url, headers=headers, timeout=30)
            elapsed_time = time.time() - start_time

            metadata = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                "response_time": elapsed_time,
                "status_code": response.status_code,
                "url": url,
                "data_source": "MeteoSwiss"
            }

            if response.status_code == 200:
                data = response.json()

                # MeteoSwiss data structure: {"features": [...]}
                if isinstance(data, dict) and 'features' in data:
                    features = data['features']
                    for feature in features:
                        # Add metadata to each feature
                        feature["_metadata"] = metadata
                        yield (json.dumps(feature),)
                else:
                    # Handle other data structures
                    data["_metadata"] = metadata
                    yield (json.dumps(data),)
            else:
                # Return error information
                error_data = {
                    "_metadata": metadata,
                    "error": f"HTTP {response.status_code}: {response.text}"
                }
                yield (json.dumps(error_data),)

        except Exception as e:
            elapsed_time = time.time() - start_time
            error_metadata = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                "response_time": elapsed_time,
                "status_code": None,
                "url": url,
                "data_source": "MeteoSwiss",
                "error": str(e)
            }
            yield (json.dumps({"_metadata": error_metadata}),)
$$;