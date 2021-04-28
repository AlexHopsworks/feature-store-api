#
#   Copyright 2021 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hsfs import client


class ProvenanceApi:
    def __init__(self):
        """Provenance endpoint for `trainingdatasets` and `featuregroups` resource."""

    def app_links(
        self,
        in_artifact_type: str = None,
        in_artifact_name: str = None,
        in_artifact_version: int = None,
        out_artifact_type: str = None,
        out_artifact_name: str = None,
        out_artifact_version: int = None,
    ):

        _client = client.get_instance()
        path_params = ["project", _client._project_id, "provenance", "links"]
        query_params = {"only_apps": "true", "full_link": "true", "filter_by": []}
        in_query = False
        if in_artifact_type is not None:
            query_params["filter_by"].append("IN_TYPE:" + in_artifact_type)
        if in_artifact_name is not None and in_artifact_version is not None:
            query_params["filter_by"].append(
                "IN_ARTIFACT:" + in_artifact_name + "_" + str(in_artifact_version)
            )
        if out_artifact_type is not None:
            query_params["filter_by"].append("OUT_TYPE:" + out_artifact_type)
        if out_artifact_name is not None and out_artifact_version is not None:
            query_params["filter_by"].append(
                "OUT_ARTIFACT:" + out_artifact_name + "_" + str(out_artifact_version)
            )
            in_query = True

        headers = {"content-type": "application/json"}
        json_dict = _client._send_request(
            "GET", path_params, query_params, headers=headers
        )

        result = []
        if "items" in json_dict:
            for item in json_dict["items"]:
                if in_query:
                    values = item["in"]["entry"]
                else:
                    values = item["out"]["entry"]
                for i in values:
                    key_split = i["key"].rsplit("_", 1)
                    result.append(
                        {
                            "name": key_split[0],
                            "version": key_split[1],
                            "project": i["value"]["projectName"],
                            "app_id": i["value"]["appId"],
                        }
                    )

        return result
