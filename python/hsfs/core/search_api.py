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


class SearchApi:
    def __init__(self):
        """Search endpoint for `trainingdatasets` and `featuregroups` resource."""

    def search(
        self,
        type: str,
        term: str = None,
        name: str = None,
        description: str = None,
        tags: str = None,
        offset: int = 0,
        limit: int = 100,
    ):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "elastic",
            "fs",
        ]
        query_params = {"docType": type, "from": offset, "size": limit, "filter_by": []}

        if term is not None:
            query_params["term"] = term
        if name is not None:
            query_params["filter_by"].append("filter_by=NAME:" + term)
        if description is not None:
            query_params["filter_by"].append("filter_by=DESCRIPTION:" + term)
        if tags is not None:
            query_params["filter_by"].append("filter_by=METADATA:" + term)

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, query_params, headers=headers)

    def global_search(
        self,
        type: str,
        term: str = None,
        name: str = None,
        description: str = None,
        tags: str = None,
        offset: int = 0,
        limit: int = 100,
    ):
        _client = client.get_instance()
        path_params = [
            "elastic",
            "fs",
        ]
        query_params = {"docType": type, "from": offset, "size": limit, "filter_by": []}

        if term is not None:
            query_params["term"] = term
        if name is not None:
            query_params["filter_by"].append("filter_by=NAME:" + name)
        if description is not None:
            query_params["filter_by"].append("filter_by=DESCRIPTION:" + description)
        if tags is not None:
            query_params["filter_by"].append("filter_by=METADATA:" + tags)

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, query_params, headers=headers)
