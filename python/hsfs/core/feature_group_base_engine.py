#
#   Copyright 2020 Logical Clocks AB
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

from hsfs.core import (
    feature_group_api,
    storage_connector_api,
    tags_api,
    kafka_api,
    provenance_api,
)


class FeatureGroupBaseEngine:
    ENTITY_TYPE = "featuregroups"

    def __init__(self, feature_store_id):
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._feature_group_api = feature_group_api.FeatureGroupApi(feature_store_id)
        self._storage_connector_api = storage_connector_api.StorageConnectorApi(
            feature_store_id
        )
        self._kafka_api = kafka_api.KafkaApi()
        self._provenance_api = provenance_api.ProvenanceApi()

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)

    def add_tag(self, feature_group, name, value):
        """Attach a name/value tag to a feature group."""
        self._tags_api.add(feature_group, name, value)

    def delete_tag(self, feature_group, name):
        """Remove a tag from a feature group."""
        self._tags_api.delete(feature_group, name)

    def get_tag(self, feature_group, name):
        """Get tag with a certain name."""
        return self._tags_api.get(feature_group, name)[name]

    def get_tags(self, feature_group):
        """Get all tags for a feature group."""
        return self._tags_api.get(feature_group)

    def sourced_from(self, feature_group):
        return self._provenance_api.app_links(
            in_artifact_type="FEATURE",
            out_artifact_type="FEATURE",
            out_artifact_name=feature_group.name,
            out_artifact_version=feature_group.version,
        )

    def generated_feature_groups(self, feature_group):
        return self._provenance_api.app_links(
            in_artifact_type="FEATURE",
            in_artifact_name=feature_group.name,
            in_artifact_version=feature_group.version,
            out_artifact_type="FEATURE",
        )

    def generated_training_datasets(self, feature_group):
        return self._provenance_api.app_links(
            in_artifact_type="FEATURE",
            in_artifact_name=feature_group.name,
            in_artifact_version=feature_group.version,
            out_artifact_type="TRAINING_DATASET",
        )

    def update_statistics_config(self, feature_group):
        """Update the statistics configuration of a feature group."""
        self._feature_group_api.update_metadata(
            feature_group, feature_group, "updateStatsConfig"
        )
