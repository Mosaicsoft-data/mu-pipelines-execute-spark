from typing import cast

import deprecation
from mu_pipelines_configuration_provider.diy_configuration_provider import (
    DIYConfigurationProvider,
)
from mu_pipelines_interfaces.config_types.connection_properties import (
    ConnectionProperties,
)
from mu_pipelines_interfaces.config_types.execute_config import ExecuteConfig
from mu_pipelines_interfaces.config_types.global_properties.global_properties import (
    GlobalProperties,
)
from mu_pipelines_interfaces.config_types.job_config import JobConfigItem
from mu_pipelines_interfaces.configuration_provider import ConfigurationProvider

from mu_pipelines_execute_spark.load_csv.load_csv import LoadCSV


@deprecation.fail_if_not_removed
def test_deprecation():
    config: ExecuteConfig = cast(
        ExecuteConfig, dict({"type": "CSVReadCommand", "file_location": "./file.csv"})
    )

    configuration_provider: ConfigurationProvider = DIYConfigurationProvider(
        job_config=[
            cast(
                JobConfigItem,
                dict(
                    {
                        "execution": [
                            {"type": "CSVReadCommand", "file_location": "./file.csv"}
                        ],
                        "destination": [],
                    }
                ),
            )
        ],
        global_properties=cast(GlobalProperties, dict({"library": "spark"})),
        connection_config=cast(ConnectionProperties, dict({})),
    )

    LoadCSV(config, configuration_provider)
