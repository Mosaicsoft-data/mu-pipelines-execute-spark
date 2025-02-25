from typing import TypedDict, cast

from mu_pipelines_interfaces.config_types.execute_config import ExecuteConfig
from mu_pipelines_interfaces.configuration_provider import ConfigurationProvider
from mu_pipelines_interfaces.execute_module_interface import ExecuteModuleInterface
from pyspark.sql import DataFrame, DataFrameReader, SparkSession

from mu_pipelines_execute_spark.context.spark_context import MUPipelinesSparkContext


class AdditionalAttribute(TypedDict):
    key: str
    value: str


class IngestCSVConfig(TypedDict):
    file_location: str
    delimiter: str
    quotes: str
    additional_attributes: list[AdditionalAttribute]


class IngestCSV(ExecuteModuleInterface):
    def __init__(
        self, config: ExecuteConfig, configuration_provider: ConfigurationProvider
    ):
        super().__init__(config, configuration_provider)
        """
        TODO need to determine how to validate the config
        """
        ingest_csv_config: IngestCSVConfig = cast(IngestCSVConfig, self._config)
        assert "file_location" in ingest_csv_config
        assert (
            len(ingest_csv_config["file_location"]) > 0
        )  # whatever makes sense to validate for path
        # TODO should all csv config be required?

    def execute(self, context: MUPipelinesSparkContext) -> DataFrame:
        spark: SparkSession = context["spark"]
        ingest_csv_config: IngestCSVConfig = cast(IngestCSVConfig, self._config)

        reader: DataFrameReader = spark.read

        if "delimiter" in ingest_csv_config:
            reader = reader.option("delimiter", ingest_csv_config["delimiter"])

        if "quotes" in ingest_csv_config:
            reader = reader.option("quote", ingest_csv_config["quotes"])

        if "additional_attributes" in ingest_csv_config:
            for additional_attribute in ingest_csv_config["additional_attributes"]:
                reader = reader.option(
                    additional_attribute["key"], additional_attribute["value"]
                )

        return reader.csv(ingest_csv_config["file_location"])


# https://spark.apache.org/docs/latest/sql-data-sources-csv.html#csv-files

# "execution": [
#     {
#         "type": "IngestCSV",
#         "_file_location": "This can be a URL or accessible location",
#         "file_location": "",
#         "delimiter": ",",
#         "quotes": "escape_all",
#         "_additional_attributes": "optional argument to pass extra properties",
#         "additional_attributes": [
#             {
#                 "key": "key1",
#                 "value": "value1"
#             },
#             {
#                 "key": "key2",
#                 "value": "value2"
#             }
#         ]
#     }
# ]
