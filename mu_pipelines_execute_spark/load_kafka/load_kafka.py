from typing import TypedDict, cast

from mu_pipelines_interfaces.config_types.execute_config import ExecuteConfig
from mu_pipelines_interfaces.configuration_provider import ConfigurationProvider
from mu_pipelines_interfaces.execute_module_interface import ExecuteModuleInterface
from pyspark.sql import DataFrame, SparkSession

from mu_pipelines_execute_spark.context.spark_context import MUPipelinesSparkContext
import json
from pyspark.sql.types import (
    StructType,
    StructField
)
from pyspark.sql.functions import from_json, col, max


class AdditionalAttribute(TypedDict):
    key: str
    value: str


class LoadKafkaConfig(TypedDict):
    schema: str
    kafka_brokers: str
    kafka_topic: str
    additional_attributes: list[AdditionalAttribute]


class LoadKafka(ExecuteModuleInterface):
    def __init__(
        self, config: ExecuteConfig, configuration_provider: ConfigurationProvider
    ):
        super().__init__(config, configuration_provider)
        """
        TODO need to determine how to validate the config
        """
        load_kafka_config: LoadKafkaConfig = cast(LoadKafkaConfig, self._config)
        assert "kafka_brokers" in load_kafka_config
        assert (
            len(load_kafka_config["kafka_brokers"]) > 0
        )  # whatever makes sense to validate for path

    def execute(self, context: MUPipelinesSparkContext) -> DataFrame:
        spark: SparkSession = context["spark"]
        load_kafka_config: LoadKafkaConfig = cast(LoadKafkaConfig, self._config)

        with open("schema.json", "r") as f:
            schema_json = json.load(f)

        # Convert JSON to Spark StructType
        schema = StructType(
            [
                StructField(
                    field["name"],
                    eval(field["type"].capitalize() + "Type()"),
                    field["nullable"],
                )
                for field in schema_json["fields"]
            ]
        )

        # read offset
        startingoffset = json.load(open("offsets.json", "r"))
        reader = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", load_kafka_config["kafka_brokers"])
            .option("subscribe", load_kafka_config["kafka_topic"])
            .option("startingOffsets", startingoffset)
            .load()
        )
        latest_offsets = reader.groupBy("partition").agg(
            max("offset").alias("max_offset")
        )
        # save offset
        json.dump(latest_offsets, open("offsets.json", "w"))

        # Parse incoming messages
        parsed_df = (
            reader.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )

        return parsed_df


# https://spark.apache.org/docs/latest/sql-data-sources-csv.html#csv-files

# "execution": [
#     {
#         "type": "KafkaReadCommand",
#         "schema": "contact_schema",
#         "kafka_brokers": "",
#         "kafka_topic": "",
#         "startingOffsets": "json.dumps(load_offsets())"
#     }
# ]
