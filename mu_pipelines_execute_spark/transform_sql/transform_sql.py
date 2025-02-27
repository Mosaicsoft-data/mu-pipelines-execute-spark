from typing import TypedDict, cast

from mu_pipelines_interfaces.config_types.execute_config import ExecuteConfig
from mu_pipelines_interfaces.configuration_provider import ConfigurationProvider
from mu_pipelines_interfaces.execute_module_interface import ExecuteModuleInterface
from pyspark.sql import DataFrame, SparkSession

from mu_pipelines_execute_spark.context.spark_context import MUPipelinesSparkContext


class AdditionalAttribute(TypedDict):
    key: str
    value: str


class TransformSQLConfig(TypedDict, total=False):
    sql: str
    sql_file_location: str


class TransformSQL(ExecuteModuleInterface):
    sql_statement: str

    def __init__(
        self, config: ExecuteConfig, configuration_provider: ConfigurationProvider
    ):
        super().__init__(config, configuration_provider)
        sql_config: TransformSQLConfig = cast(TransformSQLConfig, self._config)
        if "sql" in sql_config:
            self.sql = sql_config["sql"]

        elif "sql_file_location" in sql_config:
            sql_statement: str | None = (
                self._configuration_provider.load_job_supporting_artifact(
                    sql_config["sql_file_location"], str
                )
            )
            assert sql_statement is not None
            self.sql_statement = sql_statement

        else:
            raise AssertionError("sql or sql_file_location required for TransformSQL")

    def execute(self, context: MUPipelinesSparkContext) -> DataFrame:
        spark: SparkSession = context["spark"]
        return spark.sql(self.sql_statement)


# https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically

# "execution": [
#     {
#         "type": "TransformSQL",
#         "_sql_file_location": "This is a relative path to the SQL Statement",
#         "sql_file_location": "./my_query.sql",
#         "_sql": "In-Line SQL Statement to execute"
#         "sql": "Select * from people"
#     }
# ]
