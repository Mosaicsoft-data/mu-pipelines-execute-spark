from typing import TypedDict

from pyspark.sql import SparkSession


class MUPipelinesSparkContext(TypedDict):
    spark: SparkSession
