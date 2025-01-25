from pyspark.sql import SparkSession


def initialize_context(existing_context: dict) -> None:
    if "spark" not in existing_context:
        existing_context["spark"] = SparkSession.Builder().getOrCreate()
