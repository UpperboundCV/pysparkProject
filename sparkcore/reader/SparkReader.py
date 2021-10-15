import pyspark
from pyspark.sql import SparkSession


class SparkReader:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_version(self) -> str:
        return self.spark_session.version

    def read_txt_file(self, txt_path: str, have_header: bool, delimiter: str, is_infershema: bool) -> pyspark.sql.dataframe.DataFrame:
        # "helper/data/bluebook_doors_seats_collection_20210818.txt"
        return self.spark_session.read \
            .option("header", "true" if have_header else "false") \
            .option("delimiter", delimiter) \
            .option("inferschema", "true" if is_infershema else "false") \
            .format("csv") \
            .load(txt_path)
