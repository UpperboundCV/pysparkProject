from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from configProvider.ConfigProvider import ConfigProvider


class SparkCore:
    def __init__(self, mode: str) -> None:
        try:
            conf_provider = ConfigProvider(mode=mode)
            self.spark_conf = SparkConf().setAll(pairs=conf_provider.get_spark_configs())
            self.spark_session = SparkSession \
                .builder \
                .config(conf=self.spark_conf) \
                .master(conf_provider.get_spark_master()) \
                .enableHiveSupport() \
                .config("spark.hive.mapred.supports.subdirectories", "true") \
                .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true") \
                .appName(conf_provider.get_spark_app_name()) \
                .getOrCreate()
        except Exception as e:
            self.is_error = True
            raise TypeError(f"error to create spark_core: {e}")
        finally:
            self.close_session()
            if self.is_error:
                exit(1)

    def get_spark_session(self) -> SparkSession:
        return self.spark_session

    def get_conf(self) -> list[tuple[str, str]]:
        return self.spark_session.sparkContext.getConf().getAll()

    def close_session(self) -> None:
        self.spark_session.stop()
