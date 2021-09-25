from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from configProvider.ConfigProvider import ConfigProvider


class SparkCore:
    def __init__(self, mode: str) -> None:
        conf_provider = ConfigProvider(mode=mode)
        self.spark_conf = SparkConf().setAll(pairs=conf_provider.get_spark_configs())
        self.spark_session = SparkSession \
            .builder \
            .config(conf=self.spark_conf) \
            .master(conf_provider.get_spark_master()) \
            .appName(conf_provider.get_spark_app_name()) \
            .getOrCreate()

    def get_spark_session(self):
        return self.spark_session

    def get_conf(self) -> str:
        return self.spark_session.sparkContext.getConf().getAll()

    def stop(self) -> None:
        self.spark_session.stop()
