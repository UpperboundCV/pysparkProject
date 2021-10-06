from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
try:
    from configProvider.ConfigProvider import ConfigProvider
except:
    from .configProvider.ConfigProvider import ConfigProvider

class SparkCore:
    is_error = False

    def __init__(self, mode: str) -> None:
        try:
            conf_provider = ConfigProvider(mode=mode)
            self.spark_conf = SparkConf().setAll(pairs=conf_provider.get_spark_configs())
            self.spark_session = SparkSession \
                .builder \
                .config(conf=self.spark_conf) \
                .master(conf_provider.get_spark_master()) \
                .appName(conf_provider.get_spark_app_name()) \
                .getOrCreate()
        except Exception as e:
            self.is_error = True
            raise TypeError(f"error to create spark_core: {e}")
        finally:
            if self.is_error:
                exit(1)

    def get_spark_session(self) -> SparkSession:
        return self.spark_session

    def get_conf(self) -> list[tuple[str, str]]:
        return self.spark_session.sparkContext.getConf().getAll()

    def close_session(self) -> None:
        self.spark_session.stop()
