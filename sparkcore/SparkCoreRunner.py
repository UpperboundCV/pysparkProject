from SparkCore import SparkCore
from ingestion.SparkIngestion import SparkIngestion
from configProvider.ConfigProvider import ConfigProvider

if __name__ == "__main__":
    print("hello pyspark")
    config_provider = ConfigProvider()
    spark_core = SparkCore(mode=config_provider.LOCAL)
    print(f'spark conf: {spark_core.get_conf()}')
    ingestion = SparkIngestion(spark_core.spark_session)
    print(f'spark version: {ingestion.get_spark_version()}')
    spark_core.stop()
    conf = config_provider.config
    print(conf.sections())
