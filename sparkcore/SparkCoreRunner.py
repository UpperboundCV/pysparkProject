from SparkCore import SparkCore
from reader.SparkReader import SparkReader
from configProvider.SparkEnvConfig import SparkEnvConfig

from sys import platform
import os
if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

if __name__ == "__main__":
    print("hello sparkCore")
    config_provider = SparkEnvConfig('local')
    spark_core = SparkCore(mode=config_provider.LOCAL)
    print(f'spark conf: {spark_core.get_conf()}')
    ingestion = SparkReader(spark_core.spark_session)
    print(f'spark version: {ingestion.get_spark_version()}')
    spark_core.close_session()
    conf = config_provider.config
    print(conf.sections())
