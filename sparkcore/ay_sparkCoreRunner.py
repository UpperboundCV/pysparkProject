#--------------------------------------------------
#OBJECTIVE : Test run pyspark
#CREATE BY : Krisorn Chunhapongpipat [UP]
#CREATE DATE : 2021/10/01
#--------------------------------------------------

#IMPORT
from os.path import expanduser, join, abspath
import os
import sys, re
import subprocess
import datetime

# PySpark Environment
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark/"
os.environ["PYTHONPATH"] = "/opt/cloudera/parcels/CDH/lib/spark/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip")

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
