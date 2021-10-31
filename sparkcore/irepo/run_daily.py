import os
import sys
import argparse
sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')
os.environ['PYTHONPATH'] = '/home/up_python/PycharmProjects/pysparkProject/sparkcore'
from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, IntegralType, \
    DoubleType
from pyspark.sql.functions import lit, col
from typing import List
from sys import platform

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

if __name__=='__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env", required=True, help="environment: local, dev, or prod")
    args = vars(ap.parse_args())

#     check environment if local environment load data from txt files, otherwise read data from transaction table
    if args['env'] == 'local':
        pass
    else:
        pass

