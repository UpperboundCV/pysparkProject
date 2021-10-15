from SparkCore import SparkCore
from reader.SparkReader import SparkReader
from sys import platform
import os
if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"
# How to run:C:\spark\bin\spark-submit readerRunner.py
if __name__ == '__main__':
    print("hello Reader")
    # todo:create function find file in project directory
    spark_core = SparkCore(mode='local')
    spark_reader = SparkReader(spark_core.spark_session)
    try:
        df = spark_reader.read_txt_file(txt_path='helper/data/bluebook_doors_seats_collection_20210818.txt', \
                                   have_header=True, delimiter='~', is_infershema=True)
        df.show(truncate=False)
    except Exception as e:
        raise TypeError(f"{e}")
    finally:
        spark_core.close_session()
