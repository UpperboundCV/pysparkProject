from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from configProvider.ConfigProvider import ConfigProvider

# How to run:C:\spark\bin\spark-submit readerRunner.py
if __name__ == '__main__':
    print("hello Reader")
    config_provider = ConfigProvider()
    spark_core = SparkCore(mode=config_provider.LOCAL)
    spark_reader = SparkReader(spark_core.spark_session)
    try:
        df = spark_reader.read_txt_file(txt_path='sparkcore/data/bluebook_doors_seats_collection_20210818.txt', \
                                   have_header=True, delimiter='~', is_infershema=True)
        df.show(truncate=False)
    except Exception as e:
        raise TypeError(f"{e}")
    finally:
        spark_core.close_session()
