import pytest
from sparkcore.SparkCore import SparkCore
from sparkcore.reader.SparkReader import SparkReader
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sys import platform
import os
if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

@pytest.fixture
def create_spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


@pytest.fixture
def mock_data(create_spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    columns = ["Brand", "FamilyDesc", "DescEng", "VehicleType", "CC", "gear", "DoorNum", "SeatCapacity", "YearGroup"]
    schema = StructType([
        StructField("Brand", StringType(), True),
        StructField("FamilyDesc", StringType(), True),
        StructField("DescEng", StringType(), True),
        StructField("VehicleType", StringType(), True),
        StructField("CC", DoubleType(), True),
        StructField("gear", StringType(), True),
        StructField("DoorNum", DoubleType(), True),
        StructField("SeatCapacity", DoubleType(), True),
        StructField("YearGroup", IntegerType(), True),
    ])
    data = [
        ("BMW", "116i", "F20 Hatch 4dr Steptronic 8sp RWD 1.6iTT", "PS", 1.6, "A", 4.0, 5.0, 2015),
        ("BMW", "116i", "F20 Hatch 4dr Steptronic 8sp RWD 1.6iTT", "PS", 1.6, "A", 4.0, 5.0, 2014)
    ]
    df = create_spark_session.createDataFrame(data, schema=schema)
    df.show(truncate=False)
    return df


def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


def test_SparkReader(mock_data: pyspark.sql.dataframe.DataFrame, create_spark_session: pyspark.sql.SparkSession) -> None:
    spark_reader = SparkReader(create_spark_session)
    df = spark_reader.read_txt_file(txt_path='../../data/bluebook_doors_seats_collection_20210818.txt',
                                    have_header=True, delimiter='~', is_infershema=True).limit(2)
    df.show(truncate=False)
    df.printSchema()
    assert are_dfs_schema_equal(mock_data, df)
    assert are_dfs_data_equal(mock_data, df)
    create_spark_session.stop()
