import pytest
from sparkcore.SparkCore import SparkCore
from sys import platform
import pyspark
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, substring, regexp_replace, count
from sparkcore.helper.DataFrameHelper import DataFrameHelper
import random
import string
import itertools

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


def get_random_ka_user(length: int) -> str:
    # With combination of lower and upper case
    return ''.join(random.choice(string.ascii_letters) for i in range(length))


def get_random_cr_user() -> str:
    # get random string of 8 digits
    source = string.digits
    return ''.join((random.choice(source) for i in range(8)))


@pytest.fixture
def mock_ay_user_entity(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    user_login = 'user_login'
    user_status = 'status'
    entity_code = 'entity_code'
    schema = StructType([
        StructField(user_login, StringType(), True),
        StructField(user_status, StringType(), True),
        StructField(entity_code, StringType(), True)
    ])
    entity = 'AYCAL'
    data = [(30000618, 'Active', entity),
            ('KULYAWKE', 'Active', entity),
            (10000001, 'Inactive', entity),
            ('RATCHANOK', 'Inactive', entity),
            (20000002, 'Active', entity),
            ('SOMBOON', 'Active', entity),
            (30000003, 'Active', entity),
            ('onnut', 'Active', entity),
            (40000004, 'Inactive', entity),
            ('SUKUMVIT', 'Inactive', entity),
            (50000005, None, entity),
            ('PRAYUT', None, entity),
            (get_random_cr_user(), 'Active', entity),
            (get_random_ka_user(9), 'Active', entity),
            (get_random_cr_user(), 'Inactive', entity),
            (get_random_ka_user(9), 'Inactive', entity),
            (get_random_cr_user(), None, entity),
            (get_random_ka_user(9), None, entity)
            ]

    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_ka_user_entity(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    user_login = 'user_login'
    user_status = 'status'
    entity_code = 'entity_code'
    schema = StructType([
        StructField(user_login, StringType(), True),
        StructField(user_status, StringType(), True),
        StructField(entity_code, StringType(), True)
    ])
    entity = 'BAY'
    data = [(30000618, 'Active', entity),
            ('KULYAWKE', 'Active', entity),
            (10000001, 'Inactive', entity),
            ('RATCHANOK', 'Inactive', entity),
            (20000002, 'Inactive', entity),
            ('SOMBOON', 'Inactive', entity),
            (30000003, None, entity),
            ('onnut', None, entity),
            (40000004, 'Active', entity),
            ('SUKUMVIT', 'Active', entity),
            (50000005, 'Active', entity),
            ('PRAYUT', 'Active', entity),
            (get_random_cr_user(), 'Active', entity),
            (get_random_ka_user(9), 'Active', entity),
            (get_random_cr_user(), 'Inactive', entity),
            (get_random_ka_user(9), 'Inactive', entity),
            (get_random_cr_user(), None, entity),
            (get_random_ka_user(9), None, entity)
            ]

    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture()
def mock_entity_uam_result(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    entity_uam = 'entity_uam'
    entity_total = 'entity_total'
    schema = StructType([StructField(entity_uam, StringType(), True),
                         StructField(entity_total, IntegerType(), False)])
    data = [(None, 10), ('Auto', 2), ('Bay', 6), ('Aycal', 6)]

    df = spark_session.createDataFrame(data, schema)
    return df


def test_entity_uam(mock_ay_user_entity: pyspark.sql.dataframe.DataFrame,
                    mock_ka_user_entity: pyspark.sql.dataframe.DataFrame,
                    mock_entity_uam_result: pyspark.sql.dataframe.DataFrame) -> None:
    mock_ay_user_entity.show(truncate=False)
    mock_ka_user_entity.show(truncate=False)
    entities = ['ka', 'ay']
    entity_status = ['status', 'entity']
    key_list = ['user_login']
    entity_uam = 'entity_uam'
    ay_user_entity_df = mock_ay_user_entity \
        .withColumnRenamed('status', 'ay_status') \
        .withColumnRenamed('entity_code', 'ay_entity')
    ka_user_entity_df = mock_ka_user_entity \
        .withColumnRenamed('status', 'ka_status') \
        .withColumnRenamed('entity_code', 'ka_entity')
    all_entities_df = DataFrameHelper.combine_entity_df(ay_df=ay_user_entity_df, ka_df=ka_user_entity_df,
                                                        join_key=key_list)
    uam_cal_list = key_list + [entities + "_" + entity_status for
                               entity_status, entities in
                               itertools.product(entity_status, entities)]
    print('\n'.join(uam_cal_list))
    entity_status_labeled_df = all_entities_df.selectExpr(*uam_cal_list)
    entity_status_labeled_df.show(truncate=False)
    print(f'num_row = {entity_status_labeled_df.count()}')
    uam_df = entity_status_labeled_df.withColumn(entity_uam, DataFrameHelper.add_entity_uam())
    uam_df.show(n=100, truncate=False)
    assert uam_df.count() == 24
    entity_uam_summary_df = uam_df.groupBy(entity_uam).agg((count("*").cast(IntegerType())).alias('entity_total'))
    entity_uam_summary_df.show(n=100, truncate=False)
    assert are_dfs_schema_equal(entity_uam_summary_df, mock_entity_uam_result)
    assert are_dfs_data_equal(entity_uam_summary_df, mock_entity_uam_result)
