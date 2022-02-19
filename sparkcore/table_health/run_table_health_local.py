# IMPORT
import sys
import argparse
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# local PySpark Environment
sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore/')

# HOME MADE PACKAGE
from TableHealth import TableHealth
from SparkCore import SparkCore
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from ColumnDescriptor import ColumnDescriptor


def mock_test_data(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-15'
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('brand', StringType(), True)
    ])
    data = [(date, "a01", 2000.50, 'Mitsu'),
            (date, "a02", 12345.99, 'Mitsu'),
            (date, "a03", 10.0, 'Honda'),
            (date, None, None, None)]
    df = spark_session.createDataFrame(data, schema)
    return df


def create_test_table(spark_session: pyspark.sql.SparkSession) -> None:
    db_name = 'kadev_collection'
    tb_name = 'mock_cl'
    table_path = '/tmp/mock_cl'
    fields = [ColumnDescriptor('account_no', 'string', '"none"'),
              ColumnDescriptor('price', 'integer', '"none"'),
              ColumnDescriptor('brand', 'string', '"none"')]
    partitions = [ColumnDescriptor('data_date', 'string', '"none"')]
    table_property = TableProperty(db_name=db_name, tb_name=tb_name, table_path=table_path, fields=fields,
                                   partitions=partitions)
    spark_writer = SparkWriter(spark_session)
    spark_writer.create_table(table_property)
    empty_mock_df = spark_session.table(f'{db_name}.{tb_name}')
    empty_mock_df.show(truncate=False)
    mock_test_data(spark_session).write.format('orc').partitionBy('data_date').mode('overwrite').saveAsTable(
        f'{db_name}.{tb_name}')


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-s", "--schema", required=True, help="schema name")
    ap.add_argument("-t", "--table_name", required=True, help="table name")
    args = vars(ap.parse_args())
    try:
        if args['env'] == 'local':
            env = args['env']
            db_name = args['schema']
            tb_name = args['table_name']
            print('Hello')
            spark_core = SparkCore(env, f'spark_{db_name}.{tb_name}_health')
            create_test_table(spark_core.spark_session)
            table_health = TableHealth(spark_session=spark_core.spark_session, source_schema=db_name,
                                       source_table_name=tb_name)
            table_health.save()

            table_health_df = spark_core.spark_session.table(f'{table_health.schema}.{table_health.health_table_name}')
            table_health_df.show(truncate=False)
            table_health_df.printSchema()
            spark_core.close_session()
        else:
            raise TypeError(f"input environment is not right: {args['env']}")
    except Exception as e:
        raise TypeError(f" error: {e}")
