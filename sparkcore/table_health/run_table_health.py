# IMPORT
import os
import sys
import argparse
# cdp PySpark Environment
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark/"
os.environ["PYTHONPATH"] = "/opt/cloudera/parcels/CDH/lib/spark/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
sys.path.append('/nfs/msa/dapscripts/ka/pln/dev/tfm/pys/collection/table_health/')
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip")
# HOME MADE PACKAGE
from sparkcore.TableHealth import TableHealth
from sparkcore.SparkCore import SparkCore
import pyspark
from sparkcore.TableHealth import TableHealth
from sparkcore.writer.TableProperty import TableProperty
from sparkcore.writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from sparkcore.ColumnDescriptor import ColumnDescriptor

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-s", "--schema", required=True, help="schema name")
    ap.add_argument("-t", "--table_name", required=True, help="table name")
    args = vars(ap.parse_args())
    try:
        if args['env'] == 'dev' or args['env'] == 'prod':
            env = args['env']
            db_name = args['schema']
            tb_name = args['table_name']

            spark_core = SparkCore(env, f'spark_{db_name}.{tb_name}_health')

            table_health = TableHealth(spark_session=spark_core.spark_session, source_schema=db_name,
                                       source_table_name=tb_name, env=env)
            table_health.save()

            table_health_df = spark_core.spark_session.table(f'{table_health.schema}.{table_health.health_table_name}')
            table_health_df.show(truncate=False)
            table_health_df.printSchema()
            spark_core.close_session()
        else:
            raise TypeError(f"input environment is not right: {args['env']}")
    except Exception as e:
        raise TypeError(f" error: {e}")
