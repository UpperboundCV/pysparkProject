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
from typing import List

def collection_target() -> List[str]: 
    return ['kadev_collection.haircut_account',
            'aydev_collection.haircut_account',
            'kadev_pst_afs.haircut_account_staging',
            'aydev_pst_afs.haircut_account_staging',
            'kadev_collection.haircut_account_fact',
            'aydev_collection.haircut_account_fact',
            'kadev_pst_afs.dr_account_staging',
            'aydev_pst_afs.dr_account_staging',
            'kadev_collection.dr_account',
            'aydev_collection.dr_account',
            'kadev_collection.dr_account_fact',
            'aydev_collection.dr_account_fact',
            'kadev_pst_afs.dr_account',
            'aydev_pst_afs.dr_account',
            'kadev_pst_afs.collection_mis_stg',
            'aydev_pst_afs.collection_mis_stg',
            'kadev_collection.collection_mis_details',
            'aydev_collection.collection_mis_details',
            'kadev_collection.collection_mis_sums',
            'aydev_collection.collection_mis_sums',
            'kadev_collection.collection_backend_model',
            'aydev_collection.collection_backend_model',
            'kadev_collection.cl_coll_profile',
            'aydev_collection.cl_coll_profile',
            'kadev_collection.cl_coll_org',
            'aydev_collection.cl_coll_org',
            'kadev_collection.cl_message_hist',
            'aydev_collection.cl_message_hist',
            'kadev_collection.cl_cape_limit_cr',
            'aydev_collection.cl_cape_limit_cr',
            'kadev_pst_afs.auto_litigation_staging',
            'aydev_pst_afs.auto_litigation_staging',
            'kadev_pst_afs.auto_litigation_txn',
            'aydev_pst_afs.auto_litigation_txn',
            'kadev_pst_afs.auto_litigation_fact',
            'aydev_pst_afs.auto_litigation_fact',
            'kadev_pst_afs.auto_broke_litigation',
            'aydev_pst_afs.auto_broke_litigation',
            'kadev_pst_afs.fact_cl_related_fee',
            'aydev_pst_afs.fact_cl_related_fee']

if __name__ == "__main__":    
    env = 'dev'
    
    for collection_table in collection_target():
        partition_df = None
        [db_name, tb_name] = collection_table.split('.')
        abb_name = db_name[:2] + '_' +tb_name
        spark_core = SparkCore(env, f'{abb_name}_health')        
        try:
            partition_df = spark_core.spark_session.sql(f'show partitions {collection_table}')            
        except Exception as e:            
            print(f'{e}')
        if partition_df is not None:
            print(f"{collection_table}")
            
            [db_name, tb_name] = collection_table.split('.')
            table_health = TableHealth(spark_session=spark_core.spark_session, source_schema=db_name,
                                       source_table_name=tb_name, env=env)
            table_health.save()

            table_health_df = spark_core.spark_session.table(f'{table_health.schema}.{table_health.health_table_name}')
            table_health_df.show(truncate=False)
            table_health_df.printSchema()
        spark_core.close_session()      
