import sys
import argparse

sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')

from configProvider.ConfigProvider import ConfigProvider
from SparkCore import SparkCore
from TableCreator import TableCreator
from pyspark.sql.functions import col, round, lit
from datetime import datetime
from ColumnDescriptor import ColumnDescriptor
from writer.SparkWriter import SparkWriter

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-t", "--table", required=True, help="Table: auction or redbook")
    args = vars(ap.parse_args())
    try:
        if (args['env'] == 'dev' or args['env'] == 'prod') and (args['table'] == 'auction' or args['table'] == 'redbook'):
            now = datetime.now()
            env = args['env']
            result_table = args['table']
            config_path = '../car_price_output/config/'
            config_provider = ConfigProvider(config_path, env)
            account_key_path = config_provider.config['account_txt'].get('path')
            print(f"account path: {account_key_path}")
            result_table_path = config_provider.config[f'{result_table}_result'].get('path')
            result_db = config_provider.config[f'{result_table}_result'].get('db_name')
            result_tb = config_provider.config[f'{result_table}_result'].get('table_name')
            print(f"{result_table} result path: {result_table_path}")
            print(f"{result_table} save into {result_db}.{result_tb}")
            spark_core = SparkCore(env, f"{result_table}_car_price_output")
            print(f"spark config: {spark_core.get_conf()}")
            result_df = spark_core.spark_session.read.option("header", True).csv(result_table_path)
            # result_df.select('ACCOUNT_KEY','AUTO_YEAR').show(truncate=False)
            account_df = spark_core.spark_session.read.option("header", True).csv(account_key_path)
            # account_df.show(truncate=False)
            result_df_col = result_df.columns
            additional_cols = ['account_number', 'product_code', 'branch_code', 'gecid', 'collection_number']
            final_cols = result_df_col + additional_cols
            final_result_df = result_df.join(account_df, on=["ACCOUNT_KEY"]).select(*final_cols)
            # final_result_df.select("ACCOUNT_KEY", *additional_cols).show(truncate=False)
            pth_month_key = now.strftime("%Y%m")

            # save result
            if final_result_df.count() > 0:
                except_col_list = ['gecid', 'collection_number']
                ka_result_df = final_result_df.withColumn("ptn_month_key", lit(pth_month_key)).where(
                    col('gecid') == '52800000')
                ay_result_df = final_result_df.withColumn("ptn_month_key", lit(pth_month_key)).where(
                    col('gecid') == '60000000')

                result_fields = [ColumnDescriptor(column_name=col_type[0], data_type=col_type[1],
                                                  comment=f'{col_type[0]} in {result_table}') for col_type in
                                 final_result_df.dtypes if col_type[0] not in except_col_list]
                ptn_fields = [
                    ColumnDescriptor(column_name='pth_month_key', data_type='string', comment='partition column')]
                print('\n'.join([col_detail.name for col_detail in result_fields]))
                spark_writer = SparkWriter(spark_core.spark_session)

                ka_table_creator = TableCreator(spark_session=spark_core.spark_session,
                                                schema=f'ka{result_db}', table_name=result_tb, fields=result_fields,
                                                partition_cols=ptn_fields, env=env)
                ka_table_creator.create()

                if ka_result_df.count() > 0:
                    ka_result_df.drop(*except_col_list).write.format("orc").insertInto(f'ka{result_db}.{result_tb}',
                                                                                       overwrite=True)
                    ka_export_path = config_provider.config[f'{result_table}_result'].get('ka_export_path')
                    ka_result_df.where(col('collection_number') >= 6).drop(*except_col_list).toPandas().to_csv(
                        ka_export_path, index=False)

                ka_df = spark_core.spark_session.table(f'ka{result_db}.{result_tb}')
                ka_df.show(truncate=False)
                print("Done ka")

                ay_table_creator = TableCreator(spark_session=spark_core.spark_session,
                                                schema=f'ay{result_db}', table_name=result_tb, fields=result_fields,
                                                partition_cols=ptn_fields, env=env)
                ay_table_creator.create()

                if ay_result_df.count() > 0:
                    ay_result_df.drop(*except_col_list).write.format("orc").insertInto(f'ay{result_db}.{result_tb}',
                                                                                       overwrite=True)
                    ay_export_path = config_provider.config[f'{result_table}_result'].get('ay_export_path')
                    ay_result_df.where(col('collection_number') >= 6).drop(*except_col_list).toPandas().to_csv(
                        ay_export_path, index=False)

                ay_df = spark_core.spark_session.table(f'ay{result_db}.{result_tb}')
                ay_df.show(truncate=False)
                print("Done ay")

            else:
                raise TypeError("No result from car result prediction")
            spark_core.close_session()
        else:
            raise TypeError(
                f"input environment is not right: {args['env']} or {args['table']} result table is not in car_price")
    except Exception as e:
        raise TypeError(f" error: {e}")
