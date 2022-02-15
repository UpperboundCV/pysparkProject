""" TableCreator creates table from its schema.
It will create table by automatically providing HDFS path to create table.
Its input is list of columns of table.
"""
from typing import Optional, Match, List
from sparkcore.writer.SparkWriter import SparkWriter
from sparkcore.writer.TableProperty import TableProperty
from sparkcore.ColumnDescriptor import ColumnDescriptor
from pyspark.sql import SparkSession
import re


class TableCreator:
    db_source = ['afs', 'web', 'manual', 'osx']
    db_datamart = ['collection', 'autodatamart']
    db_zone = ['pst', 'crt']

    def __init__(self, spark_session: SparkSession, schema: str, table_name: str, fields: List[ColumnDescriptor],
                 partition_cols: List[ColumnDescriptor], env: str) -> None:
        self.env = env
        self.spark_session = spark_session
        self.schema = schema
        self.table_name = table_name
        self.fields = fields
        self.partitions = partition_cols
        self.table_path = self.get_table_path()

    def get_table_path(self) -> Optional[str]:
        """return table path which user in this department only"""

        def table_regex() -> str:
            return f"((ay|ka)(prod|dev|dev1)(_)({'|'.join(self.db_datamart)}))|" \
                   f"((ay|ka)(prod|dev|dev1)(_)(pst|crt)(_)({'|'.join(self.db_source)}))"

        def validate_schema_name() -> bool:
            return bool(re.match(table_regex(), self.schema))

        def add_source_or_destination(src_or_dst: str) -> Optional[str]:
            if src_or_dst in self.db_source or src_or_dst in self.db_datamart:
                return '/' + src_or_dst
            else:
                raise TypeError("source or destination schema is not valid")

        if validate_schema_name():
            print(f'environment: {self.env}')
            base = '' if self.env != 'local' else '/tmp'
            source_or_destination = self.schema.split('_')[-1]
            if 'ka' in self.schema:
                base = base + '/data/ka'
                if 'dev' in self.schema:
                    base = base + '/dev'
                    if 'crt' in self.schema:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        return base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                elif 'prod' in self.schema:
                    base = base + '/prod'
                    if 'crt' in self.schema:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        return base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                else:
                    raise TypeError(f"There is no recognized environment of table.")
            elif 'ay' in self.schema:
                base = base + '/data/ay'
                if 'dev' in self.schema:
                    base = base + '/dev'
                    if 'crt' in self.schema:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        return base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                elif 'prod' in self.schema:
                    base = base + '/prod'
                    if 'crt' in self.schema:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        return base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        return base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                else:
                    raise TypeError(f"There is no recognized environment of table.")
            else:
                raise TypeError(f"schema is not in neither ka or ay")
        else:
            raise TypeError(
                f"schema name is invalid. Please check whether your table is in this pattern or not: "
                f"{table_regex()}")

    def create(self) -> bool:
        table_health_name = self.table_name
        table_health_property = TableProperty(db_name=self.schema,
                                              tb_name=self.table_name,
                                              table_path=self.get_table_path(),
                                              fields=self.fields,
                                              partitions=self.partitions)
        spark_writer = SparkWriter(self.spark_session)
        spark_writer.create_table(table_health_property)
        return spark_writer.does_table_exist(self.schema, self.table_name)
