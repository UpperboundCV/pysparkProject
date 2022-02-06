""" TableCreator creates table from its schema.
It will create table by automatically providing HDFS path to create table.
Its input is list of columns of table.
"""
from typing import Optional, Match
import re


class TableCreator:
    db_source = ['afs', 'web']
    db_datamart = ['collection','autodatamart']

    def __init__(self, schema: str, table_name: str) -> None:
        self.schema = schema
        self.table_name = table_name
        self.table_path = self.get_table_path()

    def get_table_path(self) -> Optional[str]:
        """return table path which user in this department only"""

        # todo please have regex to check pattern schema [entity][environment]_[db_source/db_datamart]
        def validate_schema_name() -> bool:
            return bool(
                re.match(f"(ay|ka)(prod|dev|dev1)(_)({'|'.join((self.db_source + self.db_datamart))})", self.schema))

        def add_source_or_destination(src_or_dst: str) -> Optional[str]:
            if src_or_dst in self.db_source or src_or_dst in self.db_datamart:
                return '/' + src_or_dst
            else:
                raise TypeError("source or destination schema is not valid")

        if validate_schema_name():
            base = ''
            source_or_destination = self.schema.split('_')[1]
            if 'ka' in self.schema:
                base = base + '/data/ka'
                if 'dev' in self.schema:
                    base = base + '/dev'
                    if 'crt' in self.schema:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        base = base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                elif 'prod' in self.schema:
                    base = base + '/prod'
                    if 'crt' in self.schema:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        base = base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                else:
                    raise TypeError(f"There is no recognized environment of table.")
            elif 'ay' in self.schema:
                base = base + '/data/ay'
                if 'dev' in self.schema:
                    base = base + '/dev'
                    if 'crt' in self.schema:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        base = base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                elif 'prod' in self.schema:
                    base = base + '/prod'
                    if 'crt' in self.schema:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    elif 'pst' in self.schema:
                        base = base + '/pst' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                    else:
                        base = base + '/crt' + add_source_or_destination(source_or_destination) + '/' + self.table_name
                else:
                    raise TypeError(f"There is no recognized environment of table.")
            else:
                raise TypeError(f"schema is not in neither ka or ay")
        else:
            raise TypeError(
                f"schema name is invalid. Please check whether your table is in this pattern or not: "
                f"(ay|ka)(prod|dev|dev1)(_)({'|'.join((self.db_source + self.db_datamart))})")
