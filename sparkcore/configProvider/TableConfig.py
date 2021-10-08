from sparkcore.configProvider.ConfigProvider import ConfigProvider
from sparkcore.writer.ColumnDescriptor import ColumnDescriptor
from sparkcore.writer.TableProperty import TableProperty
from typing import List,Optional


class TableConfig(ConfigProvider):
    DB: str = 'database'
    TB: str = 'table'
    TB_PATH: str = 'table_path'
    PARTITIONS: str = 'partitions'
    FIELDS: str = 'fields'

    def __init__(self, config_path: str, mode: str) -> None:
        super().__init__(config_path, mode)

    def get_table_config(self, table_name: str) -> TableProperty:
        return TableProperty(
            db_name=self.config[table_name].get(self.DB),
            tb_name=self.config[table_name].get(self.TB),
            table_path=self.config[table_name].get(self.TB_PATH),
            fields=self.to_column_description(self.config[table_name].get(self.FIELDS)),
            partitions=self.to_column_description(self.config[table_name].get(self.PARTITIONS))
        )

    def to_column_description(self, fields: str) -> Optional[List[ColumnDescriptor]]:
        if fields is None:
            return None
        else:
            fields_split = fields.split(',')
            lst_cols: List[ColumnDescriptor] = list()
            # todo: loop can be optimized
            for field in fields_split:
                description = field.rstrip('\n').split(':')
                # print(description[0])
                # print(description[1])
                # print(description[2])
                lst_cols.append(ColumnDescriptor(column_name=description[0],
                                                 data_type=description[1],
                                                 comment=description[2]))
            # print(f"exit=> {fields}")
            return lst_cols
