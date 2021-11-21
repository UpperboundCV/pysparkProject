from .ConfigProvider import ConfigProvider
from typing import List, Optional, Dict

try:
    from ColumnDescriptor import ColumnDescriptor
except:
    from sparkcore.ColumnDescriptor import ColumnDescriptor


class TableConfig(ConfigProvider):
    DB: str = 'database'
    TB: str = 'table'
    TB_PATH: str = 'table_path'
    PARTITIONS: str = 'partitions'
    FIELDS: str = 'fields'
    CHECK_POINT_PATH: str = 'checkpoint_path'

    def __init__(self, config_path: str, mode: str, table_name: str) -> None:
        super().__init__(config_path, mode)
        self.db_name = self.config[table_name].get(self.DB)
        self.tb_name = self.config[table_name].get(self.TB)
        self.table_path = self.config.get(table_name, self.TB_PATH, fallback=None)
        self.fields = self.to_column_description(self.config.get(table_name, self.FIELDS, fallback=None))
        self.partitions = self.to_column_description(self.config.get(table_name, self.PARTITIONS, fallback=None))
        self.check_point_path = self.config.get(table_name, self.CHECK_POINT_PATH, fallback=None)

    def to_column_description(self, column_descriptions: str) -> Optional[List[ColumnDescriptor]]:
        if column_descriptions is None:
            return None
        else:
            print(f"fields: {column_descriptions}")
            fields_split = column_descriptions.split('\n')
            lst_cols = []
            # todo: loop can be optimized
            for field in fields_split:
                description = field.rstrip('\n').split(':')
                # print(f'description0:{description[0]}')
                # print(f'description1:{description[1]}')
                # print(f'description2:{description[2]}')
                column_descriptor = ColumnDescriptor(column_name=description[0],
                                                     data_type=description[1],
                                                     comment=description[2])
                lst_cols.append(column_descriptor)
                # print("==================================")
            # print(type(lst_cols))
            # print("****************************")
            return lst_cols

    def column_to_data_type(self) -> Dict[str, str]:
        column_desc_lst = self.fields + self.partitions
        return {column_desc.name: column_desc.data_type for column_desc in column_desc_lst}
