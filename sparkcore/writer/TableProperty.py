import ColumnDescriptor
from typing import List


class TableProperty:
    def __init__(self, db_name: str,
                 tb_name: str,
                 table_path: str,
                 partitions: List[ColumnDescriptor],
                 fields: List[ColumnDescriptor]) -> None:
        self.db_name = db_name
        self.tb_name = tb_name
        self.table_path = table_path
        self.partitions = partitions
        self.fields = fields
