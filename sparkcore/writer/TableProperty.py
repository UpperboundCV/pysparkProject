from typing import Any, List, Optional
from ColumnDescriptor import ColumnDescriptor


class TableProperty:
    TEXT_FORMAT: str = 'TEXTFILE'
    ORC_FORMAT: str = 'ORC'

    def __init__(self, db_name: str,
                 tb_name: str,
                 table_path: str,
                 fields: List[ColumnDescriptor],
                 partitions: Optional[List[ColumnDescriptor]] = None) -> None:
        self.database = db_name
        self.table = tb_name
        self.table_path = table_path
        self.partition_by = partitions
        self.column_descriptions = fields

    def column_types_to_str(self, column_specs: List[ColumnDescriptor]) -> str:
        column_types_process = []
        print(type(column_specs[0]))
        print(','.join([f"{column_spec.name} {column_spec.data_type} " for column_spec in
                column_specs]))
        if type(column_specs) is list:
            print('cond1')
            column_vectors = column_specs
            column_types_process = [
                f"{column_spec.name} {column_spec.data_type} COMMENT '{column_spec.comment}'" for column_spec in
                column_specs]
        else:
            print('cond2')
            column_types_process = [
                f"{column_spec.name} {column_spec.data_type} COMMENT '{column_spec.comment}'" for column_spec in
                column_specs[0]]
        output = ',\n'.join(column_types_process)
        print(output)
        return output

    def create_table_sql(self, table_format: str = ORC_FORMAT, delimitor: str = None) -> str:
        table_type = 'external'
        partition_set_up = f"partitioned by{'' if self.partition_by is None else f'({self.column_types_to_str(self.partition_by)})'}"
        row_format_delimit = f"row format delimited fields terminated BY \'{delimitor}\'" + "\n" if not (
                table_format == self.ORC_FORMAT) else ""
        stored_format = f"stored as {table_format}"
        location = f"location \'{self.table_path}\'"
        tblproperties = f"TBLPROPERTIES (\'bucketing_version\'=\'2\', \'transactional\'=\'false\')"  # ,\'external.table.purge\'=\'true\')"
        return f"create {table_type} table if not exists {self.database}.{self.table}(\n" \
               f"{self.column_types_to_str(self.column_descriptions)})" \
               f"\n{partition_set_up}" \
               f"\n{row_format_delimit}{stored_format}" \
               f"\n{location}" \
               f"\n{tblproperties}"
