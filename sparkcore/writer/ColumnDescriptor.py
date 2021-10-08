class ColumnDescriptor:
    def __init__(self,column_name: str, data_type: str, comment: str) -> None:
        self.name = column_name
        self.data_type = data_type
        self.comment = comment

