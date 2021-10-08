class ColumnDescriptor:
    def __init__(self,column_name: str, data_type: str, comment: str) -> None:
        self.name = str(column_name)
        self.data_type = str(data_type)
        self.comment = str(comment)

    def get_name(self):
        return str(self.name)

    def get_data_type(self):
        return str(self.data_type)

    def get_comment(self):
        return str(self.comment)

    def to_str(self):
        return str(self.name + "\t" + self.data_type + "\t" + self.comment)

