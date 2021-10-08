from TableConfig import TableConfig

if __name__ == '__main__':
    config_path="../data/config/"
    table_config = TableConfig(config_path,'local')
    bluebook_property = table_config.get_table_config('bluebook')
    print(bluebook_property.database)
    print(bluebook_property.table)
    print(bluebook_property.table_path)
    # print(table_property.partitions))
    # print("\n".join(table_property.fields))
    print(bluebook_property.create_table_sql(
        table_format=bluebook_property.ORC_FORMAT,
        delimitor=None
    ))
    redbook_property = table_config.get_table_config('redbook')
    print(redbook_property.database)
    print(redbook_property.table)
    print(redbook_property.table_path)
    # print(table_property.partitions))
    # print("\n".join(table_property.fields))
    print(redbook_property.create_table_sql(
        table_format=redbook_property.ORC_FORMAT,
        delimitor=None
    ))