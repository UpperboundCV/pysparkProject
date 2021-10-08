from sparkcore.configProvider.TableConfig import TableConfig

if __name__=='__main__':
    config_path = "../data/config/"
    table_config = TableConfig(config_path, 'local')
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
    # to be able to write table: D:\winutils\bin\winutils.exe chmod 777 D:\tmp\hive
    # spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    #  FindFileOwnerAndPermission error (1789): The trust relationship between this workstation and the primary domain failed.