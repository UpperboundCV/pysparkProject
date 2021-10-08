from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from sys import platform
import os
if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

if __name__ == '__main__':
    config_path = "../data/config/"
    redbook_table_config = TableConfig(config_path, 'local', 'bluebook')
    redbook_table_property = TableProperty(db_name=redbook_table_config.db_name,
                                           tb_name=redbook_table_config.tb_name,
                                           table_path=redbook_table_config.table_path,
                                           fields=redbook_table_config.fields,
                                           partitions=redbook_table_config.partitions)
    print(redbook_table_property.database)
    print(redbook_table_property.table)
    print(redbook_table_property.table_path)
    print(redbook_table_property.create_table_sql(
        table_format=redbook_table_property.ORC_FORMAT,
        delimitor=None
    ))
    redbook_table_config = TableConfig(config_path, 'local', 'redbook')
    redbook_table_property = TableProperty(db_name=redbook_table_config.db_name,
                                           tb_name=redbook_table_config.tb_name,
                                           table_path=redbook_table_config.table_path,
                                           fields=redbook_table_config.fields,
                                           partitions=redbook_table_config.partitions)
    print(redbook_table_property.database)
    print(redbook_table_property.table)
    print(redbook_table_property.table_path)
    print(redbook_table_property.create_table_sql(
        table_format=redbook_table_property.ORC_FORMAT,
        delimitor=None
    ))