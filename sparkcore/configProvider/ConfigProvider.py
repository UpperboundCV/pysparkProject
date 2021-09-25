import configparser
from typing import List, Tuple


class ConfigProvider:
    LOCAL: str = 'local'
    DEV: str = 'dev'
    PROD: str = 'prod'
    # section and sub section of config file *.ini
    SPARK: str = 'spark'
    APP_NAME: str = 'app_name'
    MASTER: str = 'master'
    EXECUTOR_MEMORY: str = 'executor_memory'
    EXECUTOR_CORES: str = 'executor_cores'
    DRIVER_MEMORY: str = 'driver_memory'
    LOCAL_DIR: str = 'local_dir'
    # spark config
    SPARK_EXECUTOR_MEMORY: str = 'spark.executor.memory'
    SPARK_APP_NAME: str = 'spark.app.name'
    SPARK_EXECUTOR_CORES: str = 'spark.executor.cores'
    SPARK_DRIVER_MEMORY: str = 'spark.driver.memory'
    SPARK_LOCAL_DIR: str = 'spark.local.dir'
    SPARK_MASTER: str = 'spark.master'

    def __init__(self, mode: str = LOCAL) -> None:
        self.config = configparser.ConfigParser()
        try:
            if mode == self.LOCAL:
                self.config.read('resources/local.ini')
            elif mode == self.DEV:
                self.config.read('resources/dev.ini')
            else:
                self.config.read('resources/prod.ini')
        except Exception as e:
            print("cannot read local config")
            raise

    def get_spark_configs(self) -> List[Tuple[str, str]]:
        return [
            (self.SPARK_EXECUTOR_MEMORY, self.config[self.SPARK].get(f'{self.EXECUTOR_MEMORY}', '2g')),
            (self.SPARK_EXECUTOR_CORES, self.config[self.SPARK].get(f'{self.EXECUTOR_CORES}', '1')),
            (self.SPARK_DRIVER_MEMORY, self.config[self.SPARK].get(f'{self.DRIVER_MEMORY}', '4g')),
            (self.SPARK_LOCAL_DIR, self.config[self.SPARK].get(f'{self.LOCAL_DIR}', '/tmp'))
        ]

    def get_spark_app_name(self) -> str:
        return self.config[self.SPARK].get(f'{self.SPARK_APP_NAME}',
                                           'sparkProject')  # change default value to YYYYMMDDHHMMSS

    def get_spark_master(self) -> str:
        return self.config[self.SPARK].get(f'{self.SPARK_MASTER}', 'local[4]')
