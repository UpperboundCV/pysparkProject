import configparser
import traceback
from typing import List, Tuple
import os.path


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
            current_path = os.path.abspath(os.path.dirname(__file__))
            env_config_path = os.path.join(current_path, "../../sparkEnvConfig/")
            if mode == self.LOCAL:
                env_config_path += 'local.ini'
            elif mode == self.DEV:
                env_config_path += 'dev.ini'
            else:
                env_config_path += 'prod.ini'
            self.config.read(env_config_path)
        except Exception as e:
            traceback.print_exc(e)

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
