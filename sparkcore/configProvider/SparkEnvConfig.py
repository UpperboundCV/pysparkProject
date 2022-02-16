from typing import List, Tuple
from .ConfigProvider import ConfigProvider


class SparkEnvConfig(ConfigProvider):  # next will inherite from config provider
    # section and sub section of config file *.ini
    SPARK: str = 'spark'
    APP_NAME: str = 'app_name'
    MASTER: str = 'master'
    EXECUTOR_MEMORY: str = 'executor_memory'
    EXECUTOR_CORES: str = 'executor_cores'
    DRIVER_MEMORY: str = 'driver_memory'
    DEPLOY_MODE: str = 'deploy_mode'
    LOCAL_DIR: str = 'local_dir'
    MAX_RESULT_SIZE: str = 'max_result_size'
    # spark config
    SPARK_EXECUTOR_MEMORY: str = 'spark.executor.memory'
    SPARK_APP_NAME: str = 'spark.app.name'
    SPARK_EXECUTOR_CORES: str = 'spark.executor.cores'
    SPARK_DRIVER_MEMORY: str = 'spark.driver.memory'
    SPARK_LOCAL_DIR: str = 'spark.local.dir'
    SPARK_MASTER: str = 'spark.master'
    SPARK_DEPLOY_MODE: str = 'spark.submit.deployMode'
    SPARK_DRIVER_MAXRESULTSIZE: str = 'spark.driver.maxResultSize'
    SPARK_PARTITION_OVERWRITE_MODE: str = 'spark.sql.sources.partitionOverwriteMode'
    HIVE_EXEC_DYNAMIC_PARTITION: str = 'hive.exec.dynamic.partition'
    HIVE_EXEC_DYNAMIC_PARTITION_MODE: str = 'hive.exec.dynamic.partition.mode'

    def __init__(self, mode: str, job_name: str = "") -> None:
        super().__init__('../../sparkEnvConfig/', mode)
        self.mode = mode
        self.job_name = job_name
        print(f"{self.SPARK_EXECUTOR_MEMORY}:{self.config[self.SPARK].get(self.EXECUTOR_MEMORY, '2g')}")
        print(f"{self.SPARK_EXECUTOR_CORES}:{self.config[self.SPARK].get(self.EXECUTOR_CORES, '1')}")
        print(f"{self.SPARK_DRIVER_MEMORY}:{self.config[self.SPARK].get(self.DRIVER_MEMORY, '4g')}")

    def get_spark_configs(self) -> List[Tuple[str, str]]:
        base_config_list = [
            (self.SPARK_EXECUTOR_MEMORY, self.config[self.SPARK].get(self.EXECUTOR_MEMORY, '2g')),
            (self.SPARK_EXECUTOR_CORES, self.config[self.SPARK].get(self.EXECUTOR_CORES, '1')),
            (self.SPARK_DRIVER_MEMORY, self.config[self.SPARK].get(self.DRIVER_MEMORY, '4g')),
            (self.HIVE_EXEC_DYNAMIC_PARTITION, "true"),
            (self.HIVE_EXEC_DYNAMIC_PARTITION_MODE, "nonstrict")
        ]
        if self.mode == super().LOCAL:
            print(f'mode: {super().LOCAL}')
            base_config_list.append((self.SPARK_LOCAL_DIR, self.config[self.SPARK].get(self.LOCAL_DIR, '/tmp')))
        elif self.mode == super().DEV:
            print(f'mode: {super().DEV}')
            base_config_list.append(
                (self.SPARK_DRIVER_MAXRESULTSIZE, self.config[self.SPARK].get(self.SPARK_DRIVER_MAXRESULTSIZE, '64g')))
            base_config_list.append((self.SPARK_PARTITION_OVERWRITE_MODE, "dynamic"))
            base_config_list.append((self.SPARK_DEPLOY_MODE, self.config[self.SPARK].get(self.DEPLOY_MODE, 'client')))
        elif self.mode == super().PROD:
            print(f'mode: {super().PROD}')
            base_config_list.append(
                (self.SPARK_DRIVER_MAXRESULTSIZE, self.config[self.SPARK].get(self.SPARK_DRIVER_MAXRESULTSIZE, '64g')))
            base_config_list.append((self.SPARK_PARTITION_OVERWRITE_MODE, "dynamic"))
            base_config_list.append((self.SPARK_DEPLOY_MODE, self.config[self.SPARK].get(self.DEPLOY_MODE, 'cluster')))
        else:
            raise TypeError(f"environment input")
        return base_config_list

    def get_spark_app_name(self) -> str:
        # force job_name to be defined at calling sparkcore
        app_name_cnf = self.config[self.SPARK].get(f'{self.APP_NAME}_{self.job_name}',
                                                   self.job_name)  # change default value to YYYYMMDDHHMMSS
        print(f'spark_app_name: {app_name_cnf}')
        return app_name_cnf

    def get_spark_master(self) -> str:
        print(f"spark_master : {self.config[self.SPARK].get(self.MASTER, 'local[4]')}")
        return self.config[self.SPARK].get(self.MASTER, 'local[4]')
