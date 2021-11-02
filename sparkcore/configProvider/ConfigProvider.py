import configparser
import os.path
import traceback


class ConfigProvider:
    LOCAL: str = 'local'
    DEV: str = 'dev'
    PROD: str = 'prod'

    def __init__(self, config_path: str, mode: str) -> None:
        self.config = configparser.ConfigParser()
        try:
            current_path = os.path.abspath(os.path.dirname(__file__))
            env_config_path = os.path.join(current_path, config_path)
            print(f'env_config_path:{env_config_path}')
            if mode == self.LOCAL:
                print(f'config mode: {self.LOCAL}')
                env_config_path += 'local.ini'
            elif mode == self.DEV:
                print(f'config mode: {self.DEV}')
                env_config_path += 'dev.ini'
            else:
                print(f'config mode: {self.PROD}')
                env_config_path += 'prod.ini'
            self.config.read(env_config_path)
        except Exception as e:
            traceback.print_exc(e)


