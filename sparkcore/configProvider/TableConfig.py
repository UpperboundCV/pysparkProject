import configparser
import traceback
from typing import List, Tuple
import os.path


class TableConfig:
    LOCAL: str = 'local'
    DEV: str = 'dev'
    PROD: str = 'prod'