import gzip
from pathlib import Path
from config import storage as settings
from setup_logger import logger


class Storage:
    """
    methods for reading and writing data
    """

    def __init__(self) -> None:
        data_dir = Path(settings['TSE_CACHE_DIR'])
        path_file = Path(settings['PATH_FILE_NAME'])
        home = Path.home()
        self._data_dir = home / data_dir
        path_file = home / path_file
        if path_file.is_file():
            with open(path_file, 'r', encoding='utf-8') as f:
                data_path = Path(f.readline())
                if data_path.is_dir:
                    self._data_dir = data_path
        else:
            with open(path_file, 'w+', encoding='utf-8') as f:
                f.write(str(self._data_dir))

        self._data_dir.mkdir(parents=True, exist_ok=True)
        logger.info('data dir: %s', self._data_dir)

        # todo: uncomment
        """
        class ITD:
            def __init__(self, stg: Storage):
                self.get_items = stg._itd_get_items
                self.set_item = stg._itd_set_item

        self.itd = ITD(self)
        """

    def get_item(self, key: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings['PRICES_DIR']
        file_path = dir / (key + '.csv')
        if not file_path.is_file():
            with open(file_path, 'w+', encoding='utf-8') as f:
                return f.write('')
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def set_item(self, key: str, value: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings['PRICES_DIR']
        if not dir.is_dir():
            dir.mkdir(parents=True, exist_ok=True)
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            f.write(value)

    async def get_item_async(self, key: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        if not dir.is_dir():
            dir.mkdir(parents=True, exist_ok=True)
        if zip:
            file_path = dir / (key + '.gz')
            if not file_path.is_file():
                with gzip.open(file_path, mode="wt") as zf:
                    zf.write('')
            with gzip.open(file_path, mode='rt') as zf:
                return zf.read()
        else:
            file_path = dir / (key + '.csv')
            if not file_path.is_file():
                with open(file_path, 'w+', encoding='utf-8') as f:
                    f.write('')
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()

    async def set_item_async(self, key: str, value: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        if not dir.is_dir():
            dir.mkdir(parents=True, exist_ok=True)
        if zip:
            file_path = dir / (key + '.gz')
            with gzip.open(file_path, mode="wt") as zf:
                zf.write(value)
        else:
            file_path = dir / (key + '.csv')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(value)

    async def get_items(self, sel_ins=[]) -> dict:
        result = {}
        dir = self._data_dir / settings['PRICES_DIR']
        if not dir.is_dir():
            dir.mkdir(parents=True, exist_ok=True)
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.csv', '')
                if key not in sel_ins:
                    continue
                file_path = dir/x.name
                with open(file_path, 'r', encoding='utf-8') as f:
                    result[key] = f.read()
        return result

    # todo: complete this
    """
    # todo: line 83 tse.js is not correctly ported to python
    def _itd_get_items(self, keys: list, full=False):
        result = {}
        dir = self._data_dir / settings['INTRADAY_DIR']
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.gz', '').replace('.csv', '')
                if key in keys:
                    result[key] = self.itd_get_item(key, full)
        return result

    # set intraday item
    # todo: line 107 tse.js is not correctly ported to python
    def _itd_set_item(self, key: str, obj: dict):
        key = key.replace('tse.', '')
        dir = self._data_dir / settings.INTRADAY_DIR
        for k in obj.keys:
            file_path = dir / (key + '.' + k + '.gz')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(obj[k])
    """

    @property
    def cache_dir(self):
        return self._data_dir

    @cache_dir.setter
    def cache_dir(self, value: str):
        self._data_dir = Path(value)

    async def read_tse_csv(self, f_name: str, data: list):
        """
        Reads a csv file from the TSE and returns a list of dicts
        :param f_name: file name
        :param data: list of dicts
        """
        pass

    async def write_tse_csv(self, f_name: str, data: list):
        """
        Writes a csv file to the TSE
        :param f_name: file name
        :param data: list of dicts
        """
        pass
