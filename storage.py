from pathlib import Path
from zipfile import ZipFile
from config import storage as settings


class Storage:
    def __init__(self) -> None:
        home = Path.home()
        default_dir = home / settings.TSE_CATCH_DIR
        self._data_dir = default_dir
        path_file = home / settings.PATH_FILE_NAME
        if path_file.is_file():
            with open(path_file, 'r', encoding='utf-8') as f:
                _data_dir = Path(f.readline())
                if _data_dir.is_dir:
                    self._data_dir = _data_dir
        else:
            with open(path_file, 'w+', encoding='utf-8') as f:
                f.write(str(self._data_dir))

        class ITD:
            """
            Intraday data
            """

            def __init__(self, stg: Storage):
                self.get_items = stg.itd_get_items
                self.set_item = stg.itd_set_item

        self.itd = ITD(self)

    # todo: add use read_csv if possible
    def get_item(self, key: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            return f.read()

    def set_item(self, key: str, value: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            f.write(value)

    def get_item_async(self, key: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        if zip:
            file_path = dir / (key + '.gz')
            with ZipFile(file_path) as zf:
                with open(zf, 'w+') as infile:
                    return infile.read()
        else:
            file_path = dir / (key + '.csv')
            with open(file_path, 'w+', encoding='utf-8') as f:
                return f.read()

    def set_item_async(self, key: str, value: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / settings.PRICES_DIR
        if zip:
            file_path = dir / (key + '.gz')
            with ZipFile(file_path, 'w') as zf:
                zf.write(value)
        else:
            file_path = dir / (key + '.csv')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(value)

    def get_items(self, keys: list) -> dict:
        result = {}
        dir = self._data_dir / settings.PRICES_DIR
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.csv', '')
                if key in keys:
                    result[key] = self.get_item(key)
        return result

    # get intraday item
    # todo: line 83 tse.js is not correctly ported to python
    def _itd_get_items(self, keys: list, full=False):
        result = {}
        dir = self._data_dir / settings.INTRADAY_DIR
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

    @property
    def catch_dir(self):
        return self._data_dir

    @catch_dir.setter
    def catch_dir(self, value: str):
        self._data_dir = Path(value)
