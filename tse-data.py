# %% Configuration
API_URL = 'http://service.tsetmc.com/tsev2/data/TseClient2.aspx'
TSE_CATCH_DIR = 'tse-catch'
PATH_FILE_NAME = '.tse'
PRICES_DIR = 'prices'
INTRADAY_DIR = 'intraday'


# %% Class storage
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile


class storage:
    def __init__(self) -> None:
        home = Path.home()
        default_dir = home / TSE_CATCH_DIR
        self._data_dir = default_dir
        path_file = home / PATH_FILE_NAME
        if path_file.is_file():
            with open(path_file, 'r', encoding='utf-8') as f:
                _data_dir = Path(f.readline())
                if _data_dir.is_dir:
                    self._data_dir = _data_dir
        else:
            with open(path_file, 'w+', encoding='utf-8') as f:
                f.write(str(self._data_dir))
    
    # todo: add use read_csv if possible
    def get_item(self, key: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            return f.read()

    def set_item(self, key: str, value: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            f.write(value)

    def get_item_async(self, key: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
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
            dir = self._data_dir / PRICES_DIR
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
        dir = self._data_dir / PRICES_DIR
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.csv', '')
                if key in keys:
                    result[key] = self.get_item(key)
        return result

    # get intraday item
    # todo: line 83 tse.js is not correctly ported to python
    def itd_get_items(self, keys: list, full=False):
        result = {}
        dir = self._data_dir / INTRADAY_DIR
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.gz', '').replace('.csv', '')
                if key in keys:
                    result[key] = self.itd_get_item(key, full)
        return result

    # set intraday item
    # todo: line 107 tse.js is not correctly ported to python
    def itd_set_item(self, key: str, obj: dict):
        key = key.replace('tse.', '')
        dir = self._data_dir / INTRADAY_DIR
        for k in obj:
            file_path = dir / (key + '.' + k + '.gz')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(obj[k])


    @property
    def catch_dir(self):
        return self._data_dir

    @catch_dir.setter
    def catch_dir(self, value: str):
        self._data_dir = Path(value)


class tse_rq:
    def tse_instrument(self, DEven: str):
        params = {
			't': 'Instrument',
			'a': DEven
		}
        return self.make_request(params)