"""
Read and write tse files
"""
import gzip
from pathlib import Path

import pandas as pd
from .config import storage as settings
from .setup_logger import logger


class Storage:
    """
    methods for reading and writing data
    """

    def __init__(self) -> None:
        data_dir = Path(settings["TSE_CACHE_DIR"])
        path_file = Path(settings["PATH_FILE_NAME"])
        home = Path.home()
        self._data_dir = home / data_dir
        path_file = home / path_file
        if path_file.is_file():
            with open(path_file, "r", encoding="utf-8") as f:
                data_path = Path(f.readline())
                if data_path.is_dir():
                    self._data_dir = data_path
        else:
            with open(path_file, "w+", encoding="utf-8") as f:
                f.write(str(self._data_dir))

        self._data_dir.mkdir(parents=True, exist_ok=True)
        logger.info("data dir: %s", self._data_dir)

        # todo: uncomment
        # pylint: disable=W0105
        """
        class ITD:
            def __init__(self, stg: Storage):
                self.get_items = stg._itd_get_items
                self.set_item = stg._itd_set_item

        self.itd = ITD(self)
        """
        # pylint: enable=W0105

    def get_item(self, key: str) -> str:
        """
        Reads a file from the cache dir and returns a string

        :param key: file name
        :return: string
        """
        key = key.replace("tse.", "")
        tse_dir = self._data_dir
        if key.startswith("prices."):
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        file_path = tse_dir / (key + ".csv")
        if not file_path.is_file():
            with open(file_path, "w+", encoding="utf-8") as f:
                return f.write("")
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def set_item(self, key: str, value: str) -> None:
        """
        Writes a file to the cache dir

        :param key: file name
        :param value: text to write
        """

        key = key.replace("tse.", "")
        tse_dir = self._data_dir
        if key.startswith("prices."):
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        file_path = tse_dir / (key + ".csv")
        with open(file_path, "w+", encoding="utf-8") as f:
            f.write(value)

    async def get_item_async(self, key: str, tse_zip=False):
        """
        Reads a file from the cache dir and returns a string

        :param key: file name
        :return: string
        """
        key = key.replace("tse.", "")
        tse_dir = self._data_dir
        if key.startswith("prices."):
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        if tse_zip:
            file_path = tse_dir / (key + ".gz")
            if not file_path.is_file():
                with gzip.open(file_path, mode="wt") as zip_f:
                    zip_f.write("")
            with gzip.open(file_path, mode="rt") as zip_f:
                return zip_f.read()
        else:
            file_path = tse_dir / (key + ".csv")
            if not file_path.is_file():
                with open(file_path, "w+", encoding="utf-8") as f:
                    f.write("")
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()

    async def set_item_async(self, key: str, value: str, tse_zip=False):
        """
        Writes a file to the cache dir

        :param key: file name
        :param value: text to write
        """

        key = key.replace("tse.", "")
        tse_dir = self._data_dir
        if key.startswith("prices."):
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        if tse_zip:
            file_path = tse_dir / (key + ".gz")
            with gzip.open(file_path, mode="wt") as zip_f:
                zip_f.write(value)
        else:
            file_path = tse_dir / (key + ".csv")
            with open(file_path, "w+", encoding="utf-8") as f:
                f.write(value)

    def get_items(self, f_names: list[str]) -> dict:
        """
        Reads selected instruments files from the cache dir and returns a dict

        :return: dict
        """

        res = {}
        res = {name: self.read_tse_csv_blc(f"prices.{name}") for name in f_names}
        return res

    # todo: complete this
    # pylint: disable=W0105
    """
    # todo: line 83 tse.js is not correctly ported to python
    def _itd_get_items(self, keys: list, full=False):
        result = {}
        tse_dir = self._data_dir / settings['INTRADAY_DIR']
        p = tse_dir.glob('**/*')
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
        tse_dir = self._data_dir / settings.INTRADAY_DIR
        for k in obj.keys:
            file_path = tse_dir / (key + '.' + k + '.gz')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(obj[k])
    """
    # pylint: enable=W0105

    @property
    def cache_dir(self):
        """
        :return: cache dir
        """
        return self._data_dir

    @cache_dir.setter
    def cache_dir(self, value: str):
        self._data_dir = Path(value)

    async def read_tse_csv(self, f_name: str) -> pd.DataFrame:
        """
        Reads a csv TSE file and returns a DataFrame

        :param f_name: str, file name
        :param data: DataFrame, list of dicts
        """

        return self.read_tse_csv_blc(f_name=f_name)

    async def write_tse_csv(self, f_name: str, data: pd.DataFrame) -> None:
        """
        Writes a csv file to the TSE

        :param f_name: str, file name
        :param data: list, list of dicts
        """

        self.write_tse_csv_blc(f_name=f_name, data=data)

    def read_tse_csv_blc(self, f_name: str) -> pd.DataFrame:
        """
        Reads a csv TSE file and returns a DataFrame

        :param f_name: str, file name
        :param data: DataFrame, list of dicts
        """

        f_name = f_name.replace("tse.", "")
        tse_dir = self._data_dir
        if f_name.startswith("prices."):
            f_name = f_name.replace("prices.", "")
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        file_path = tse_dir / (f_name + ".csv")
        res = pd.DataFrame()
        if file_path.is_file():
            try:
                res = pd.read_csv(file_path, encoding="utf-8")
            except pd.errors.EmptyDataError:
                pass
        return res

    def write_tse_csv_blc(self, f_name: str, data: pd.DataFrame, **kwargs) -> None:
        """
        Writes a csv file to the TSE

        :param f_name: str, file name
        :param data: list, list of dicts
        """

        f_name = f_name.replace("tse.", "")
        tse_dir = self._data_dir
        if f_name.startswith("prices."):
            f_name = f_name.replace("prices.", "")
            tse_dir = self._data_dir / settings["PRICES_DIR"]
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        if len(data) == 0:
            return
        file_path = tse_dir / (f_name + ".csv")
        if kwargs.get("append", False):
            data.to_csv(file_path, index=False, encoding="utf-8")
        else:
            data.to_csv(
                file_path, index=False, encoding="utf-8", mode="a", header=False
            )
