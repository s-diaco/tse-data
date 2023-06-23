"""
Read and write tse files
"""
from pathlib import Path

import pandas as pd
from dtse.config import storage as settings
from dtse.setup_logger import logger


class Storage:
    """
    methods for reading and writing data
    """

    def __init__(self, **kwargs) -> None:
        data_dir = Path(settings["TSE_CACHE_DIR"])
        path_file = Path(settings["PATH_FILE_NAME"])
        home = Path.home()
        self._data_dir = home / data_dir
        path_file = home / path_file
        if path_file.is_file():
            with open(path_file, "r", encoding="utf-8") as file:
                data_path = Path(file.readline())
                if data_path.is_dir():
                    self._data_dir = data_path
        else:
            with open(path_file, "w+", encoding="utf-8") as file:
                file.write(str(self._data_dir))

        self._data_dir.mkdir(parents=True, exist_ok=True)
        if "tse_dir" in kwargs:
            self._data_dir = Path(kwargs["tse_dir"])
        logger.info("data dir: %s", self._data_dir)

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
            with open(file_path, "w+", encoding="utf-8") as file:
                file.write("")
                return ""
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

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

    def read_prc_csv(self, f_names: list[str]) -> pd.DataFrame:
        """
        Reads selected instruments files from the cache dir and returns a dict.

        :f_names: list[str], list of file names to read from.

        :return: dict
        """

        prices_list = [self.read_tse_csv_blc(f"prices.{name}") for name in f_names]
        prices_list = [prcs for prcs in prices_list if not prcs.empty]
        if prices_list:
            res = pd.concat(prices_list).set_index(["InsCode", "DEven"]).sort_index()
        else:
            res = pd.DataFrame()
        return res

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
            data.to_csv(file_path, encoding="utf-8", mode="a", header=False)
        else:
            data.to_csv(file_path, encoding="utf-8")
