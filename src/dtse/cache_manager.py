"""
manage cached data
"""

from datetime import datetime
from pathlib import Path
from sqlite3 import connect

import numpy as np
import pandas as pd

from dtse.setup_logger import logger as tse_logger


class TSECache:
    """
    Manage TSE data cached in memory and/or csv files
    """

    def __init__(self, settings) -> None:
        """
        Initialize TSECache class

        :settings: dict, Configuration from config file and user input.
        """

        self.settings = settings
        self._instruments: pd.DataFrame | None = None
        self._splits: pd.DataFrame | None = None
        self._prices: pd.DataFrame | None = None
        self._prices_merged: pd.DataFrame | None = None
        self.merged_instruments: pd.DataFrame | None = None
        # TODO: delete if not used
        self.last_devens: pd.Series | None = None
        self.cache_to_csv = self.settings["cache"] if "cache" in self.settings else True
        self._init_cache_dir()
        self._last_instrument_update = self._read_last_inst_upd()
        self._read_instrums()
        self._read_splits()

    def _read_last_inst_upd(self) -> str:
        """
        Reads lastInstrumUpdate.txt file from cache dir and returns a string.

        :return: str, the date that instrumnts list is last updated.
        returns an empty string if file not found
        """
        key = "lastInstrumUpdate"
        tse_dir = self.cache_dir
        file_path = tse_dir / f"{key}.txt"
        if not file_path.is_file():
            return ""
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    def _init_cache_dir(self) -> None:
        if "tse_dir" in self.settings:
            self._data_dir = Path(self.settings["tse_dir"])
        else:
            data_dir = Path(self.settings["TSE_CACHE_DIR"])
            path_file = Path(self.settings["PATH_FILE_NAME"])
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
        tse_logger.info("data dir: %s", self._data_dir)
        self._cnx = connect(self._data_dir / self.settings["DB_FILE_NAME"])

    @property
    def cache_dir(self):
        """
        :return: cache dir
        """
        return self._data_dir

    @cache_dir.setter
    def cache_dir(self, value: str):
        self._data_dir = Path(value)

    @property
    def instruments(self):
        """All instruments and their details."""
        return self._instruments

    @instruments.setter
    def instruments(self, value: pd.DataFrame):
        if not value.empty:
            self._instruments = value
            if self.cache_to_csv:
                f_name = "instruments"
                self._instruments.to_sql(f_name, self._cnx, if_exists="replace")
                self._save_last_inst_upd()

    def _save_last_inst_upd(self):
        today = datetime.now().strftime("%Y%m%d")
        if self._last_instrument_update != today:
            self._last_instrument_update = today
            key = "lastInstrumUpdate"
            file_path = self.cache_dir / f"{key}.txt"
            with open(file_path, "w+", encoding="utf-8") as file:
                file.write(today)

    @property
    def splits(self):
        """Data about stock splits"""
        return self._splits

    @splits.setter
    def splits(self, value: pd.DataFrame):
        if not value.empty:
            self._splits = value
            if self.cache_to_csv:
                f_name = "splits"

                # TODO: if_exists=replce or if_exists=append?
                self._splits.to_sql(f_name, self._cnx, if_exists="replace")
                self._save_last_inst_upd()

    @property
    def prices(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._prices is not None:
            if not self._prices.empty:
                return self._prices.sort_index()
        return None

    @property
    def prices_merged(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._prices_merged is not None:
            return self._prices_merged.sort_index()
        return None

    def add_to_prices(self, dframes: list[pd.DataFrame]) -> bool:
        """
        Adds a list of dataframes to "prices" property.

        :dframes: list[pd.DataFrame], list of dataframes to add to prices property.

        :return: bool, True if value added to prices, else False.
        """

        # TODO: fix empty data frames price at first non trading days of each InsCode
        dframes = [data for data in dframes if not data.empty]

        if dframes:
            if self._prices is None:
                self._prices = pd.concat(dframes)
            else:
                tot_prices = [self._prices]
                tot_prices.extend(dframes)
                self._prices = pd.concat(tot_prices)
            return True
        else:
            return False

    @property
    def last_instrument_update(self):
        """last date of updating list of instrument and splits"""
        return self._last_instrument_update

    def read_prices(self, selected_syms: pd.DataFrame):
        """
        updates a dicts of prices for ins_codes in
        self.prices and self.prices_merged
        """

        # TODO: do not load all price files at one. there may be hundreds of them.
        prices = self._read_prc(f_names=selected_syms.index.tolist())
        if not prices.empty:
            self._prices = prices.sort_index()

    def _read_prc(self, f_names: list[str]) -> pd.DataFrame:
        """
        Reads selected instruments files from the cache dir and returns a dict.

        :f_names: list[str], list of file names to read from.

        :return: pd.DataFrame
        """

        table = "daily_prices"
        # check if table exists in db
        check_query = (
            f"SELECT name FROM sqlite_schema WHERE type='table' AND name='{table}';"
        )
        find_table = pd.read_sql(sql=check_query, con=self._cnx)
        if find_table.empty:
            return find_table
        else:
            # TODO: is it a safe query?
            codes = ", ".join(str(code) for code in f_names)
            query = f"SELECT * FROM {table} WHERE InsCode In ({codes});"
            data = pd.read_sql(sql=query, con=self._cnx)
            return data

    def refresh_prices_merged(self, selected_syms):
        """
        Updates prices_merged property.
        """

        if self._prices is not None:
            merged_prcs = self._prices[
                self._prices.index.isin(selected_syms.index, level=0)
            ]
            merged_prcs = merged_prcs.join(selected_syms[["Symbol", "CComVal"]])

            # fix if InsCode has changed and closing price for the first day with new InsCode is set to 1000 or 0
            merged_prcs["Prev_Symbol"] = merged_prcs["Symbol"]
            merged_prcs["Curr_Symbol"] = merged_prcs["Symbol"]
            merged_prcs = merged_prcs.reset_index().set_index(["Symbol", "DEven"])
            merged_prcs = merged_prcs.sort_index()
            merged_prcs["Prev_CComVal"] = merged_prcs["CComVal"].shift(1)
            merged_prcs["Prev_Symbol"] = merged_prcs["Prev_Symbol"].shift(1)
            merged_prcs["upd"] = (
                merged_prcs["Prev_CComVal"] != merged_prcs["CComVal"]
            ) & (merged_prcs["Prev_Symbol"] == merged_prcs["Curr_Symbol"])
            if not merged_prcs[merged_prcs["upd"]].empty:
                merged_prcs["yday+1"] = merged_prcs["PriceYesterday"].shift(+1)
                merged_prcs[
                    (merged_prcs["upd"])
                    & (
                        (merged_prcs["PClosing"] == 0)
                        | (merged_prcs["PClosing"] == 1000)
                    )
                ]["PClosing"] = merged_prcs["yday+1"]

            # drop temporary columns
            candid_drop_cols = [
                "upd",
                "Prev_CComVal",
                "Prev_Symbol",
                "Curr_Symbol",
                "yday+1",
                "CComVal",
            ]
            drop_cols = [
                drop_col
                for drop_col in candid_drop_cols
                if drop_col in merged_prcs.columns
            ]
            merged_prcs = merged_prcs.drop(
                drop_cols,
                axis=1,
            )
            self._prices_merged = merged_prcs

    def _read_instrums(self):
        """
        reads list of all cached instruments
        and updates "instruments" and "merged_instruments" properties
        """

        table_name = "instruments"
        instrums = self._read_table(table=table_name)
        if instrums is not None and (not instrums.empty):
            self._instruments = instrums
            if self.settings["merge_similar_symbols"]:
                instrums = self._instruments
                instrums["Duplicated"] = instrums["Symbol"].duplicated(keep=False)
                instrums["IsRoot"] = ~instrums["Symbol"].duplicated(keep="first")

    def _read_splits(self):
        """
        reads stock splits and their dates from cache file an updates splits property
        """

        table_name = "splits"
        splits = self._read_table(table=table_name)
        if splits is not None and (not splits.empty):
            self._splits = splits

    def _read_table(self, table: str) -> pd.DataFrame | None:
        """
        reads a table from database and returns its data

        :table: str, name of the table

        :return: pd.DataFrame, data read from the table.
        returns 'None' if the table doesn't exist.
        """

        # check if table exists in db
        check_query = (
            f"SELECT name FROM sqlite_schema WHERE type='table' AND name='{table}';"
        )
        find_table = pd.read_sql(sql=check_query, con=self._cnx)
        if find_table.empty:
            return None
        else:
            query = f"SELECT * FROM {table}"
            data = pd.read_sql(sql=query, con=self._cnx)
            return data

    def get_instrum_prcs(self, instrument: dict, settings: dict) -> pd.DataFrame:
        """
        get cached instrument prices

        :instrument: dict, instrument to get prices for
        :settings: dict, app config from the config file or user input

        :return: pd.DataFrame, prices for instrument
        """

        if self._prices_merged is None or self._instruments is None:
            raise AttributeError("Some required data is missing in cache.")
        ins_code = instrument["InsCode"]
        merges = self._instruments[self._instruments["Duplicated"]]
        prices_merged = self._prices_merged

        prices = pd.DataFrame()
        ins_codes = []

        if not instrument["IsRoot"]:
            # Old and inactive instruments
            if settings["merge_similar_symbols"]:
                return pd.DataFrame()
            prices = self.prices[ins_code]
            ins_codes = [ins_code]
        else:
            # Active instrument with similar inactive symbols
            is_head = instrument["InsCode"] in merges.InsCode.values
            if is_head and settings["merge_similar_symbols"]:
                # TODO: why is this "if" needed?
                if ins_code in prices_merged:
                    prices = prices_merged[ins_code]
                ins_codes = merges[merges["Symbol"] == instrument["Symbol"]][
                    "InsCode"
                ].values
            else:
                # TODO: why is this "if" needed?
                if ins_code in self._prices:
                    prices = self._prices[ins_code]
                    ins_codes = [ins_code]

        if prices.empty:
            return prices

        prices = self.adjust(settings["adjust_prices"], ins_codes)

        if not settings["days_without_trade"]:
            prices = prices[prices["ZTotTran"] > 0]

        # TODO: should be higher in this method
        prices = prices[prices.index.levels[1] > int(settings["start_date"])]

        return prices

    def adjust(self, cond: int, ins_codes: list[int]) -> pd.DataFrame:
        """
        Adjust closing prices according to the condition
        0: make no adjustments
        1: adjust according to dividends and splits
        2: adjust according to splits
        3: adjust according to cash dividends

        :cond: int, price adjust type. can be 0 (no adjustment), 1, 2 or 3
        :ins_codes: list, instrument codes

        :return: pd.DataFrame, adjusted closing prices

        :raises: ValueError, if there is no price data in cache
        """

        if self.prices is not None:
            prices = self.prices[
                self.prices.index.isin(ins_codes, level="InsCode")
            ].sort_index(level=1)
            # remove every price data before DEF_START.
            # before this date, server has no reliable data.
            prices = prices[
                prices.index.get_level_values("DEven") > self.settings["DEF_START"]
            ]
            cp_len = len(prices)
            if cp_len < 2:
                return prices
            price_cols = list(prices.columns)
            if len(ins_codes) > 1:
                # find stock moves between markets
                # and adjust for nominal price in the new market, if necessary
                prices["ShiftedClose"] = prices["PClosing"].shift(1)
                nom_idxs = prices.groupby("InsCode").head(1).index[1:]
                default_prs = self.settings["NOMINAL_PRICES"]
                prices["nom_pr"] = (
                    prices["PClosing"].isin(default_prs)
                    & prices.index.isin(nom_idxs)
                    & (prices["ShiftedClose"] != prices["PriceYesterday"])
                )
                while not prices[prices["nom_pr"]].empty:
                    # index of first and last row with nominal price
                    first_idx = prices[prices["nom_pr"]].index[0]
                    cond_1 = prices.index.isin([first_idx[0]], level="InsCode")
                    cond_2 = prices["QTotTran5J"] != 0
                    last_idx = prices.loc[cond_1 & cond_2].index[0]

                    # the price to replace nominal price
                    rep_pr = prices.loc[first_idx, "ShiftedClose"]

                    prices.loc[first_idx:last_idx, "PClosing"].iloc[:-1] = rep_pr
                    prices.loc[first_idx:last_idx, "PriceYesterday"] = rep_pr
                    prices.loc[first_idx:last_idx, "nom_pr"].iloc[0] = False
            if not cond:
                return prices
            if cond in [1, 3]:
                prices["ShiftedYDay"] = prices["PriceYesterday"].shift(-1)
                prices["AdjMultiplr"] = (
                    prices["ShiftedYDay"] / prices["PClosing"]
                ).fillna(1)
            if cond in [2, 3]:
                if (self._splits is not None) and (not self._splits.empty):
                    filtered_splits = self._splits[
                        self._splits.index.isin(ins_codes, level="InsCode")
                    ]
                    filtered_splits["SplitMultiplr"] = (
                        filtered_splits["NumberOfShareOld"]
                        / filtered_splits["NumberOfShareNew"]
                    )
                    prices = prices.join(filtered_splits[["SplitMultiplr"]]).fillna(1)
                else:
                    prices["SplitMultiplr"] = 1
            if cond == 1:
                prices["AdjMultiplrCumProd"] = (
                    prices.AdjMultiplr.iloc[::-1].cumprod().iloc[::-1]
                )
                prices["AdjPClosing"] = round(
                    prices.AdjMultiplrCumProd * prices.PClosing
                ).astype(int)
            elif cond == 2:
                prices["SplitMultiplrCumProd"] = (
                    prices.SplitMultiplr.iloc[::-1]
                    .cumprod()
                    .iloc[::-1]
                    .shift(-1, fill_value=1)
                )
                prices["AdjPClosing"] = round(
                    prices.SplitMultiplrCumProd * prices.PClosing
                ).astype(int)
            elif cond == 3:
                prices["DividMultiplr"] = prices["AdjMultiplr"]
                prices.loc[
                    ~prices["SplitMultiplr"].shift(-1).isin([1]),
                    "DividMultiplr",
                ] = 1
                prices["DividMultiplrCumProd"] = (
                    prices.DividMultiplr.iloc[::-1].cumprod().iloc[::-1]
                )
                prices["AdjPClosing"] = round(
                    prices.DividMultiplrCumProd * prices.PClosing
                ).astype(int)
            price_cols.append("AdjPClosing")
            return prices[price_cols]
        else:
            raise ValueError("No price data available.")

    def write_prc_csv(self, f_name: str, data: pd.DataFrame) -> None:
        """
        Write price data to csv file.

        :f_name: str, File name
        :data: pd.DataFrame, Stock price data
        """

        self.write_tse_csv(
            f_name=f_name,
            data=data,
            subdir=self.settings["PRICES_DIR"],
        )

    def write_tse_csv(self, f_name: str, data: pd.DataFrame, **kwargs) -> None:
        """
        Write data to csv file.

        :f_name: str, File name
        :data: pd.DataFrame, Stock price data
        """

        if "subdir" in kwargs:
            tse_dir = self._data_dir / str(kwargs.get("subdir"))
        else:
            tse_dir = self._data_dir
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        if len(data) == 0:
            return
        file_path = tse_dir / f"{f_name}.csv"
        data.to_csv(file_path, encoding="utf-8")
