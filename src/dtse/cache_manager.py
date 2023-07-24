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

        # remove empty data frames and first non trading days of each InsCode
        dframes = [
            data.loc[data[data["QTotTran5J"].gt(0)].index[0] :]
            for data in dframes
            if not data.empty
        ]

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

        # if settings["adjust_prices"] in [1, 2, 3]
        if settings["adjust_prices"]:
            prices = adjust(settings["adjust_prices"], prices, self.splits, ins_codes)

        if not settings["days_without_trade"]:
            prices = prices[prices["ZTotTran"] > 0]

        prices = prices[prices.index.levels[1] > int(settings["start_date"])]

        return prices

    def adjust(self, cond: int, ins_codes: list[int]):
        """
        Adjust closing prices according to the condition
        0: make no adjustments
        1: adjust according to dividends and splits (yday / close of yesterday)
        2: adjust according to splits
        3: adjust according to cash dividends

        :cond: int, price adjust type. can be 0 (no adjustment), 1 or 2
        :closing_prices: pd.DataFrame, daily prices of a stock symbol
        :splits: pd.DataFrame, stock splits and their dates
        :ins_codes: list, instrument codes

        :return: pd.DataFrame, adjusted closing prices
        """

        # TODO: use only new method after timing both
        if self.prices is not None:
            closing_prices = self.prices[
                self.prices.index.isin(ins_codes, level="InsCode")
            ].sort_index(level=1)
            new_method = True
            if new_method:
                cl_pr = closing_prices
                cl_pr_cols = list(cl_pr.columns)
                cp_len = len(closing_prices)
                if (
                    cond
                    and cp_len > 1
                    and (self._splits is not None)
                    and (not self._splits.empty)
                ):
                    filtered_splits = self._splits[
                        self._splits.index.isin(ins_codes, level="InsCode")
                    ]
                    if cond in [1, 3]:
                        cl_pr["ShiftedYDay"] = cl_pr["PriceYesterday"].shift(-1)
                        cl_pr["YDayDiff"] = cl_pr["PClosing"] / cl_pr["ShiftedYDay"]
                    if cond in [2, 3]:
                        cl_pr = cl_pr.join(filtered_splits[["StockSplits"]]).fillna(0)
                        filtered_splits["StockSplits"] = (
                            filtered_splits["NumberOfShareNew"]
                            / filtered_splits["NumberOfShareOld"]
                        )
                    if cond == 1:
                        cl_pr["YDayDiffFactor"] = (
                            (1 / cl_pr.YDayDiff.iloc[::-1])
                            .replace(np.inf, 1)
                            .cumprod()
                            .iloc[::-1]
                        )
                        cl_pr["AdjPClosing"] = cl_pr.YDayDiffFactor * cl_pr.PClosing
                    elif cond == 2:
                        cl_pr["SplitFactor"] = (
                            (1 / cl_pr.StockSplits.iloc[::-1])
                            .replace(np.inf, 1)
                            .cumprod()
                            .iloc[::-1]
                        )
                        cl_pr["AdjPClosing"] = cl_pr.SplitFactor * cl_pr.PClosing
                    elif cond == 3:
                        cl_pr["DividDiff"] = 1
                        cl_pr.loc[
                            ~cl_pr["YDayDiff"].isin([1])
                            & cl_pr["StockSplits"].isin([0]),
                            "DividDiff",
                        ] = cl_pr[["YDayDiff"]]
                        cl_pr["DividDiffFactor"] = (
                            (1 / cl_pr.DividDiff.iloc[::-1])
                            .replace(np.inf, 1)
                            .cumprod()
                            .iloc[::-1]
                        )
                        cl_pr["AdjPClosing"] = cl_pr.DividDiffFactor * cl_pr.PClosing
                    cl_pr_cols.append("AdjPClosing")
                return cl_pr[cl_pr_cols]

            filtered_splits = self._splits[
                self._splits.index.isin(ins_codes, level="InsCode")
            ]
            cl_pr = closing_prices
            cp_len = len(closing_prices)
            adjusted_cl_prices = []
            res = cl_pr
            if cond and cp_len > 1:
                gaps = 0
                num = 1
                adjusted_cl_prices.append(cl_pr.iloc[-1].to_dict())
                if cond == 1:
                    for i in range(cp_len - 2, -1, -1):
                        curr_prcs = cl_pr.iloc[i]
                        next_prcs = cl_pr.iloc[i + 1]
                        if (
                            curr_prcs.PClosing != next_prcs.PriceYesterday
                            and curr_prcs.InsCode == next_prcs.InsCode
                        ):
                            gaps += 1
                if (cond == 1 and (gaps / cp_len < 0.08)) or cond == 2:
                    for i in range(cp_len - 2, -1, -1):
                        curr_prcs = cl_pr.iloc[i]
                        next_prcs = cl_pr.iloc[i + 1]
                        prcs_dont_match = (
                            curr_prcs.PClosing != next_prcs.PriceYesterday
                        ) and (curr_prcs.InsCode == next_prcs.InsCode)
                        if cond == 1 and prcs_dont_match:
                            num = num * next_prcs.PriceYesterday / curr_prcs.PClosing
                        elif (
                            cond == 2
                            and prcs_dont_match
                            and filtered_splits.index.isin(
                                [next_prcs.DEven], level="DEven"
                            ).any()
                        ):
                            target_share = filtered_splits.xs(
                                next_prcs.DEven, level="DEven"
                            ).iloc[0]
                            old_shares = target_share["NumberOfShareOld"]
                            new_shares = target_share["NumberOfShareNew"]
                            num = num * old_shares / new_shares
                        close = round(num * float(curr_prcs.PClosing), 2)
                        last = round(num * float(curr_prcs.PDrCotVal), 2)
                        low = round(num * float(curr_prcs.PriceMin), 2)
                        high = round(num * float(curr_prcs.PriceMax), 2)
                        yday = round(num * float(curr_prcs.PriceYesterday), 2)
                        first = round(num * float(curr_prcs.PriceFirst), 2)

                        adjusted_closing_price = {
                            "InsCode": curr_prcs.InsCode,
                            "DEven": curr_prcs.DEven,
                            "PClosing": close,
                            "PDrCotVal": last,
                            "PriceMin": low,
                            "PriceMax": high,
                            "PriceYesterday": yday,
                            "PriceFirst": first,
                            "ZTotTran": curr_prcs.ZTotTran,
                            "QTotTran5J": curr_prcs.QTotTran5J,
                            "QTotCap": curr_prcs.QTotCap,
                        }
                        adjusted_cl_prices.append(adjusted_closing_price)
                    res = pd.DataFrame(adjusted_cl_prices[::-1])
            return res.astype(int)

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
