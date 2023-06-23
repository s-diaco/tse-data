"""
manage cached data
"""

from datetime import datetime
from pathlib import Path

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
        self._stored_prices: pd.DataFrame | None = None
        self._stored_prices_merged: pd.DataFrame | None = None
        self.merged_instruments: pd.DataFrame | None = None
        # TODO: delete if not used
        self.last_devens: pd.Series | None = None
        self.cache_to_csv = self.settings["cache"] if "cache" in self.settings else True
        if self.cache_to_csv:
            self._init_cache_dir(settings)
        self._last_instrument_update = self._get_last_inst_upd()
        self._read_instrums_csv()
        self._read_splits_csv()

    def _get_last_inst_upd(self) -> str:
        """
        Reads a file from the cache dir and returns a string

        :return: str
        """
        key = "lastInstrumUpdate"
        tse_dir = self.cache_dir
        file_path = tse_dir / f"{key}.csv"
        if not file_path.is_file():
            with open(file_path, "w+", encoding="utf-8") as file:
                file.write("")
                return ""
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    def _init_cache_dir(self, settings) -> None:
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
        if "tse_dir" in settings:
            self._data_dir = Path(settings["tse_dir"])
        tse_logger.info("data dir: %s", self._data_dir)

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
                file_path = self.cache_dir / (f_name + ".csv")
                self._instruments.to_csv(file_path, encoding="utf-8")
                self._save_last_inst_upd()

    def _save_last_inst_upd(self):
        today = datetime.now().strftime("%Y%m%d")
        if self._last_instrument_update != today:
            self._last_instrument_update = today
            key = "lastInstrumUpdate"
            file_path = self.cache_dir / "{key}.csv"
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
                file_path = self.cache_dir / (f_name + ".csv")
                self._splits.to_csv(file_path, encoding="utf-8")
                self._save_last_inst_upd()

    @property
    def stored_prices(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._stored_prices is not None:
            if not self._stored_prices.empty:
                return self._stored_prices.sort_index()
        return None

    @property
    def stored_prices_merged(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._stored_prices_merged is not None:
            return self._stored_prices_merged.sort_index()
        return None

    def add_to_stored_prices(self, value: list[pd.DataFrame]):
        """
        Adds a list of dataframes to "stored_prices" property.
        """

        if value:
            value = [data for data in value if not data.empty]
            if self._stored_prices is None:
                self._stored_prices = pd.concat(value)
            else:
                tot_prices = [self._stored_prices]
                tot_prices.extend(value)
                self._stored_prices = pd.concat(tot_prices)

    @property
    def last_instrument_update(self):
        """last date of updating list of instrument and splits"""
        return self._last_instrument_update

    def read_prc_csv(self, selected_syms: pd.DataFrame):
        """
        updates a dicts of prices for ins_codes in
        self.stored_prices and self.stored_prices_merged
        """

        # TODO: do not load all price files at one. there may be hundreds of them.
        prices = self._parse_prc_csv(f_names=selected_syms.index.tolist())
        if not prices.empty:
            self._stored_prices = prices
            if self.settings["merge_similar_symbols"]:
                self.refresh_prices_merged(selected_syms)

    def _parse_prc_csv(self, f_names: list[str]) -> pd.DataFrame:
        """
        Reads selected instruments files from the cache dir and returns a dict.

        :f_names: list[str], list of file names to read from.

        :return: dict
        """

        csv_dir = self.cache_dir / self.settings["PRICES_DIR"]
        prices_list = [
            pd.read_csv(
                csv_dir / f"{name}.csv",
                encoding="utf-8",
                index_col=["InsCode", "DEven"],
            )
            for name in f_names
            if (csv_dir / f"{name}.csv").is_file()
        ]
        prices_list = [prcs for prcs in prices_list if not prcs.empty]
        if prices_list:
            res = pd.concat(prices_list)
        else:
            res = pd.DataFrame()
        return res

    def refresh_prices_merged(self, selected_syms):
        """
        Updates stored_prices_merged property.
        """

        if not self._stored_prices is None:
            merged_prcs = self._stored_prices[
                self._stored_prices.index.isin(selected_syms.index, level=0)
            ]
            merged_prcs = merged_prcs.join(selected_syms["Symbol"])
            merged_prcs = merged_prcs.reset_index().set_index(["Symbol", "DEven"])
            self._stored_prices_merged = merged_prcs

    def _read_instrums_csv(self):
        """
        reads list of all cached instruments
        and updates "instruments" and "merged_instruments" properties
        """

        f_name = "instruments"
        instrums_file = self.cache_dir / f"{f_name}.csv"
        if instrums_file.is_file():
            try:
                instrums = pd.read_csv(
                    instrums_file, encoding="utf-8", index_col="InsCode"
                )
                self._instruments = instrums
                if self.settings["merge_similar_symbols"]:
                    instrums = self._instruments
                    instrums["Duplicated"] = instrums["Symbol"].duplicated(keep=False)
                    instrums["IsRoot"] = ~instrums["Symbol"].duplicated(keep="first")
            except pd.errors.EmptyDataError:
                pass

    def _read_splits_csv(self):
        """
        reads stock splits and their dates from cache file an updates splits property
        """

        file = "splits"
        splits_file = self.cache_dir / f"{file}.csv"
        if splits_file.is_file():
            try:
                splits = pd.read_csv(
                    splits_file, encoding="utf-8", index_col=["InsCode", "DEven"]
                )
                self._splits = splits
            except pd.errors.EmptyDataError:
                pass

    def get_instrum_prcs(self, instrument: dict, settings: dict) -> pd.DataFrame:
        """
        get cached instrument prices

        :instrument: dict, instrument to get prices for
        :settings: dict, app config from the config file or user input

        :return: DataFrame, prices for instrument
        """

        ins_code = instrument["InsCode"]
        # Old instruments with outdated InsCode
        not_root = not instrument["IsRoot"]
        # TODO: copy values by ref
        merges = self.merged_instruments[self.merged_instruments["Duplicated"]]
        stored_prices_merged = self._stored_prices_merged

        prices = pd.DataFrame()
        ins_codes = []

        if not_root:
            if settings["merge_similar_symbols"]:
                return pd.DataFrame()
            prices = self.stored_prices[ins_code]
            ins_codes = [ins_code]
        else:
            # New instrument with similar old symbols
            is_head = instrument["InsCode"] in merges.InsCode.values
            if is_head & settings["merge_similar_symbols"]:
                # TODO: why is this "if" needed?
                if ins_code in stored_prices_merged:
                    prices = stored_prices_merged[ins_code]
                ins_codes = merges[merges["Symbol"] == instrument["Symbol"]][
                    "InsCode"
                ].values
            else:
                # TODO: why is this "if" needed?
                if ins_code in self.stored_prices:
                    prices = self.stored_prices[ins_code]
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

    def get_symbol_names(self, ins_codes: list[str]) -> dict:
        """
        retrives the symbol names

        :param ins_code: list of strings, codes of the selected symbols

        :return: dict, {code: symbol}
        """

        int_ins_codes = [int(ins_code) for ins_code in ins_codes]
        ret_val_df = self._instruments[self._instruments["InsCode"].isin(int_ins_codes)]
        ret_val = {
            row["InsCode"]: row["Symbol"]
            for row in ret_val_df.to_dict(orient="records")
        }
        return ret_val

    def adjust(
        self,
        cond: int,
        closing_prices: pd.DataFrame,
        ins_codes: list[int],
    ):
        """
        Adjust closing prices according to the condition
        0: make no adjustments
        1: adjust according to dividends and splits (yday / close of yesterday)
        2: adjust according to splits
        3: adjust according to cash dividends

        :cond: int, price adjust type. can be 0 (no adjustment), 1 or 2
        :closing_prices: pd.DataFrame, prices (daily time frame) for a stock symbol
        :splits: pd.DataFrame, stock splits and their dates
        :ins_codes: list, instrument codes

        :return: pd.DataFrame, adjusted closing prices
        """

        # TODO: use only new method after timing both
        # TODO: should work when there is multple codes
        new_method = True
        if new_method:
            cl_pr = closing_prices
            cl_pr_cols = list(cl_pr.columns)
            cp_len = len(closing_prices)
            if cond and cp_len > 1:
                for ins_code in ins_codes:
                    filtered_shares = self._splits[
                        self._splits.index.isin([ins_code], level="InsCode")
                    ]
                    if cond in [1, 3]:
                        cl_pr["ShiftedYDay"] = cl_pr["PriceYesterday"].shift(-1)
                        cl_pr["YDayDiff"] = cl_pr["PClosing"] / cl_pr["ShiftedYDay"]
                    if cond in [2, 3]:
                        cl_pr = cl_pr.join(filtered_shares[["StockSplits"]]).fillna(0)
                        filtered_shares["StockSplits"] = (
                            filtered_shares["NumberOfShareNew"]
                            / filtered_shares["NumberOfShareOld"]
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

        filtered_shares = self._splits[
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
                        and filtered_shares.index.isin(
                            [next_prcs.DEven], level="DEven"
                        ).any()
                    ):
                        target_share = filtered_shares.xs(
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
