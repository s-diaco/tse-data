"""
manage cached data
"""

from datetime import datetime

import numpy as np
import pandas as pd

from dtse.storage import Storage
from dtse.tse_parser import parse_instruments, parse_shares


class TSECache:
    """
    price data cached in csv files
    """

    def __init__(self, **tse_cache_kwargs) -> None:
        """
        initialize TSECachedPrices class
        """

        self.storage = Storage()
        self.selected_syms = pd.DataFrame()
        self._instruments = pd.DataFrame()
        self._splits = pd.DataFrame()
        self.stored_prices = pd.DataFrame()
        self.stored_prices_merged = pd.DataFrame()
        self.splits = pd.DataFrame()
        self.merges = []
        self.merged_instruments = pd.DataFrame()
        self.settings = {}
        if tse_cache_kwargs:
            self.settings.update(tse_cache_kwargs)
        self._read_instrums_csv()
        self.read_splits_csv()
        self.last_devens = {}
        self._last_instrument_update = self.storage.get_item("tse.lastInstrumUpdate")

    @property
    def instruments(self):
        """All instruments and their details."""
        return self._instruments

    @instruments.setter
    def instruments(self, value: pd.DataFrame):
        if not value.empty:
            self._instruments = value
            cache_to_file = self.settings["cache"] if "cache" in self.settings else True
            if cache_to_file:
                filename = "tse.instruments"
                self.storage.write_tse_csv_blc(f_name=filename, data=self._instruments)
                today = datetime.now().strftime("%Y%m%d")
                if self._last_instrument_update != today:
                    self._last_instrument_update = today
                    self.storage.set_item("tse.lastInstrumUpdate", today)

    @property
    def splits(self):
        """stock splits and their dates"""
        return self._splits

    @splits.setter
    def splits(self, value: pd.DataFrame):
        if not value.empty:
            self._splits = value
            cache_to_file = self.settings["cache"] if "cache" in self.settings else True
            if cache_to_file:
                filename = "tse.splits"
                self.storage.write_tse_csv_blc(f_name=filename, data=self._instruments)
                today = datetime.now().strftime("%Y%m%d")
                if self._last_instrument_update != today:
                    self._last_instrument_update = today
                    self.storage.set_item("tse.lastInstrumUpdate", today)

    @property
    def last_instrument_update(self):
        """last date of updating list of instrument and splits"""
        return self._last_instrument_update

    def read_prc_csv(self, selected_syms: pd.DataFrame):
        """
        updates a dicts of prices for ins_codes in
        self.stored_prices and self.stored_prices_merged
        """

        self.selected_syms = selected_syms
        self.stored_prices = self.storage.read_prc_csv(
            f_names=self.selected_syms.index.tolist()
        )
        if self.settings["merge_similar_symbols"] and not self.stored_prices.empty:
            self.refresh_prices_merged(selected_syms)

    def refresh_prices_merged(self, selected_syms):
        """
        Updates stored_prices_merged property.
        """

        code_groups = [
            selected_syms[selected_syms["Symbol"].isin([sym])]["InsCode"]
            for sym in selected_syms[
                selected_syms["Symbol"].duplicated(keep=False)
            ].Symbol.unique()
        ]
        for code_group_df in code_groups:
            # TODO: why the first one? test [26787658273107220, 68635710163497089] codes
            latest = code_group_df.iloc[0]
            # TODO: why is this "if" needed?
            similar_prcs = [
                self.stored_prices[code]
                for code in code_group_df.values
                if code in self.stored_prices
            ]
            if similar_prcs:
                similar_prcs.reverse()
                self.stored_prices_merged[latest] = pd.concat(similar_prcs)

    def _read_instrums_csv(self, **kwargs):
        """
        reads list of all cached instruments
        and updates "instruments" and "merged_instruments" properties
        """

        self.instruments = parse_instruments(self.storage, **kwargs)
        if not self.instruments.empty:
            self.merged_instruments = self._find_similar_syms()

    def read_splits_csv(self, **kwargs):
        """
        reads stock splits and their dates from cache file an updates splits property
        """

        self.splits = parse_shares(**kwargs)
        if len(self.splits.index):
            self.splits = self.splits.set_index(keys=["InsCode", "DEven"])

    def _find_similar_syms(self) -> pd.DataFrame:
        """
        Process similar symbols an add "SymbolOriginal" column to DataFrame

        :return: pd.DataFrame, processed dataframe
        """

        instrums = self.instruments.sort_values(by="DEven", ascending=False)
        instrums["Duplicated"] = instrums["Symbol"].duplicated(keep=False)
        instrums["IsRoot"] = ~instrums["Symbol"].duplicated(keep="first")
        return instrums

    def get_instrument_prices(self, instrument: dict, settings: dict) -> pd.DataFrame:
        """
        get cached instrument prices

        :instrument: dict, instrument to get prices for
        :cache: TSECache, local tse cache
        :settings: dict, app config from the config file or user input

        :return: DataFrame, prices for instrument
        """

        ins_code = instrument["InsCode"]
        # Old instruments with outdated InsCode
        not_root = not instrument["IsRoot"]
        # TODO: copy values by ref
        merges = self.merged_instruments[self.merged_instruments["Duplicated"]]
        stored_prices_merged = self.stored_prices_merged
        split_and_divds = self.splits

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
            prices = adjust(
                settings["adjust_prices"], prices, split_and_divds, ins_codes
            )

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
        ret_val_df = self.instruments[self.instruments["InsCode"].isin(int_ins_codes)]
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
                    filtered_shares = self.splits[
                        self.splits.index.isin([ins_code], level="InsCode")
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

        filtered_shares = self.splits[
            self.splits.index.isin(ins_codes, level="InsCode")
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
