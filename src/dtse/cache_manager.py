"""
manage cached data
"""

from dtse.data_services import adjust
import pandas as pd

from dtse.tse_parser import parse_instruments, parse_shares

from dtse.storage import Storage
from dtse import config as cfg


class TSECache:
    """
    price data cached in csv files
    """

    def __init__(self, **tse_cache_kwargs) -> None:
        """
        initialize TSECachedPrices class

        :selected_syms: list, instrument codes to look for in cached data
        """
        self.storage = Storage()
        self.symbol_as_dict_key: bool = False
        self.selected_syms = pd.DataFrame()
        self._instruments=pd.DataFrame()
        self.stored_prices = {}
        self.stored_prices_merged = {}
        self.merges = []
        self.splits = pd.DataFrame()
        self.merged_instruments = pd.DataFrame()
        self.settings = {}
        if tse_cache_kwargs:
            self.settings.update(tse_cache_kwargs)
        self._refresh_instrums()
        self.refresh_splits()
        self.last_devens = {}
        self.last_instrument_update=self.strg.get_item("tse.lastInstrumentUpdate")

    @property
    def instruments(self):
        """All instruments and their details."""
        return self._instruments

    @instruments.setter
    def radius(self, value:pd.Dataframe):
        self._instruments = value

    def refresh_prices(self, selected_syms: pd.DataFrame, symbol_as_dict_key=False):
        """
        updates a dicts of prices for ins_codes in
        self.stored_prices and self.stored_prices_merged
        """

        self.selected_syms = selected_syms
        prc_dict = self.strg.get_items(f_names=self.selected_syms.InsCode.tolist())
        if symbol_as_dict_key:
            self.symbol_as_dict_key = symbol_as_dict_key
            self.stored_prices = {
                k: v.set_index(["InsCode", "DEven"])
                for k, v in prc_dict.items()
                if not v.empty
            }
        else:
            self.stored_prices = {
                v.InsCode[0]: v.set_index(["InsCode", "DEven"])
                for _, v in prc_dict.items()
                if not v.empty
            }
        if self.settings["merge_similar_symbols"]:
            self.refresh_prices_merged(selected_syms)

    def refresh_prices_merged(self, selected_syms):
        """
        updates a dicts of prices for ins_codes in self.stored_prices_merged
        """

        code_groups = [
            selected_syms[selected_syms["Symbol"].isin([sym])]["InsCode"]
            for sym in selected_syms[
                selected_syms["Symbol"].duplicated(keep=False)
            ].Symbol.unique()
        ]
        for codes in code_groups:
            # TODO: why the first one? test [26787658273107220, 68635710163497089] codes
            latest = codes.iloc[0]
            # TODO: why is this "if" needed?
            similar_prcs = [
                self.stored_prices[code]
                for code in codes.values
                if code in self.stored_prices
            ]
            if similar_prcs:
                similar_prcs.reverse()
                self.stored_prices_merged[latest] = pd.concat(similar_prcs)

    def _refresh_instrums(self, **kwargs):
        """
        updates list of all cached instruments
        and updates "instruments" and "merged_instruments" variables
        """

        self.instruments = parse_instruments(**kwargs)
        self.merged_instruments = self._merge_similar_syms()

    def refresh_splits(self, **kwargs) -> pd.DataFrame:
        """
        updates stock splits and their dates for each symbol
        """

        self.splits = parse_shares(**kwargs)
        if len(self.splits.index):
            self.splits = self.splits.set_index(keys=["InsCode", "DEven"])

    def _merge_similar_syms(self) -> pd.DataFrame:
        """
        Process similar symbols an add "SymbolOriginal" column to DataFrame

        :return: pd.DataFrame, processed dataframe
        """

        postfix = self.settings.pop("SYMBOL_RENAME_STRING", cfg.SYMBOL_RENAME_STRING)
        instrums = self.instruments.sort_values(by="DEven", ascending=False)
        instrums["Duplicated"] = instrums["Symbol"].duplicated(keep=False)
        instrums["IsRoot"] = ~instrums["Symbol"].duplicated(keep="first")
        instrums["SymbolOriginal"] = instrums["Symbol"]
        instrums.loc[~instrums["IsRoot"], "Symbol"] = (
            instrums["Symbol"]
            + postfix
            + instrums.groupby(["Symbol"]).cumcount().astype("string")
        )
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
            is_head = instrument["InsCode"] in merges.InsCode.values & settings["merge_similar_symbols"]:
            if is_head:
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
