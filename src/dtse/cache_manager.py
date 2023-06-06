"""
manage cached data
"""

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
        self.symbol_as_dict_key: bool = False
        self.selected_syms = pd.DataFrame()
        self.stored_prices = {}
        self.stored_prices_merged = {}
        self.merges = []
        self.shares = {}
        self.instruments = pd.DataFrame()
        self.merged_instruments = pd.DataFrame()
        self.settings = {}
        if tse_cache_kwargs:
            self.settings.update(tse_cache_kwargs)
        self.refresh_instrums()
        self.refresh_shares()
        self.last_devens = {}

    def refresh_prices(self, selected_syms: pd.DataFrame, symbol_as_dict_key=False):
        """
        updates a dict of last devens for ins_codes in self.stored_prices
        """

        strg = Storage()
        self.selected_syms = selected_syms
        prc_dict = strg.get_items(f_names=self.selected_syms.InsCode.tolist())
        if symbol_as_dict_key:
            self.symbol_as_dict_key = symbol_as_dict_key
            self.stored_prices = {k: v for k, v in prc_dict.items() if not v.empty}
        else:
            self.stored_prices = {
                v.InsCode[0]: v for _, v in prc_dict.items() if not v.empty
            }

    def refresh_instrums(self, **kwargs):
        """
        updates list of all cached instruments
        and updates "instruments" and "merged_instruments" variables
        """
        self.instruments = parse_instruments(**kwargs)
        self.merged_instruments = self._merge_similar_syms()

    def refresh_shares(self, **kwargs):
        """
        updates cached share numbers (changes in total shares for each symbol)
        """

        self.shares = parse_shares(**kwargs)

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
