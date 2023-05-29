"""
manage cached data
"""

import pandas as pd

from dtse.tse_parser import parse_instruments

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

    def refresh_prices(self, selected_syms: pd.DataFrame, symbol_as_dict_key=False):
        """
        updates a dict of last devens for ins_codes in self.stored_prices
        """

        strg = Storage()
        self.selected_syms = selected_syms
        prc_dict = strg.get_items(f_names=self.selected_syms.Symbol.tolist())
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

    def _merge_similar_syms(self) -> pd.DataFrame:
        """
        Process similar symbols an add "SymbolOriginal" column to DataFrame

        :return: pd.DataFrame, processed dataframe
        """

        postfix = self.settings.pop("SYMBOL_RENAME_STRING", cfg.SYMBOL_RENAME_STRING)
        instrums = self.instruments.sort_values(by="DEven", ascending=False)
        instrums.loc[instrums["Symbol"].duplicated(), "SymbolOriginal"] = instrums[
            "Symbol"
        ]
        instrums.loc[instrums["Symbol"].duplicated(), "SymbolRenamed"] = (
            instrums["Symbol"]
            + postfix
            + instrums.groupby(["Symbol"]).cumcount().astype("string")
        )
        instrums.Symbol = instrums.SymbolRenamed.fillna(instrums.Symbol)
        instrums = instrums.drop(columns=["SymbolRenamed"])
        return instrums
