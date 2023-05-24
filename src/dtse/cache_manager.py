"""
manage cached data
"""

import pandas as pd

from dtse.tse_parser import parse_instruments

from dtse.storage import Storage


class TSECachedData:
    """
    price data cached in csv files
    """

    def __init__(self) -> None:
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

    def upd_cached_prices(self, selected_syms: pd.DataFrame, symbol_as_dict_key=False):
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

    def upd_cached_instrums(self):
        """
        updates list of all cached instruments and returns a DataFrame
        """
        self.instruments = parse_instruments()
        self._merge_similar_syms()

    def _merge_similar_syms(self) -> pd.DataFrame:
        """
        Process similar symbols an add "SymbolOriginal" column to DataFrame

        :return: pd.DataFrame, processed dataframe
        """

        instrums = self.instruments
        sym_groups = [x for x in instrums.groupby("Symbol")]
        dups = [v for v in sym_groups if len(v[1]) > 1]
        for dup in dups:
            dup_sorted = dup[1].sort_values(by="DEven", ascending=False)
            for i in range(1, len(dup_sorted)):
                instrums.loc[dup_sorted.iloc[i].name, "SymbolOriginal"] = instrums.loc[
                    dup_sorted.iloc[i].name, "Symbol"
                ]
                postfix = self.settings["SYMBOL_RENAME_STRING"] + str(i)
                instrums.loc[dup_sorted.iloc[i].name, "Symbol"] += postfix
        return instrums
