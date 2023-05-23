"""
manage cached data
"""

import pandas as pd
from .storage import Storage


class TSECachedData:
    """
    price data cached in csv files
    """

    def __init__(self, selected_syms: pd.DataFrame) -> None:
        """
        initialize TSECachedPrices class

        :selected_syms: list, instrument codes to look for in cached data
        """
        self.symbol_as_dict_key: bool = False
        self.selected_syms = selected_syms
        self.stored_prices = {}
        self.merges = []
        self.stored_prices_merged = {}
        self.shares = {}
        self.update_stored_prices()

    def update_stored_prices(self, symbol_as_dict_key=False):
        """
        updates a dict of last devens for ins_codes in self.stored_prices
        """

        strg = Storage()
        prc_dict = strg.get_items(f_names=self.selected_syms.Symbol.values)
        if symbol_as_dict_key:
            self.symbol_as_dict_key = symbol_as_dict_key
            self.stored_prices = {k: v for k, v in prc_dict.items() if not v.empty}
        else:
            self.stored_prices = {
                v.InsCode[0]: v for _, v in prc_dict.items() if not v.empty
            }
