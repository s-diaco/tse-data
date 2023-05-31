"""
Manage TSE Data
"""


import numbers
import re

from pandas import DataFrame

from dtse.cache_manager import TSECache

from . import config as cfg
from . import data_services as data_svs
from .data_structs import TSEColumn, TSEInstrument
from .price_update_helper import PricesUpdateHelper
from .progress_bar import ProgressBar
from .setup_logger import logger as tse_logger
from .storage import Storage
from .tse_parser import parse_instruments


class TSE:
    """
    Manage TSE Data
    """

    tse_cache: TSECache
    progressbar: ProgressBar

    def __init__(self):
        self.settings = cfg.default_settings
        self.progressbar = ProgressBar()

    async def _filter_expired_prices(
        self,
        selection,
    ) -> DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instruments to check
        :self.settings: dict, configs for the module
        :percents: dict, progress bar data

        :return: DataFrame, symbols to update
        """

        cache_mngr = self.tse_cache
        # TODO: do not convert to string (remove astype(str))
        # TODO: stored_prices shouldn't be in PricesUpdateHelper Class
        to_update = selection[["InsCode", "DEven", "YMarNSC"]]
        # TODO: Ensure it returns the last value not the first one.
        # is it good to store last_devens in a file?
        last_devens = DataFrame(
            {
                prc_df.iloc[-1]["InsCode"]: prc_df.iloc[-1]["DEven"]
                for prc_df in cache_mngr.stored_prices.values()
            }.items(),
            columns=["InsCode", "cached_DEven"],
        ).astype("int64")
        first_possible_deven = self.settings["start_date"]
        if not last_devens.empty:
            last_possible_deven = await data_svs.get_last_possible_deven()
            # merge selection with last_devens (from cached data) to find out witch syms need an update
            sel_merged = selection.merge(last_devens, how="left", on="InsCode")
            # symbol doesn't have data
            sel_merged.cached_DEven = sel_merged.cached_DEven.fillna(
                first_possible_deven
            ).astype("int64")
            # symbol has data but outdated
            sel_merged["need_upd"] = sel_merged["cached_DEven"].map(
                lambda deven: data_svs.should_update(str(deven), last_possible_deven)
            )
            to_update = sel_merged[sel_merged["need_upd"]][
                ["InsCode", "cached_DEven", "YMarNSC"]
            ].rename({"cached_DEven": "DEven"})
        """
        prog_fin = progressbar.pn + progressbar.ptot
        """
        """
        if callable(progressbar.progress_func):
            progressbar.progressbar.prog_func(progressbar.pn + progressbar.ptot * (0.01))
        """
        """
        progress_tuple = (
            progressbar.progress_func,
            progressbar.pn,
            progressbar.ptot - progressbar.ptot * (0.02),
        )"""
        """
        progressbar.pn = progressbar.prog_n
        """
        # TODO: price update helper Should update inscode_lastdeven file
        # with new cached instruments in _on_result or do not read it from this file
        """
        if callable(progressbar.progress_func) and progressbar.pn != prog_fin:
            progressbar.progressbar.prog_func(prog_fin)
        result.pn = prog_fin"""

        to_update["InNormalMarket"] = (to_update.YMarNSC != "NO").astype(int)
        to_update = to_update.drop("YMarNSC", axis=1)
        return to_update

    async def get_prices(self, symbols, **kwconf):
        """
        get prices for symbols

        :symbols: list, symbols to get prices for

        :return: dict, prices for symbols
        """

        if not symbols:
            return
        self.settings = cfg.default_settings
        if kwconf:
            self.settings.update(kwconf)
        """
        progressbar.prog_func = self.settings.get("on_progress")
        if not callable(progressbar.prog_func):
            progressbar.prog_func = None
        progressbar.prog_tot = self.settings.get("progress_tot")
        if not isinstance(progressbar.prog_tot, numbers.Number):
            progressbar.prog_tot = cfg.default_settings["progress_tot"]
        progressbar.prog_n = 0
        """
        """
        if callable(progressbar.prog_func):
            progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
            progressbar.prog_func(progressbar.prog_n)
        """

        await data_svs.update_instruments()
        self.tse_cache = TSECache()
        instruments = self.tse_cache.instruments
        # TODO: does it return the full list before 8:30 a.m.?
        # check if names in symbols are valid symbol names
        selected_syms = instruments[instruments["Symbol"].isin(symbols)]
        if selected_syms.empty:
            raise ValueError(f"No instruments found for symbols: {symbols}.")
        not_founds = [
            sym for sym in symbols if sym not in selected_syms["Symbol"].values
        ]
        if not_founds:
            tse_logger.warning("symbols not found: %s", not_founds)

        """
        if callable(progressbar.prog_func):
            progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
            progressbar.prog_func(progressbar.prog_n)
            if callable(progressbar.prog_func):
                progressbar.prog_func(progressbar.prog_tot)
        """

        self.tse_cache.refresh_prices(selected_syms=selected_syms)
        to_update = await self._filter_expired_prices(selected_syms)
        price_manager = PricesUpdateHelper(cache_manager=self.tse_cache)
        update_result = await price_manager.start(
            update_needed=to_update,
            settings=self.settings,
            progressbar=self.progressbar,
        )
        self.tse_cache.refresh_prices(selected_syms=selected_syms)
        for inst in selected_syms.to_dict(orient="records"):
            self._get_instrument_prices(inst)

        """
        progressbar.prog_n = update_result
        if error:
            err = (error.title, error.detail)
            result["error"] = (1, err)
            if callable(progressbar.prog_func):
                progressbar.prog_func(progressbar.prog_tot)
            return result

        if fails:
            syms = [(i.ins_code, i.SymbolOriginal) for i in selected_syms_df]
            title = "Incomplete Price Update"
            succs = list(map((lambda x: syms[x]), succs))
            fails = list(map((lambda x: syms[x]), fails))
            err = (title, succs, fails)
            result["error"] = (3, err)
            for v, i, a in selected_syms_df:
                if fails.include(v.ins_code):
                    a[i] = None
                else:
                    a[i] = 0
        if merge_similar_symbols:
            selected_syms_df = selected_syms_df[:extras_index]

        columns = self.settings["columns"]

        def col(col_name):
            row = col_name
            column = TSEColumn(row)
            final_header = column.header or column.name
            return {column, final_header}

        columns = list(map(col, self.settings["columns"]))
        """
        """
        pi = progressbar.prog_tot * 0.20 / selected_syms_df.length
        """

        return res

    # TODO: delete
    """
    async def get_instruments(self, struct=True, arr=True, struct_key="InsCode"):
        get instruments

        :struct: bool, return structure
        :arr: bool, return array
        :structKey: str, key to use for structure

        :return: dict, instruments
        valids = None
        # TODO: complete
        if valids.indexOf(struct_key) == -1:
            struct_key = "InsCode"

        last_update = strg.get_item("tse.lastInstrumentUpdate")
        err = await data_svs.update_instruments()
        if err and not last_update:
            raise err
        return await strg.read_tse_csv("tse.instruments")
    """

    def _get_instrument_prices(self, instrument) -> DataFrame:
        """
        get instrument prices

        :instrument: dict, instrument to get prices for

        :return: DataFrame, prices for instrument
        """

        ins_code = instrument["InsCode"]
        sym_orig = instrument.pop("SymbolOriginal", None)
        # TODO: copy values by ref
        merges = self.tse_cache.merged_instruments[
            self.tse_cache.merged_instruments["Duplicated"]
        ]
        stored_prices_merged = self.tse_cache.stored_prices_merged
        shares = self.tse_cache.shares

        prices = DataFrame()
        ins_codes = []

        if sym_orig:
            if self.settings["merge_similar_symbols"]:
                return self.settings["MERGED_SYMBOL_CONTENT"]
            prices = self.tse_cache.stored_prices[ins_code]
            ins_codes = [ins_code]
        else:
            is_root = instrument["IsRoot"]
            prices = (
                self.tse_cache.stored_prices[ins_code],
                stored_prices_merged[ins_code],
            )[is_root]
            ins_codes = ([ins_code], merges["InsCode"].values)[is_root]

        if not prices:
            return

        if self.settings["adjust_prices"] == 1 or self.settings["adjust_prices"] == 2:
            prices = data_svs.adjust(
                self.settings["adjust_prices"], prices, shares, ins_codes
            )

        if not self.settings["days_without_trade"]:
            prices = prices[prices["ZTotTran"] > 0]

        prices = prices[prices["DEven" > self.settings["start_date"]]]

        return prices
