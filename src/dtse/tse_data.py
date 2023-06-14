"""
Manage TSE Data
"""

import pandas as pd
from pandas import DataFrame

from dtse.cache_manager import TSECache

from . import config as cfg
from . import data_services as data_svs
from .price_update_helper import PricesUpdateManager
from .progress_bar import ProgressBar
from .setup_logger import logger as tse_logger


class TSE:
    """
    Manage TSE Data
    """

    tse_cache: TSECache
    progressbar: ProgressBar

    def __init__(self):
        self.settings = cfg.default_settings
        self.progressbar = ProgressBar()

    async def _get_expired_prices(self, selection) -> DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instruments to check
        :self.settings: dict, configs for the module
        :percents: dict, progress bar data

        :return: DataFrame, symbols to update
        """

        cache = self.tse_cache
        to_update = selection[["InsCode", "DEven", "YMarNSC"]]
        # TODO: Ensure it returns the last value not the first one.
        # is it good to store last_devens in a file?
        last_devens = DataFrame(
            {
                prc_df.iloc[-1].name[0]: prc_df.iloc[-1].name[1]
                for prc_df in cache.stored_prices.values()
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

        to_update["NotInNoMarket"] = (to_update.YMarNSC != "NO").astype(int)
        to_update = to_update.drop("YMarNSC", axis=1)
        return to_update

    async def get_prices(self, symbols: list[str], **kwconf) -> dict:
        """
        get prices for symbols

        :symbols: list, symbols to get prices for

        :return: dict, prices for symbols
        """

        if not symbols:
            tse_logger.warning("No symbols requested")
            return {}
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

        self.tse_cache = TSECache(
            merge_similar_symbols=self.settings["merge_similar_symbols"],
            cache=self.settings["cache"],
        )
        await data_svs.update_instruments(self.tse_cache)
        instruments = self.tse_cache.instruments
        # TODO: does it return the full list before 8:30 a.m.?
        selected_syms = instruments[instruments["Symbol"].isin(symbols)]
        if selected_syms.empty:
            raise ValueError(f"No instruments found for symbol names: {symbols}.")
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
        to_update = await self._get_expired_prices(selected_syms)
        price_manager = PricesUpdateManager(cache_manager=self.tse_cache)
        update_result = await price_manager.start(
            update_needed=to_update,
            settings=self.settings,
            progressbar=self.progressbar,
        )
        if self.settings["merge_similar_symbols"]:
            self.tse_cache.refresh_prices_merged(selected_syms=selected_syms)
        res = {}
        for inst in self.tse_cache.merged_instruments[
            self.tse_cache.merged_instruments["SymbolOriginal"].isin(
                selected_syms["Symbol"].values
            )
        ].to_dict(orient="records"):
            res[inst["Symbol"]] = self.tse_cache.get_instrument_prices(
                inst, self.settings
            )

        """
        progressbar.prog_n = update_result
        pi = progressbar.prog_tot * 0.20 / selected_syms_df.length
        """

        return res
