"""
Manage TSE Data
"""

import pandas as pd

from dtse import config as cfg
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse.price_update_helper import PricesUpdateManager
from dtse.progress_bar import ProgressBar
from dtse.setup_logger import logger as tse_logger


class TSE:
    """
    Manage TSE Data
    """

    def __init__(self):
        self.settings = cfg.default_settings
        self.progressbar = ProgressBar()

    async def _get_expired_prices(self, selection) -> pd.DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instruments to check

        :return: pd.DataFrame, symbols to update
        """

        cache = self.cache
        to_update = selection[["DEven", "YMarNSC"]]
        # TODO: Ensure it returns the last value not the first one.
        # is it good to store last_devens in a file?
        if cache.prices is not None:
            # TODO: delete if inscode_lastdeven file is not used
            cache.last_devens = (
                cache.prices.reset_index().groupby("InsCode")["DEven"].max()
            )
        first_possible_deven = self.settings["start_date"]
        # TODO: method called twice (in "data_services.update_instruments()"). why?
        cache.last_possible_deven = await data_svs.get_last_possible_deven()
        # merge selection with last_devens (from cached data)
        # to find out witch syms need an update
        to_update["cached_DEven"] = cache.last_devens
        if cache.last_devens is None:
            to_update["cached_DEven"] = int(first_possible_deven)
        else:
            to_update = selection.merge(
                cache.last_devens.rename("cached_DEven"), how="left", on="InsCode"
            )
            # symbol doesn't have data
            to_update.cached_DEven = to_update.cached_DEven.fillna(
                first_possible_deven
            ).astype("int64")

        # symbol has data but outdated
        to_update["need_upd"] = to_update["cached_DEven"].map(
            lambda deven: data_svs.should_update(str(deven), cache.last_possible_deven)
        )
        to_update = to_update[to_update["need_upd"]][
            ["cached_DEven", "YMarNSC"]
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

        # TODO: check the dict
        cache_cfg = cfg.storage
        cache_cfg.update(self.settings)
        self.cache = TSECache(cache_cfg)
        await data_svs.update_instruments(self.cache)
        if self.cache.instruments is None:
            raise ValueError(f"No instruments loaded.")
        instruments = self.cache.instruments
        # TODO: does it return the full list before 8:30 a.m.?
        tse_logger.info("Loaded %s instruments.", len(instruments))
        selected_syms = instruments[instruments["Symbol"].isin(symbols)]
        tse_logger.info(
            "%s Symbols loaded (including duplicated symbols).", len(selected_syms)
        )
        if selected_syms.empty:
            raise ValueError(f"No instruments found for any symbols.")
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

        self.cache.read_prices(selected_syms=selected_syms)
        to_update = await self._get_expired_prices(selected_syms)
        price_manager = PricesUpdateManager(cache_manager=self.cache)
        update_result = await price_manager.start(
            upd_needed=to_update,
            settings=self.settings,
            progressbar=self.progressbar,
        )
        tse_logger.info("Data download ended.")
        if not update_result["succs"]:
            tse_logger.info("No data downloaded.")
        if update_result["fails"]:
            tse_logger.warning(
                "Failed to get some data. codes: %s", update_result["fails"]
            )
        res = update_result
        if self.settings["merge_similar_symbols"]:
            df = self.cache.prices_merged
            res = {sym: df.xs(sym) for sym in df.index.levels[0]}

            for sym, data in res.items():
                filename = f"{sym}"
                self.cache.write_prc_csv(f_name=filename, data=data)

        """
        progressbar.prog_n = update_result
        pi = progressbar.prog_tot * 0.20 / selected_syms_df.length
        """

        return res
