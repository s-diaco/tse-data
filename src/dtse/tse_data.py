"""
Manage TSE Data
"""

from dataclasses import asdict
import pandas as pd

from dtse import config as cfg
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse.price_updater import PriceUpdater
from dtse.logger import logger as tse_logger


class TSE:
    """
    Manage TSE Data
    """

    def __init__(self):
        self.settings = cfg.default_settings
        self.codes: list[int] = []

    async def _get_expired_prices(self) -> pd.DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instrument codes to check

        :return: pd.DataFrame, symbols to update
        """

        first_possible_deven = self.settings["start_date"]
        sel_insts = self.cache.instruments.loc[self.codes]
        if self.cache.last_devens is not None and not self.cache.last_devens.empty:
            sel_insts = sel_insts.join(
                self.cache.last_devens["LastDEven"]
                .astype("Int64")
                .rename("cached_DEven")
            ).fillna(int(first_possible_deven))
        else:
            sel_insts["cached_DEven"] = first_possible_deven

        sel_insts["outdated"] = sel_insts["cached_DEven"].map(
            lambda deven: data_svs.should_update(
                str(deven),
                self.cache.last_possible_deven,
            )
        )
        sel_insts["NotInNoMarket"] = (sel_insts.YMarNSC != "NO").astype(int)
        outdated_insts = sel_insts.loc[
            sel_insts["outdated"], ["cached_DEven", "NotInNoMarket"]
        ].rename({"cached_DEven": "DEven"}, axis=1)
        return outdated_insts

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

        # Initialize cache
        cache_cfg = cfg.storage
        cache_cfg.update(self.settings)
        self.cache = TSECache(cache_cfg)

        # Get the latest instuments data
        await data_svs.update_instruments(self.cache)
        if self.cache.instruments is None:
            raise ValueError("No instruments loaded.")

        # TODO: does it return the full list before 8:30 a.m.?
        tse_logger.info("Loaded %s instruments.", len(self.cache.instruments))
        selected_syms = self.cache.instruments.loc[
            self.cache.instruments["Symbol"].isin(symbols)
        ]
        # TODO: use codes instead of selected_syms.
        self.codes = list(selected_syms.index)
        tse_logger.info("%s codes loaded.", len(selected_syms))
        if selected_syms.empty:
            raise ValueError("No instruments found for any symbols.")
        not_founds = [
            sym for sym in symbols if sym not in selected_syms["Symbol"].values
        ]
        # TODO: does آ اس پ create any problems?
        if not_founds:
            tse_logger.warning("symbols not found: %s", ",".join(not_founds))

        self.cache.read_prices(selected_syms=selected_syms)
        to_update = await self._get_expired_prices()
        if not to_update.empty:
            price_manager = PriceUpdater(cache=self.cache)
            update_result = await price_manager.update_prices(outdated_insts=to_update)
            if complete_dl := update_result["succs"]:
                tse_logger.info(
                    "Data download completed for codes: %s",
                    ", ".join([str(x) for x in complete_dl]),
                )
            else:
                tse_logger.info("No data downloaded.")
            if update_result["fails"]:
                tse_logger.warning(
                    "Failed to get some data. codes: %s",
                    ",".join(update_result["fails"]),
                )
        # drop index columns from the list of columns
        cols = list(asdict(cfg.PriceColNames()).values())[2:]
        res = self.cache.prices_by_symbol(symbols=symbols, cols=cols)

        if self.settings["write_csv"]:
            self.cache.write_prc_csv(res)

        return res
