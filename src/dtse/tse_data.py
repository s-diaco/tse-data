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
        self._settings = cfg.default_settings
        self._codes: list[int] = []
        self._cache: TSECache | None = None

    async def _get_expired_prices(self) -> pd.DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instrument codes to check

        :return: pd.DataFrame, symbols to update
        """

        first_possible_deven = self._settings["start_date"]
        sel_insts = self._cache.instruments.loc[self._codes]
        if self._cache.last_devens is not None and not self._cache.last_devens.empty:
            sel_insts = sel_insts.join(
                self._cache.last_devens["LastDEven"]
                .astype("Int64")
                .rename("cached_DEven")
            ).fillna(int(first_possible_deven))
        else:
            sel_insts["cached_DEven"] = first_possible_deven

        sel_insts["outdated"] = sel_insts["cached_DEven"].map(
            lambda deven: data_svs.should_update(
                str(deven),
                self._cache.last_possible_deven,
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
        self._settings = cfg.default_settings
        if kwconf:
            self._settings.update(kwconf)

        # Initialize cache
        cache_cfg = cfg.storage
        cache_cfg.update(self._settings)
        self._cache = TSECache(cache_cfg)

        # Get the latest instuments data
        await data_svs.update_instruments(self._cache)
        if self._cache.instruments is None:
            raise ValueError("No instruments loaded.")

        # TODO: does it return the full list before 8:30 a.m.?
        tse_logger.info("Loaded %s instruments.", len(self._cache.instruments))
        self._codes = self._cache.instruments.loc[
            self._cache.instruments["Symbol"].isin(symbols)
        ].index.to_list()
        tse_logger.info("%s codes loaded.", len(self._codes))
        if not self._codes:
            raise ValueError("No instruments found for any symbols.")
        not_founds = set(symbols) - set(
            self._cache.instruments.loc[self._codes, "Symbol"]
        )
        if not_founds:
            tse_logger.warning("symbols not found: %s", ",".join(not_founds))

        self._cache.read_prices(codes=self._codes)
        to_update = await self._get_expired_prices()
        if to_update.empty:
            tse_logger.info("No download needed. Reading from database.")
        else:
            price_manager = PriceUpdater(cache=self._cache)
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
        res = self._cache.prices_by_symbol(symbols=symbols, cols=cols)

        if self._settings["write_csv"]:
            self._cache.write_prc_csv(res)

        return res
