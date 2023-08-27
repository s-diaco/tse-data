"""
Manage TSE Data
"""

import pandas as pd

from dtse import config as cfg
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse.price_updater import PriceUpdater
from dtse.setup_logger import logger as tse_logger


class TSE:
    """
    Manage TSE Data
    """

    def __init__(self):
        self.settings = cfg.default_settings

    async def _get_expired_prices(self, sel_insts) -> pd.DataFrame:
        """
        check if there is no updated cached data for symbol and return a dataframe
        containing that symbols (InsCode, DEven, YMarNSC)

        :selection: list, instrument codes to check

        :return: pd.DataFrame, symbols to update
        """

        if self.cache.prices is not None:
            self.cache.last_devens = (
                self.cache.prices.reset_index().groupby("InsCode")["DEven"].max()
            )
        first_possible_deven = self.settings["start_date"]
        # TODO: merge selection with last_devens (from cached data) to find out
        # witch syms need an update. dont use ["DEven"].max()
        if self.cache.last_devens is not None and not self.cache.last_devens.empty:
            # TODO: test
            self.cache.instruments = self.cache.instruments.merge(
                self.cache.last_devens.rename("cached_DEven").astype("Int64"),
                how="left",
                on="InsCode",
            ).fillna(int(first_possible_deven))
        else:
            self.cache.instruments["cached_DEven"] = first_possible_deven

        self.cache.instruments["outdated"] = self.cache.instruments["cached_DEven"].map(
            lambda deven: data_svs.should_update(
                str(deven),
                self.cache.last_possible_deven,
            )
        )

        # TODO: price update helper Should update inscode_lastdeven file
        # with new cached instruments in _on_result or do not read it from this file
        self.cache.instruments["NotInNoMarket"] = (
            self.cache.instruments.YMarNSC != "NO"
        ).astype(int)
        outdated_insts = self.cache.instruments.loc[
            sel_insts.index, ["cached_DEven", "NotInNoMarket"]
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
        # TODO: check the dict
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
        tse_logger.info("%s inst. codes loaded.", len(selected_syms))
        if selected_syms.empty:
            raise ValueError("No instruments found for any symbols.")
        not_founds = [
            sym for sym in symbols if sym not in selected_syms["Symbol"].values
        ]
        if not_founds:
            tse_logger.warning("symbols not found: %s", ",".join(not_founds))

        self.cache.read_prices(selected_syms=selected_syms)
        to_update = await self._get_expired_prices(selected_syms)
        price_manager = PriceUpdater(cache_manager=self.cache)
        update_result = await price_manager.start(
            upd_needed=to_update,
            settings=self.settings,
        )
        tse_logger.info(
            "Data download completed for codes: %s",
            ", ".join([str(x) for x in update_result["succs"]]),
        )
        if self.settings["cache_to_db"]:
            self.cache.update_price_db()
        if not update_result["succs"]:
            tse_logger.info("No data downloaded.")
        if update_result["fails"]:
            tse_logger.warning(
                "Failed to get some data. codes: %s", ",".join(update_result["fails"])
            )
        res = {
            sym: self.cache.prices.xs(sym) for sym in self.cache.prices.index.levels[0]
        }

        if self.settings["write_csv"]:
            self.cache.write_prc_csv(codes=self.cache.prices.index.levels[0])

        return res
