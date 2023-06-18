"""
update prices for selected symbols
"""

import asyncio
import re
from io import StringIO

import pandas as pd

from dtse import config as cfg
from dtse.cache_manager import TSECache
from dtse.progress_bar import ProgressBar
from dtse.setup_logger import logger as tse_logger
from dtse.storage import Storage
from dtse.tse_request import TSERequest


class PricesUpdateManager:
    """
    update prices for selected symbols
    """

    def __init__(self, cache_manager: TSECache) -> None:
        """
        Initialize the PricesUpdateManager class.
        """

        self.succs = []
        self.fails = []
        self.retries: int = 0
        self.retry_chunks = []
        self.timeouts = {}
        self.sym_names: dict = {}
        self.resolve = None
        # TODO: remove
        self.writing = []
        self.should_cache: bool = True
        # TODO: implement
        self.merge_similar_syms: bool = True
        self.strg = Storage()
        self.cache_manager = cache_manager

    async def _on_result(self, response, chunk, on_result_id):
        """
        proccess response
        """
        ins_codes = chunk.InsCode.values.astype(int)
        pattern = re.compile(r"^[\d.,;@-]+$")
        if isinstance(response, str) and (pattern.search(response) or response == ""):
            resp_list = response.split("@")
            if len(ins_codes) != len(resp_list):
                raise ValueError(
                    f"requested {len(ins_codes)} codes, got {len(resp_list)}"
                )
            df_list = [self.cache_manager.stored_prices]
            col_names = cfg.tse_closing_prices_info
            line_terminator = ";"
            new_prc_df_list = [
                pd.read_csv(
                    StringIO(resp),
                    names=col_names,
                    lineterminator=line_terminator,
                    index_col=["InsCode", "DEven"],
                )
                for resp in resp_list
                if resp
            ]
            df_list.extend(new_prc_df_list)
            self.cache_manager.stored_prices = pd.concat(df_list).sort_index()
            self.succs.extend(ins_codes)

            # TODO: delete if inscode_lastdeven file is not used
            self.cache_manager.last_devens = (
                self.cache_manager.stored_prices.reset_index()
                .groupby("InsCode")["DEven"]
                .max()
            )

            # TODO: Delete?
            """
            self.writing.append(
                self.should_cache
                and self.strg.write_tse_csv_blc(f_name=filename, data=data)
            )"""

            if self.should_cache:
                for ins_code in self.cache_manager.stored_prices.index.levels[0]:
                    filename = f"tse.prices.{ins_code}"
                    data = self.cache_manager.stored_prices.xs(
                        ins_code, drop_level=False
                    )
                    self.strg.write_tse_csv_blc(f_name=filename, data=data)
            self.fails = [x for x in self.fails if x not in ins_codes]

            """
            if self.progressbar.prog_func:
                filled = (
                    self.progressbar.prog_succ_req
                    / (cfg.PRICES_UPDATE_RETRY_COUNT + 2)
                    * (self.retries + 1)
                )
                self.progressbar.prog_n = int(
                    self.progressbar.prog_n + self.progressbar.prog_succ_req - filled
                )
                self.progressbar.prog_func(self.progressbar.prog_n = (
                    self.progressbar.prog_n + self.progressbar.prog_succ_req - filled)
            """
        else:
            self.fails.append(ins_codes)
            # TODO: wo req_id?
            self.retry_chunks.append(chunk)
        self.timeouts.pop(on_result_id)

    async def _request(self, chunk, req_id) -> None:
        """
        request prices
        """

        retries = cfg.PRICES_UPDATE_RETRY_COUNT
        back_off = cfg.PRICES_UPDATE_RETRY_DELAY  # seconds to try again
        ins_codes = ";".join([",".join(map(str, x)) for x in chunk.values])
        for _ in range(retries):
            # TODO: is this line needed?
            self.timeouts[req_id] = chunk

            try:
                tse_req = TSERequest()
                res = await tse_req.closing_prices(ins_codes)
                await self._on_result(res, chunk, req_id)
                break
            except:
                retries -= 1
                await asyncio.sleep(back_off)
                # Double the waiting time after each retry
                back_off = back_off * 2
                continue

    async def _batch(self, chunks: list):
        """
        gather requests
        """

        await asyncio.gather(
            *[self._request(chunk, idx) for idx, chunk in enumerate(chunks)]
        )

    # TODO: fix calculations for progress_dict and return value
    async def start(
        self,
        update_needed: pd.DataFrame,
        settings: dict,
        progressbar: ProgressBar,
    ) -> dict:
        """
        start updating daily prices

        :update_needed: list, instrument codes, their devens and markets to update
        :settings: dict, should_cache & merge_similar_symbols & ...
        :progress_dict: dict, data needed for progress bar
        """

        tse_logger.info("Getting ready to download prices.")
        self.should_cache = settings["cache"]
        self.merge_similar_syms = settings["merge_similar_symbols"]
        self.progressbar = progressbar
        # each successful request
        """
        self.total = len(update_needed)
        self.progressbar.prog_succ_req = self.progressbar.prog_tot / math.ceil(
            self.total / cfg.PRICES_UPDATE_CHUNK
        )
        # each request
        self.progressbar.prog_req = self.progressbar.prog_succ_req / (
            cfg.PRICES_UPDATE_RETRY_COUNT + 2
        )
        """
        # Yield successive evenly sized chunks from 'update_needed'.
        chunks = [
            update_needed[i : i + cfg.PRICES_UPDATE_CHUNK]
            for i in range(0, len(update_needed), cfg.PRICES_UPDATE_CHUNK)
        ]
        await self._batch(chunks)
        return {"succs": self.succs, "fails": self.fails}
