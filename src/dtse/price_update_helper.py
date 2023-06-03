"""
update prices for selected symbols
"""

import asyncio
import re

import pandas as pd
from dtse.cache_manager import TSECache

from dtse.data_services import get_symbol_names, resp_to_csv

from . import config as cfg
from .storage import Storage
from .tse_request import TSERequest
from .progress_bar import ProgressBar


class PricesUpdateHelper:
    """
    update prices for selected symbols
    """

    def __init__(self, cache_manager: TSECache) -> None:
        """
        Initialize the PricesUpdateHelper class.
        """
        self.total: int = 0
        self.succs = []
        self.fails = []
        self.retries: int = 0
        self.retry_chunks = []
        self.timeouts = {}
        # TODO: is it needed?
        self.last_devens = {}
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
        ins_codes = chunk.InsCode.values
        pattern = re.compile(r"^[\d.,;@-]+$")
        if isinstance(response, str) and (pattern.search(response) or response == ""):
            res = response.split("@")
            if len(ins_codes) != len(res):
                raise ValueError()

            if not self.sym_names:
                self.sym_names = get_symbol_names(ins_codes)

            for i, ins_code in enumerate(ins_codes):
                self.succs.append(ins_code)
                if res[i] != "":
                    old_data = self.cache_manager.stored_prices[ins_code]
                    add_to_existing_data = False
                    if not old_data.empty:
                        add_to_existing_data = True
                    data = res[i]
                    # TODO: delete if inscode_lastdeven file is not used
                    self.last_devens[ins_code] = res[i].split(",")[1]
                    col_names = cfg.tse_closing_prices_info
                    line_terminator = ";"
                    file_name = self.sym_names[ins_code]
                    self.writing.append(
                        self.should_cache
                        and resp_to_csv(
                            resp=data,
                            col_names=col_names,
                            line_terminator=line_terminator,
                            converters=None,
                            f_name="tse.prices." + file_name,
                            storage=self.strg,
                            append=add_to_existing_data,
                        )
                    )
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
        self, update_needed: pd.DataFrame, settings: dict, progressbar: ProgressBar
    ) -> dict:
        """
        start updating daily prices

        :update_needed: list, instrument codes, their devens and markets to update
        :settings: dict, should_cache & merge_similar_symbols & ...
        :progress_dict: dict, data needed for progress bar
        """
        self.should_cache = settings["cache"]
        self.merge_similar_syms = settings["merge_similar_symbols"]
        self.progressbar = progressbar
        self.total = len(update_needed)
        # each successful request
        """
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
