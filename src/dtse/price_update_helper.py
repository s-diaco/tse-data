"""
update prices for selected symbols
"""

import asyncio
import math
import re

from . import config as cfg
from .storage import Storage as strg
from .tse_request import TSERequest
from .progress_bar import ProgressBar as prog_bar


class PricesUpdateHelper:
    """
    update prices for selected symbols
    """

    def __init__(self) -> None:
        """
        Initialize the PricesUpdateHelper class.
        """
        self.total: int = 0
        self.succs = []
        self.fails = []
        self.retries: int = 0
        self.retry_chunks = []
        self.timeouts = {}
        self.stored_prices = {}
        self.last_devens = {}
        self.resolve = None
        self.writing = []
        self.should_cache: bool = True

    async def _on_result(self, response, chunk, on_result_id):
        """
        proccess response
        """
        ins_codes = [ins[0] for ins in chunk]
        pattern = re.compile(r"^[\d.,;@-]+$")
        if isinstance(response, str) and (pattern.search(response) or response == ""):
            res = response.split("@")
            if len(ins_codes) != len(res):
                raise ValueError()

            for i, ins_code in enumerate(ins_codes):
                self.succs.append(ins_code)
                if res[i] != "":
                    old_data = self.stored_prices.get(ins_code, None)
                    if old_data is None:
                        data = res[i]
                    else:
                        data = old_data + ";" + res[i]
                    self.stored_prices[ins_code] = data
                    self.last_devens[ins_code] = res[i].split(",")[1]
                    self.writing.append(
                        self.should_cache
                        and await strg().set_item_async("tse.prices." + ins_code, data)
                    )
            self.fails = [x for x in self.fails if x not in ins_codes]
            if self.progressbar.prog_func:
                filled = (
                    self.progressbar.prog_succ_req
                    / (cfg.PRICES_UPDATE_RETRY_COUNT + 2)
                    * (self.retries + 1)
                )
                self.progressbar.prog_func(
                    pn=self.progressbar.prog_n + self.progressbar.prog_succ_req - filled
                )
        else:
            self.fails.append(ins_codes)
            self.retry_chunks.append(chunk)
        self.timeouts.pop(on_result_id)

    async def _request(self, chunk, req_id) -> None:
        """
        request prices
        """

        retries = cfg.PRICES_UPDATE_RETRY_COUNT
        back_off = cfg.PRICES_UPDATE_RETRY_DELAY  # seconds to try again
        ins_codes = ";".join([",".join(map(str, x)) for x in chunk])
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

    async def _batch(self, chunks):
        """
        gather requests
        """

        await asyncio.gather(
            *[self._request(chunk, idx) for idx, chunk in enumerate(chunks)]
        )

    # TODO: fix calculations for progress_dict and return value
    async def start(self, update_needed, should_cache=True, progressbar) -> dict:
        """
        start updating daily prices

        :update_needed: list, instruments, their devens and markets to update
        :should_cache: bool, should cache prices in csv files
        :progress_dict: dict, data needed for progress bar
        """
        self.should_cache = should_cache
        self.progressbar.update(**pb_args)
        self.total = len(update_needed)
        # each successful request
        self.progressbar.prog_succ_req = self.progressbar.prog_tot / math.ceil(
            self.total / cfg.PRICES_UPDATE_CHUNK
        )
        # each request
        self.progressbar.prog_req = self.progressbar.prog_succ_req / (
            cfg.PRICES_UPDATE_RETRY_COUNT + 2
        )
        # Yield successive evenly sized chunks from 'update_needed'.
        chunks = [
            update_needed[i : i + cfg.PRICES_UPDATE_CHUNK]
            for i in range(0, len(update_needed), cfg.PRICES_UPDATE_CHUNK)
        ]
        await self._batch(chunks)
        return {"succs": self.succs, "fails": self.fails, "pn": self.progressbar.prog_n}
