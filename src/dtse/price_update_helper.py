"""
update prices for selected symbols
"""

import asyncio
import math
import re

from dtse.data_services import get_symbol_names, resp_to_csv

from . import config as cfg
from .storage import Storage
from .tse_request import TSERequest
from .progress_bar import ProgressBar


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

            if not self.sym_names:
                self.sym_names = get_symbol_names(ins_codes)

            for i, ins_code in enumerate(ins_codes):
                self.succs.append(ins_code)
                if res[i] != "":
                    # TODO: review to add to existing data if needed
                    old_data = self.stored_prices[ins_code]
                    if old_data.empty:
                        data = res[i]
                    else:
                        data = old_data + ";" + res[i]
                    self.stored_prices[ins_code] = data
                    self.last_devens[ins_code] = res[i].split(",")[1]
                    col_names = cfg.tse_closing_prices_info
                    line_terminator = ";"
                    file_name = self.sym_names[str(ins_code)]
                    self.writing.append(
                        self.should_cache
                        and await resp_to_csv(
                            resp=data,
                            col_names=col_names,
                            line_terminator=line_terminator,
                            converters=None,
                            f_name="tse.prices." + file_name,
                            storage=strg,
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

    def update_stored_prices(self, sel_ins: list):
        """
        updatex a dict of last devens for ins_codes in self.stored_prices

        :sel_ins: list, instrument codes to look for last devens
        """

        self.sym_names = get_symbol_names(sel_ins)
        prc_dict = self.strg.get_items(f_names=list(self.sym_names.values()))
        self.stored_prices = {k: v for k, v in prc_dict.items() if not v.empty}

    # TODO: fix calculations for progress_dict and return value
    async def start(self, update_needed, progressbar: ProgressBar, settings) -> dict:
        """
        start updating daily prices

        :update_needed: list, instrument codes, their devens and markets to update
        :settings: dict, should_cache & merge_similar_symbols & ...
        :progress_dict: dict, data needed for progress bar
        """
        self.should_cache = settings["should_cache"]
        self.merge_similar_syms = settings["merge_similar_syms"]
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
        ins_codes = [str(sym[0]) for sym in update_needed]
        # Yield successive evenly sized chunks from 'update_needed'.
        chunks = [
            update_needed[i : i + cfg.PRICES_UPDATE_CHUNK]
            for i in range(0, len(update_needed), cfg.PRICES_UPDATE_CHUNK)
        ]
        await self._batch(chunks)
        return {"succs": self.succs, "fails": self.fails}
