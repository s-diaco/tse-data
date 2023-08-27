"""
update prices for selected symbols
"""

import asyncio
import re
from io import StringIO

import pandas as pd
from aiohttp import ClientResponseError

from dtse import config as cfg
from dtse.cache_manager import TSECache
from dtse.setup_logger import logger as tse_logger
from dtse.tse_request import TSERequest


class PriceUpdater:
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
        self.timeouts = {}
        self.sym_names: dict = {}
        self.resolve = None
        # TODO: remove
        self.writing = []
        self.cache_to_csv: bool = False
        # TODO: implement
        self.merge_similar_syms: bool = True
        self.cache: TSECache = cache_manager

    async def _on_result(self, response, chunk, on_result_id):
        """
        Proccess response
        """

        ins_codes = chunk.index
        pattern = re.compile(r"^[\d.,;@-]+$")
        if isinstance(response, str) and (pattern.search(response) or response == ""):
            resp_list = response.split("@")
            if len(ins_codes) != len(resp_list):
                raise ValueError(
                    f"requested {len(ins_codes)} codes, got {len(resp_list)}"
                )
            col_names = cfg.tse_closing_prices_info
            line_terminator = cfg.SERVER_LN_TERMINATOR
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
            self.cache.add_to_prices(new_prc_df_list)
            self.succs.extend(ins_codes)
            self.timeouts.pop(on_result_id)
        else:
            self.fails.extend(ins_codes)

    async def _request(self, chunk, req_id) -> None:
        """
        request prices
        """

        retries = cfg.PRICES_UPDATE_RETRY_COUNT
        back_off = cfg.PRICES_UPDATE_RETRY_DELAY  # seconds to try again
        request_param_str = ",".join(
            chunk.to_string(header=False, index_names=False).replace("\n", ";").split()
        )
        while retries:
            # TODO: is this line needed?
            self.timeouts[req_id] = chunk

            try:
                tse_req = TSERequest()
                res = await tse_req.closing_prices(request_param_str)
                retries = 0
            except ClientResponseError as ex:
                retries -= 1
                if retries:
                    await asyncio.sleep(back_off)

                    # TODO: write a better error msg
                    tse_logger.warning(ex)

                    # Double the waiting time after each retry
                    back_off = back_off * 2
                else:
                    res = "error"
        await self._on_result(res, chunk, req_id)

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
        upd_needed: pd.DataFrame,
        settings: dict,
    ) -> dict:
        """
        start updating daily prices

        :update_needed: list, instrument codes, their devens and markets to update
        :settings: dict, should_cache & merge_similar_symbols & ...
        :progress_dict: dict, data needed for progress bar
        """

        tse_logger.info("Getting ready to download prices.")
        if "cache_to_db" in settings:
            self.cache_to_csv = settings["cache_to_db"]
        # Yield successive evenly sized chunks from 'upd_needed'.
        n_rows = cfg.PRICES_UPDATE_CHUNK
        chunks = [upd_needed[i : i + n_rows] for i in range(0, len(upd_needed), n_rows)]
        await self._batch(chunks)
        return {"succs": self.succs, "fails": self.fails}
