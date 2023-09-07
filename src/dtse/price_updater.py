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
from dtse.logger import logger as tse_logger
from dtse.tse_request import TSERequest


class PriceUpdater:
    """
    update prices for selected symbols
    """

    def __init__(self, cache: TSECache) -> None:
        """
        Initialize the class.
        """

        self.succs: list = []
        self.fails: list = []
        self._cache: TSECache = cache

    async def _on_result(self, response, chunk):
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
            line_terminator = cfg.RESP_LN_TERMINATOR
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
            self.succs.extend(ins_codes)
            self._cache.update_last_devens(self.succs)
            self._cache.add_to_prices(new_prc_df_list)
        else:
            self.fails.extend(ins_codes)

    async def _request(self, chunk) -> None:
        """
        request prices
        """

        retries = cfg.PRICES_UPDATE_RETRY_COUNT
        back_off = cfg.PRICES_UPDATE_RETRY_DELAY  # seconds to try again
        line_term = cfg.RESP_LN_TERMINATOR
        req_param = chunk.to_csv(header=False, lineterminator=line_term)
        # remove last "lineterminator" from the string
        req_param = req_param[:-1]

        while retries:
            try:
                tse_req = TSERequest()
                res = await tse_req.closing_prices(req_param)
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
        await self._on_result(res, chunk)

    async def _batch(self, chunks: list):
        """
        gather requests
        """

        await asyncio.gather(*[self._request(chunk) for chunk in chunks])

    async def update_prices(self, outdated_insts: pd.DataFrame) -> dict:
        """
        start updating daily prices for outdated codes from selected symbols.

        :outdated_insts: list, instrument codes, their devens and markets to update
        """

        tse_logger.info("Getting ready to download prices.")
        # Yield successive evenly sized chunks from 'outdated_insts'.
        n_rows = cfg.PRICES_UPDATE_CHUNK
        chunks = [
            outdated_insts[i : i + n_rows]
            for i in range(0, len(outdated_insts), n_rows)
        ]
        await self._batch(chunks)
        return {"succs": self.succs, "fails": self.fails}
