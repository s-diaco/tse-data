"""
parse and update prices
"""
import asyncio
import math
import re
from threading import Timer

from . import config as cfg
from .tse_request import TSERequest
from .storage import Storage as strg


class PricesUpdateHelper:
    """
    parse and update prices
    """

    async def __init__(self, **kwargs) -> None:
        """
        Initialize the PricesUpdateHelper class.
        """
        self.total = 0
        self.succs = []
        self.fails = []
        self.retries = 0
        self.retry_chunks = []
        self.timeouts = {}
        self.stored_prices = {}
        self.last_devens = {}
        self.qeud_retry = None
        self.resolve = None
        self.writing = []
        self.should_cache = None

        # progressbar data
        self.progressbar = ProgressBar(
            prog_func=None, prog_n=0, prog_tot=100, prog_succ_req=None, prog_req=None
        )
        await self._start(**kwargs)

    # TODO: complete
    async def _poll(self) -> None:
        """
        polling the server and gathering the results
        """
        while self.timeouts or self.qeud_retry:
            # TODO: get time in ms from cfg file
            await asyncio.sleep(0.5)
        if (
            len(self.succs) == self.total
            or self.retries >= cfg.PRICES_UPDATE_RETRY_COUNT
        ):
            _succs = self.succs
            _fails = self.fails
            self.succs = []
            self.fails = []
            # TODO: delete:
            # for idx, url in enumerate(urls):
            #     self.writing.append(asyncio.Task(fetch_page(url, idx)))

            # Write price data to files
            # TODO: does writing write the files?
            resps = await asyncio.gather(*self.writing, return_exceptions=True)
            for response in resps:
                if isinstance(response, Exception):
                    self.fails.append(response)
                else:
                    self.succs.append(response)
            self.writing = []
            return
            # TODO: what is pn?
            # resolve({succs: _succs, fails: _fails, pn});

        if len(self.retry_chunks) > 0:
            ins_codes = self.retry_chunks[0]
            self.fails = list(filter((lambda x: ins_codes.index(x) == -1), self.fails))
            self.retries += 1
            # TODO: is it working?
            # Timer(self.poll, cfg.PRICES_UPDATE_RETRY_DELAY)
            loop = asyncio.get_event_loop()
            loop.call_later(
                cfg.PRICES_UPDATE_RETRY_DELAY, self._batch, self.retry_chunks
            )
            self.retry_chunks = []
            loop.call_later(cfg.PRICES_UPDATE_RETRY_DELAY, self._poll)

    def _on_result(self, response, chunk, on_result_id):
        """
        proccess response
        """
        ins_codes = list(map(lambda x: x[0], chunk))
        pattern = re.compile(r"^[\d.,;@-]+$")
        if isinstance(response, str) and (pattern.search(response) or response == ""):
            res = dict(zip(ins_codes, response.split("@")))
            for ins_code, new_data in res.items():
                self.succs.append(ins_code)
                if new_data != "":
                    old_data = self.stored_prices.get(ins_code, None)
                    if old_data is None:
                        data = new_data
                    else:
                        data = old_data + ";" + new_data
                    self.stored_prices[ins_code] = data
                    self.last_devens[ins_code] = new_data.split(";")[-1].split(",", 2)[
                        1
                    ]
                    self.writing.append(
                        self.should_cache
                        and strg().set_item_async("tse.prices." + ins_code, data)
                    )
            self.fails = list(set(self.fails).intersection(ins_codes))
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

    def _request(self, chunk=None, req_id=None) -> None:
        """
        request prices
        """
        if chunk is None:
            chunk = []
        ins_codes = ";".join(list(map(",".join, chunk)))
        try:
            tse_req = TSERequest()
            res = tse_req.closing_prices(ins_codes)
            self._on_result(res, chunk, req_id)
        except:  # pylint: disable=bare-except
            self._on_result(None, chunk, req_id)
        if self.progressbar.prog_func:
            self.progressbar.prog_func(
                pn=self.progressbar.prog_n + self.progressbar.prog_req
            )

    def _batch(self, chunks=None):
        """
        batch request
        """
        if not chunks:
            chunks = []
        if self.qeud_retry:
            self.qeud_retry = None
        delay = 0
        for idx, chunk in enumerate(chunks):
            timeout = Timer(delay, self._request, args=(chunk, idx))
            self.timeouts[idx] = timeout
            delay += cfg.PRICES_UPDATE_RETRY_DELAY

    # TODO: fix calculations for progress_dict and return value
    async def _start(self, update_needed, should_cache, **kwargs):
        """
        start updating daily prices

        :update_needed: list, instruments to update
        :should_cache: bool, should cache prices in csv files
        :progress_dict: dict, data needed for progress bar
        """
        update_needed = kwargs.get("update_needed", [])
        self.should_cache = kwargs.get("should_cache", True)
        self.progressbar.update(**kwargs)
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
        self._batch(chunks)
        await self._poll()
        # new Promise(r => resolve = r);

    async def get_data(session, url):
        retries = cfg.PRICES_UPDATE_RETRY_COUNT
        back_off = cfg.PRICES_UPDATE_RETRY_COUNT  # seconds to try again
        for _ in range(retries):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        response.raise_for_status()
                    print(retries, response.status, url)
                    return await response.json()
            except aiohttp.client_exceptions.ClientResponseError as e:
                retries -= 1
                await asyncio.sleep(back_off)
                continue

    async def main():
        async with aiohttp.ClientSession() as session:
            attendee_urls = get_urls(
                "attendee"
            )  # returns list of URLs to call asynchronously in get_data()
            attendee_data = await asyncio.gather(
                *[get_data(session, attendee_url) for attendee_url in attendee_urls]
            )
            return attendee_data


class ProgressBar:
    """
    Manage progressbar data
    """

    prog_n: int
    prog_tot: int
    prog_succ_req: int
    prog_req: int

    def prog_func(self):
        """
        # TODO: delete
        """

    def __init__(self, **kwargs) -> None:
        """
        initialize progressbar data
        """
        default_settings = {
            "prog_func": None,
            "prog_n": 0,
            "prog_tot": 100,
            "prog_succ_req": None,
            "prog_req": None,
        }

        bad_keys = [k for k, _ in kwargs if k not in default_settings]
        if bad_keys:
            raise TypeError(f"Invalid arguments for ProgressBar.__init__: {bad_keys}")
        self.update(**kwargs)

    def update(self, **kwargs) -> None:
        """
        update progressbar data
        """
        self.__dict__.update(kwargs)
