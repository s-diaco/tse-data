"""
parse and update prices
"""
import asyncio
import math
import re

import config as cfg
import data_services as data_svs
from storage import Storage as strg
from tse_request import TSERequest


class PricesUpdateHelper:
    """
    parse and update prices
    """

    def __init__(self) -> None:
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

        # progressbar data
        self.prog_func = None
        self.prog_n = None
        self.prog_tot = None
        self.prog_succ_req = None
        self.prog_req = None

        self.should_cache = None

    # TODO: complete
    async def _poll(self) -> None:
        if (len(self.timeouts) > 0 or self.qeud_retry):
            # TODO: get time in ms from cfg file
            await asyncio.sleep(0.5)
            return self._poll()
        if(len(self.succs) == self.total or
           self.retries >= cfg.PRICES_UPDATE_RETRY_COUNT):
            _succs = self.succs
            _fails = self.fails
            self.succs = []
            self.fails = []
            # for idx, url in enumerate(urls):
            #     self.writing.append(asyncio.Task(fetch_page(url, idx)))
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
            self.fails = list(
                filter((lambda x: ins_codes.index(x) == -1), self.fails))
            self.retries += 1
            # TODO: is it working?
            # Timer(self.poll, cfg.PRICES_UPDATE_RETRY_DELAY)
            loop = asyncio.get_event_loop()
            loop.call_later(cfg.PRICES_UPDATE_RETRY_DELAY,
                            self._batch, self.retry_chunks)
            self.retry_chunks = []
            loop.call_later(cfg.PRICES_UPDATE_RETRY_DELAY, self._poll)

    def _on_result(self, response, chunk, on_result_id):
        """
        proccess response
        """
        ins_codes = list(map(lambda x: x[0], chunk))
        pattern = re.compile(r'^[\d.,;@-]+$')
        if (isinstance(response, str) and
                (pattern.search(response) or response == '')):
            res = dict(zip(ins_codes, response.split('@')))
            for ins_code, new_data in res.items():
                self.succs.append(ins_code)
                if new_data != '':
                    old_data = self.stored_prices.get(ins_code, None)
                    if old_data is None:
                        data = new_data
                    else:
                        data = old_data + ';' + new_data
                    self.stored_prices[ins_code] = data
                    self.last_devens[ins_code] = new_data.split(
                        ';')[-1].split(',', 2)[1]
                    self.writing.append(self.should_cache and strg.set_item_async(
                        'tse.prices.'+ins_code, data))
            self.fails = list(set(self.fails).intersection(ins_codes))
            if(self.progress_func):
                filled = self.prog_succ_req / \
                    (cfg.PRICES_UPDATE_RETRY_COUNT+2)*(self.retries + 1)
                self.progress_func(pn=self.progress_n +
                                   self.prog_succ_req-filled)
        else:
            self.fails.append(ins_codes)
            self.retry_chunks.append(chunk)
        self.timeouts.pop(id)

    def _request(self, chunk=None, req_id=None) -> None:
        """
        request prices
        """
        if chunk is None:
            chunk = []
        ins_codes = ';'.join(list(map(','.join, chunk)))
        try:
            tse_req = TSERequest()
            res = tse_req.closing_prices(ins_codes)
            self._on_result(res, chunk, req_id)
        except:  # pylint: disable=bare-except
            self._on_result(None, chunk, req_id)
        if(self.progress_func):
            self.progress_func(pn=self.progress_n+self.prog_req)

    # todo: complete
    def _batch(self, chunks=None):
        """
        batch request
        """
        if chunks is None:
            chunks = []
        if self.qeud_retry:
            self.qeud_retry = None
        dicts={'a'+str(indx):chunk for indx, chunk in enumerate(chunks)}
        # ids = list(map(lambda i, j: 'a'+i, chunks))
        # self.timeouts = dict(zip(ids, chunks))

    # TODO: fix calculations for progress_dict and return value
    def start(self, update_needed=None, should_cache=None, progress_dict=None):
        """
        start updating daily prices

        :update_needed: list, instruments to update
        :should_cache: bool, should cache prices in csv files
        :progress_dict: dict, data needed for progress bar
        """
        if update_needed is None:
            update_needed = []
        if progress_dict is None:
            progress_dict = {}
        self.should_cache = should_cache
        self.progress_func, self.progress_n, self.prog_tot = progress_dict
        self.total = len(update_needed)
        # each successful request
        self.prog_succ_req = self.prog_tot / \
            math.ceil(
                self.total / cfg.PRICES_UPDATE_CHUNK)
        # each request
        self.prog_req = self.prog_succ_req / \
            (cfg.PRICES_UPDATE_RETRY_COUNT + 2)
        self.succs = []
        self.fails = []
        self.retries = 0
        self.retry_chunks = []
        self.timeouts = {}
        self.qeud_retry = None
        # Yield successive evenly sized chunks from 'update_needed'.
        chunks = [update_needed[i:i + cfg.PRICES_UPDATE_CHUNK]
                  for i in range(0, len(update_needed),
                  cfg.PRICES_UPDATE_CHUNK)]
        self._batch(chunks)
        self._poll()
        return  # new Promise(r => resolve = r);

    # todo: complete
    async def update_prices(self, selection=None, should_cache=None, percents=None):
        """
        update prices

        :selection: list, instruments to update
        :should_cache: bool, should cache prices in csv files
        :percents: dict, data needed for progress bar
        """
        last_devens = strg.get_item('tse.inscode_lastdeven')
        ins_codes = None
        if last_devens:
            ins_codes = last_devens[1:]
        else:
            last_devens = {}
        result = {"succs": [], "fails": [], "error": None, "pn": percents.pn}
        prog_fin = percents.pn+percents.ptot
        last_possible_deven = await data_svs.get_last_possible_deven()
        if last_possible_deven:
            result.error = last_possible_deven
            if callable(percents.progress_func):
                percents.prog_func(prog_fin)
            return result
