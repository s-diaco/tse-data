"""
parse and update prices
"""
import asyncio
from io import StringIO
import re
import numpy as np
import pandas as pd

import config as cfg
from storage import Storage


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
        self.pf = self.pn = self.ptot = self.pSR = self.pR = None
        self.should_cache = None

    # TODO: complete
    async def poll(self) -> None:
        if (len(self.timeouts) > 0 or self.qeud_retry):
            # TODO: get time in ms from cfg file
            await asyncio.sleep(0.5)
            return self.poll()
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
                            self.batch, self.retry_chunks)
            self.retry_chunks = []
            loop.call_later(cfg.PRICES_UPDATE_RETRY_DELAY, self.poll)

    def on_result(self, response, chunk, on_result_id):
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
                    strg = Storage()
                    self.writing.append(self.should_cache and strg.set_item_async(
                        'tse.prices.'+ins_code, data))
            self.fails = list(set(self.fails).intersection(ins_codes))
            if(self.pf):
                filled = self.pSR / \
                    (cfg.PRICES_UPDATE_RETRY_COUNT+2)*(self.retries + 1)
                self.pf(pn=self.pn+self.pSR-filled)
        else:
            self.fails.append(ins_codes)
            self.retry_chunks.append(chunk)
        self.timeouts.pop(id)

        """
            def request(self, chunk=[], id=None) -> None:
                ins_codes = chunk.map(lambda i: i.join(',')).join(';')
                try:
                    r = self.rq.closing_prices(ins_codes)
                except Exception as e:
                    self.on_result(None, chunk, id)
                self.on_result(r, chunk, id)
                if pf:
                    pf(pn=pn+pR)
        """

    # todo: complete
    def batch(self, chunks=[]):
        if self.qeud_retry:
            self.qeud_retry = None
        ids = chunks.map(lambda i, j: 'a'+i)
