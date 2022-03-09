"""
parse and update prices
"""
import asyncio
from threading import Timer

import numpy as np

import config as cfg


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
        self.qeud_retry = None
        self.resolve = None
        self.writing = []
        self.pf, self.pn, self.ptot, self.pSR, self.pR = None
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

        if len(self.retry_chunks) > 0:
            ins_codes = self.retry_chunks.map(lambda x: x[0])
            self.fails = list(
                filter((lambda x: ins_codes.index(x) == -1), self.fails))
            self.retries += 1
            self.qeud_retry = Timer(
                cfg.PRICES_UPDATE_RETRY_DELAY, self.batch, self.retry_chunks)
            self.retry_chunks = []
            Timer(self.poll, cfg.PRICES_UPDATE_RETRY_DELAY)

    def on_result(self, response, chunk, id):
        ins_codes = chunk.map(lambda ins_code: ins_code)  # todo: complete
        pattern = re.compile("^[\d.,;@-]+$")
        if (type(response) is str and (pattern.match(response) or response == '')):
            res = response.replace(';', '\n').split(
                '@').map(lambda v, i: [chunk[i][0], v])
            for ins_code, new_data in res:
                self.succs.push(ins_code)
                if new_data:
                    old_data = Share.stored_prices[ins_code]
                    data = (new_data, old_data + '\n' + new_data)[old_data]
                    Share.stored_prices[ins_code] = data
                    Share.last_devens[ins_code] = new_data.split(
                        '\n')[-1].slice(-1)[0].split(',', 2)[1]
                    self.writing.push(self.should_cache and Storage.set_item_async(
                        'tse.prices.'+ins_code, data))
            fails = fails.filter(lambda i: ins_codes.index_of(i) == -1)
            if(self.pf):
                filled = self.pSR.div(
                    PRICES_UPDATE_RETRY_COUNT+2).mul(self.retries+1)
                pf(pn=pn+pSR-filled)
        else:
            self.fails.push(np.array(ins_codes))
            self.retry_chunks.push(chunk)
        self.timeouts.pop(id)

    def request(self, chunk=[], id=None) -> None:
        ins_codes = chunk.map(lambda i: i.join(',')).join(';')
        try:
            r = self.rq.closing_prices(ins_codes)
        except Exception as e:
            self.on_result(None, chunk, id)
        self.on_result(r, chunk, id)
        if pf:
            pf(pn=pn+pR)

    # todo: complete
    def batch(self, chunks=[]):
        if self.qeud_retry:
            self.qeud_retry = None
        ids = chunks.map(lambda i, j: 'a'+i)
        for(i=0, delay=0, n=chunks.length; i < n; i++, delay += PRICES_UPDATE_CHUNK_DELAY):
            id = ids[i]
            t = set_timeout(self.request, self.delay, chunks[i], id)
            self.timeouts.set(id, t)
