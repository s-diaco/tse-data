"""
parse and update prices
"""
import asyncio

import numpy as np

from config import PRICES_UPDATE_RETRY_COUNT


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
        self.timeouts = map()
        self.qeud_retry = None
        self.resolve = None
        self.writing = []
        self.pf, self.pn, self.ptot, self.pSR, self.pR = None
        self.should_cache = None

    # todo: complete
    async def poll(self) -> None:
        if self.timeouts.size > 0 or self.qeud_retry:
            await asyncio.sleep(0.5)
            return self.poll()
        if(self.succs.length == self.total or
           self.retries >= PRICES_UPDATE_RETRY_COUNT):
            _succs = np.array(self.succs)
            _fails = np.array(self.fails)
            self.succs = []
            self.fails = []
            await asyncio.gather(*self.writing)
            self.writing = []

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
