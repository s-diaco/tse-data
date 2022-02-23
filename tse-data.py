# %% Configuration
API_URL = 'http://service.tsetmc.com/tsev2/data/TseClient2.aspx'

COLS  = ['date','dateshamsi','open','high','low','last','close','vol','count','value','yesterday','symbol','name','namelatin','companycode'];
COLS_FA = ['تاریخ میلادی','تاریخ شمسی','اولین قیمت','بیشترین قیمت','کمترین قیمت','آخرین قیمت','قیمت پایانی','حجم معاملات','تعداد معاملات','ارزش معاملات','قیمت پایانی دیروز','نماد','نام','نام لاتین','کد شرکت',];

UPDATE_INTERVAL           = 1;
PRICES_UPDATE_CHUNK       = 50;
PRICES_UPDATE_CHUNK_DELAY = 300;
PRICES_UPDATE_RETRY_COUNT = 3;
PRICES_UPDATE_RETRY_DELAY = 1000;
SYMBOL_RENAME_STRING    = '-ق';
MERGED_SYMBOL_CONTENT   = 'merged';
DEFAULT_SETTINGS = {
	'columns': [0,2,3,4,5,6,7,8,9],
	'adjustPrices': 0,
	'daysWithoutTrade': False,
	'startDate': '20010321',
	'mergeSimilarSymbols': True,
	'cache': True,
	'csv': False,
	'csvHeaders': True,
	'csvDelimiter': ',',
	'onprogress': None,
	'progressTotal': 100
};



# %%
import logging
import math
from pathlib import Path
import sys
from zipfile import ZipFile

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

logger.info("Testing logger for num %d and string %s", 4, "HelloWorld")


# %% Class storage

# %% Class rq
from urllib.parse import urlparse, urlencode
import aiohttp
import asyncio


class RQ:
    def instrument(self, DEven: str):
        params = {
			't': 'Instrument',
			'a': DEven
		}
        return self.make_request(params)
    
    def instrument_and_share(self, DEven: str, LastID: str='0'):
        params = {
            't': 'InstrumentAndShare',
            'a': DEven,
            'a2': LastID
        }
        return self.make_request(params)

    def last_possible_deven(self):
        params = {
            't': 'LastPossibleDeven'
        }
        return self.make_request(params)

    def closing_prices(self, ins_codes: str):
        params = {
            't': 'ClosingPrices',
            'a': ins_codes
        }
        return self.make_request(params)

    async def make_request(self, params:dict):
        # todo: remove line below
        # url += ('&', '?')[urlparse(url).query == ''] + urlencode(params)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL, params=params) as response:
                    if response.status == 200:
                        ret_val = await response.text()
                    else:
                        ret_val = response.status + ' ' + response.reason
        except Exception as e:
            ret_val = str(e)
        return ret_val


class ClosingPrice:
    def __init__(self, _row=''):
        row = row.split(',')
        if row.length != 11:
            raise Exception('Invalid closing price row')
        self.InsCode        = row[0];  # int64
        self.DEven          = row[1];  # int32 (the rest are all decimal)
        self.PClosing       = row[2];  # close
        self.PDrCotVal      = row[3];  # last
        self.ZTotTran       = row[4];  # count
        self.QTotTran5J     = row[5];  # volume
        self.QTotCap        = row[6];  # price
        self.PriceMin       = row[7];  # low
        self.PriceMax       = row[8];  # high
        self.PriceYesterday = row[9];  # yesterday
        self.PriceFirst     = row[10]; # open


class Column:
    def __init__(self, row=[]):
        len = row.length
        if(len > 2 or len < 1):
            raise Exception('Invalid column data')
            self.name = COLS[row[0]]
            self.fname = COLS_FA[row[0]]
            self.header = row[1]


class Instrument:
    def __init__(self, _row=''):
        row = row.split(',')
        if not (row.lenght in [18,19]):
            raise Exception('Invalid instrument data')
        self.InsCode = row[0];  # int64
        self.InstrumentID = row[1];  # string
        self.LatinSymbol = row[2]; # string
        self.LatinName = row[3]; # string
        self.CompanyCode = row[4]; # string
        self.Symbol = clean_fa(row[5]).trim(); # string
        self.Name = row[6]; # string
        self.Name = row[7]; # string
        self.DEven = row[8]; # int32
        self.Flow = row[9]; # 0,1,2,3,4,5,6,7 بازار
        self.LSoc30 = row[10]; # نام 30 رقمي فارسي شرکت
        self.CGdSVal = row[11]; # A,I,O نوع نماد
        self.CGrValCot = row[12]; # 00,11,1A,...25 کد گروه نماد
        self.YMarNSC = row[13]; # NO,OL,BK,BY,ID,UI کد بازار
        self.CComVal = row[14]; # 1,3,4,5,6,7,8,9 کد تابلو
        self.CSecVal = row[15]; # []62 کد گروه صنعت
        self.CSoSecVal = row[16]; # []177 کد زیر گروه صنعت
        self.YVal = row[17]; # string نوع نماد
        if row[18]:
            self.SymbolOriginal = clean_fa(row[18]).trim(); # string


class InstrumentItd:
    def __init__(self, _row='') -> None:
        row = row.split(',')
        if row.lenght != 11:
            raise Exception('Invalid instrument_itd data')
        self.InsCode = row[0];
        self.LVal30 = clean_fa(row[1]).trim(); # نام 30 رقمي فارسي نماد
        self.LVal18AFC = clean_fa(row[2]).trim(); # کد 18 رقمي فارسي نماد
        self.FlowTitle = clean_fa(row[3]).trim();
        self.CGrValCotTitle = clean_fa(row[4]).trim();
        self.Flow = row[5]
        self.CGrValCot = row[6]
        self.CIsin = row[7]
        self.InstrumentID = row[8]
        self.ZTitad = row[9]; # تعداد سھام
        self.BaseVol = row[10]; # حجم مبنا


import jdatetime
import datetime
import re


class Share:
    def __init__(self, _row=''):
        row = row.split(',')
        if row.length != 5:
            raise Exception('Invalid share data')
        self.Idn = row[0];  # long
        self.InsCode = row[1];  # long
        self.DEven = row[2];  # int32
        self.NumberOfShareNew = int(row[3]);  # decimal
        self.NumberOfShareOld = int(row[4]);  # decimal
        self.last_devens = {}
        self.stored_prices = {}


# %% utils
    def parse_instruments(struct=False, arr=False, struct_key='InsCode', itd=False):
        instruments = None
        rows = Storage.get_item('tse.instruments'+('', '.intraday')[itd])
        if rows:
            rows.split('\n')
            for row in rows:
                if arr:
                    instruments = []
                    if itd:
                        if struct:
                            instruments.push(instrument_itd(row))
                        else:
                            instruments.push(row)
                    else:
                        if struct:
                            instruments.push(instrument(row))
                        else:
                            instruments.push(row)
                else:
                    instruments = {}
                    if itd:
                        if struct:
                            key = instrument_itd(row)[struct_key]
                            instruments[key] = instrument_itd(row)
                        else:
                            key = row.split(',',1)[0]
                            instruments[key] = row
                    else:
                        if struct:
                            key = instrument(row)[struct_key]
                            instruments[key] = instrument(row)
                        else:
                            key = row.split(',',1)[0]
                            instruments[key] = row
        return instruments

    def parse_shares(struct=False, arr=True):
        shares = None
        rows = storage.get_item('tse.shares')
        if rows:
            rows.split('\n')
            for row in rows:
                if arr:
                    shares = []
                    if struct:
                        shares.push(share(row))
                    else:
                        shares.push(row)
                else:
                    shares = {}
                    if struct:
                        key = share(row).InsCode
                        if not shares.has_key(key):
                            shares[key] = []
                        shares[key].push(share(row))
                    else:
                        key = row.split(',',2)[1]
                        shares[key].push(row)
        return shares

    def cealn_fa(str) -> str:
        return str.replace('\u200B','').replace('\s?\u200C\s?',' ').replace('\u200D','').replace('\uFEFF','').replace('ك','ک').replace('ي','ی')

    def adjust(cond, closing_prices, all_shares, ins_codes):
        shares = {i.DEven: i for i in list(set(all_shares).intersection(ins_codes))} 
        cp = closing_prices
        len = closing_prices.length
        adjusted_cl_prices = []
        res = cp
        if(cond in [1,2] and len > 1):
            gaps = 0
            num = 1
            adjusted_cl_prices.push(cp[len-1])
            if cond == 1:
                for i in range(len-2, -1, -1):
                    [curr, next] = [cp[i], cp[i+1]]
                    if curr.PClosing != next.PriceYesterday and curr.InsCode == next.InsCode:
                        gaps += 1
            if((cond == 1 and gaps/len<0.08) or cond == 2):
                for i in range(len-2, -1, -1):
                    [curr, next] = [cp[i], cp[i+1]]
                    prices_dont_match = curr.PClosing != next.PriceYesterday and curr.InsCode == next.InsCode
                    target_share = shares.get(next.DEven)
                    if (cond == 1 and prices_dont_match):
                        num = num*next.PriceYesterday/curr.PClosing
                    elif (cond == 2 and prices_dont_match and target_share):
                        old_shares = target_share.NumberOfShareOld
                        new_shares = target_share.NumberOfShareNew
                        num = num * old_shares/new_shares
                    close = round(num * curr.PClosing, 2)
                    last = round(num * curr.PDrCotVal, 2)
                    low = round(num * curr.PriceMin, 2)
                    high = round(num * curr.PriceMax, 2)
                    yday = round(num * curr.PriceYesterday, 2)
                    first = round(num * curr.PriceFirst, 2)

                    adjusted_closing_price = {
                        'InsCode': curr.InsCode,
                        'DEven': curr.DEven,
                        'PClosing': close,
                        'PDrCotVal': last,
                        'PriceMin': low,
                        'PriceMax': high,
                        'PriceYesterday': yday,
                        'PriceFirst': first,
                        'ZTotTran': curr.ZTotTran,
					    'QTotTran5J': curr.QTotTran5J,
					    'QTotCap': curr.QTotCap
                    }
                    adjusted_cl_prices.push(adjusted_closing_price)
                res = adjusted_cl_prices.reverse()
        return res

    def get_cell(self, column_name, instrument, closing_price):
        if column_name == 'dateshamsi':
            return jdatetime.date.fromgregorian(date=datetime.datetime.strptime(instrument.DEven, '%Y%m%d'))
        elif column_name == 'date':
            return instrument.DEven
        elif column_name == 'close':
            return closing_price.PClosing
        elif column_name == 'last':
            return closing_price.PDrCotVal
        elif column_name == 'low':
            return closing_price.PriceMin
        elif column_name == 'high':
            return closing_price.PriceMax
        elif column_name == 'yesterday':
            return closing_price.PriceYesterday
        elif column_name == 'open':
            return closing_price.PriceFirst
        elif column_name == 'count':
            return closing_price.ZTotTran
        elif column_name == 'vol':
            return closing_price.QTotTran5J
        elif column_name == 'value':
            return closing_price.QTotCap
        else:
            return None

    def should_update(self, deven, last_possible_deven):
        if (not deven) or deven == '0':
            return True #first time. never update
        today = datetime.date.today()
        today_deven = today.toString('yyyyMMdd')
        days_passed = abs((deven - last_possible_deven).days)
        in_weekend = today.getDay() in [4,5]
        last_update_weekday = datetime.date.fromString(last_possible_deven, 'yyyyMMdd').getDay()
        result = (
            days_passed >= UPDATE_INTERVAL and
            ((today.get_hour() > 16, True)[today_deven != last_possible_deven]) and # wait until the end of trading session
            # No update needed if: we are in weekend but ONLY if last time we updated was on last day (wednesday) of THIS week
            not (in_weekend and last_update_weekday != 3 and days_passed <= 3) # wait until the end of weekend
        )
        return result

    async def get_last_possible_deven(self):
        last_possible_deven = storage.get_item('tse.lastPossibleDeven')
        if (not last_possible_deven) or self.should_update(datetime.today().strftime('%Y%m%d'), last_possible_deven):
            try:
                res = await rq.last_possible_deven()
            except Exception as e:
                return { 'title': 'Failed request: LastPossibleDeven', 'detail': str(e) }
            pattern = re.compile("^\d{8};\d{8}$")
            if not pattern.match(res):
                return { 'title': 'Invalid server response: LastPossibleDeven' }
            last_possible_deven = res.split(';')[0] or res.split(';')[1] # todo: is it working?
            storage.set_item('tse.lastPossibleDeven', last_possible_deven)
        return last_possible_deven

    # todo: incomplte
    async def update_instruments(self):
        last_update = storage.get_item('tse.lastInstrumentUpdate')
        last_deven = None
        last_id = None
        current_instruments = None
        current_shares = None
        if not last_update:
            last_deven = 0
            last_id = 0
        else:
            current_instruments = utils.parse_instruments()
            current_shares = utils.parse_shares()
            ins_devens = object.keys(current_instruments).map(lambda k: +current_instruments[k].split(',',9)[8] ) # todo: is it working?
            share_ids = current_shares.map(lambda k: +k.split(',',1)[0]) # todo: is it working?
            last_deven = max(ins_devens)
            last_id = max(share_ids)

        last_possible_deven = await self.get_last_possible_deven()
        if(type(last_possible_deven) is dict):
            return last_possible_deven
        if self.should_update(last_deven, last_possible_deven):
            return
        try:
            res = await rq.instrument_and_share(datetime.date.today().strftime('%Y%m%d'), last_id)
        except Exception as e:
            return { 'title': 'Failed request: InstrumentAndShare', 'detail': str(e) }
        shares = await res.split('@')[1]
        try:
            instruments = await rq.instrument(last_deven)
        except Exception as e:
            return { 'title': 'Failed request: Instrument', 'detail': str(e) }

        # todo: add console instructions
        # if (instruments === '*') console.warn('Cannot update during trading session hours.');
        # if (instruments === '')  console.warn('Already updated: ', 'Instruments');
        # if (shares === '')       console.warn('Already updated: ', 'Shares');
        if instruments!='' and instruments!='*':
            rows = None
            if current_instruments:
                orig = dict(object.keys(current_instruments).map(lambda i: (
                    i = current_instruments[i].split(','),
                    i.length == 19 and (i[5] = i[18], i.pop()),
                    [i[0], i.join(',')] )))
                for v in instruments.split(';'):
                    i = v.split(',',1)[0]
                    orig[i] = v
                rows = object.keys(orig).map(lambda i: orig[i].split(','))
            else:
                rows = instruments.split(';').map(lambda i: i.split(','))
            _rows = rows.map(lambda i: [])
            dups = _rows.map(lambda i: i.length == 19 and (i[5] = i[18], i.pop()))
            code_idx = rows.map(lambda i,j: [i[0], j])
            for dup in dups:
                dup_sorted = dup.sort()

# %% class PricesUpdateManager

from threading import Timer
import numpy as np
class PricesUpdateManager:
    def __init__(self) -> None:
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
        if(self.succs.length == self.total or self.retries >= PRICES_UPDATE_RETRY_COUNT):
            _succs = np.array(self.succs)
            _fails = np.array(self.fails)
            self.succs = []
            self.fails = []
            await asyncio.gather(*self.writing)
            self.writing = []
    
    def on_result(self, response, chunk, id):
        ins_codes = chunk.map(lambda ins_code: ins_code) # todo: complete
        pattern = re.compile("^[\d.,;@-]+$")
        if (type(response) is str and (pattern.match(response) or response == '')):
            res = response.replace(';', '\n').split('@').map(lambda v, i: [chunk[i][0], v])
            for ins_code, new_data in res:
                self.succs.push(ins_code)
                if new_data:
                    old_data = Share.stored_prices[ins_code]
                    data = (new_data, old_data + '\n' + new_data)[old_data]
                    Share.stored_prices[ins_code] = data
                    Share.last_devens[ins_code] = new_data.split('\n')[-1].slice(-1)[0].split(',',2)[1]
                    self.writing.push(self.should_cache and Storage.set_item_async('tse.prices.'+ins_code, data))
            fails = fails.filter(lambda i: ins_codes.index_of(i) == -1)
            if(self.pf):
                filled = self.pSR.div(PRICES_UPDATE_RETRY_COUNT+2).mul(self.retries+1)
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
            pf(pn= pn+pR)

    # todo: complete
    def batch(self, chunks=[]):
        if self.qeud_retry:
            self.qeud_retry = None
        ids = chunks.map(lambda i, j: 'a'+i)
        for(i=0, delay=0, n=chunks.length; i<n; i++, delay+=PRICES_UPDATE_CHUNK_DELAY):
            id = ids[i]
            t = set_timeout(self.request, self.delay, chunks[i], id)
            self.timeouts.set(id, t)

    # todo: complete
    def start(self, update_needed=[], _should_cache=None, po={}):
        self.should_cache = _should_cache
        ({ self.pf, self.pn, self.ptot } = po)
        self.total = update_needed.length
        self.pSR = self.ptot / math.ceil(total / PRICES_UPDATE_CHUNK) # each successful request
        self.pR = self.pSR / (PRICES_UPDATE_RETRY_COUNT + 2);         # each request:
        self.succs = []
        self.fails = []
        self.retries = 0
        self.retry_chunks = []
        self.timeouts = map()
        self.qeud_retry = None
        chunks = np.array.split(update_needed, PRICES_UPDATE_CHUNK)
        self.batch(chunks)
        self.poll()
        return # new Promise(r => resolve = r);

# todo: complete
async def update_prices(selection=[], should_cache=None, {pf, pn, ptot}={}):
    last_devens = storage.get_item('tse.inscode_lastdeven')
    ins_codes = set()
    if last_devens:
        ents = last_devens.split('\n').map(lambda i: i.split(','))
        last_devens = Object.fromEntries(ents)
        ins_codes = set(object.keys(last_devens))
    else:
        last_devens = {}
    result = { succs: [], fails: [], error: undefined, pn }
    pfin = pn+ptot
    last_possible_deven = await getLastPossibleDeven()
    if type(last_possible_deven) is object:
        result.error = last_possible_deven
        if pf:
            pf(pn=pfin)
        return result

async def get_prices(symbols=[], _settings={}):
    if not symbols.length:
        return
    settings = {np.array(default_settings), np.array(_settings)}
    result = {data: [], error: None}
    { onprogress: pf, progressTotal: ptot } = settings
    if(type(pf) is not function):
        pf = None
    if(type(ptot) is not 'number'):
        ptot = DEFAULT_SETTINGS.progress_total
    pn = 0
    err = await update_instruments()
    if pf:
        pf(pn=pn+(ptot*0.01))
    if err:
        {title, detail} = err
    result.error = { code: 1, title, detail }
    if pf:
        pf(ptot)
        return result
    
    instruments = parse_instruments(true, undefined, 'Symbol')
    selection = symbols.map(lambda i: instruments[i])
    not_founds = symbols.filter(v,i : !selection[i])
    if pf:
        pf(pn= pn+(ptot*0.01))
    if not_founds.length:
        result.error = { code: 2, title: 'Incorrect Symbol', symbols: notFounds };
        if pf:
            pf(ptot)
            return result
    { merge_similar_symbols } = settings
    merges = map()
    extras_index = -1
    if merge_similar_symbols:
        syms = object.keys(instruments)
        ins = syms.map(lambda i : instruments[i])
        roots = set(ins.filter(i => i.SymbolOriginal).map(i => i.SymbolOriginal))

        merges = map(np.array(roots)).map(lambda i : i.symbol_original).map(lambda i : i.symbol_original)

        for i, j in ins:
            { SymbolOriginal: orig, Symbol: sym, InsCode: code } = i;
            rename_or_root = orig or symbols
            if not merges.has(rename_or_root):
                return
            regx = regexp(SYMBOL_RENAME_STRING+'(\\d+)')
            merges.get(rename_or_root).push({ sym, code, order: orig ? +sym.match(regx)[1] : 1 })
        for [, v] in np.array(merges):
            v.sort((a, b) : a.order - b.order)
        const extras = selection.map(({Symbol: sym}) =>
            merges.has(sym) &&
            merges.get(sym).slice(1).map(i => instruments[i.sym])
            ).flat().filter(i=>i);

        extras_index = selection.length
        selection.push(np.array(extras))

    update_result = await update_prices(selection, settings.cache, {pf, pn, ptot: ptot.mul(0.78)})
    { succs, fails, error } = updateResult
    ({ pn } = updateResult)

    if error:
        { title, detail } = error
        result.error = { code: 1, title, detail }
        if pf:
            pf(ptot)
        return result

    if fails.length:
        syms = Object.fromEntries( selection.map(i => [i.InsCode, i.Symbol]) )
        result.error = { code: 3, title: 'Incomplete Price Update',
            fails: fails.map(k => syms[k]),
            succs: succs.map(k => syms[k])
        }
        for v, i, a in selection:
            if fails.include(v.incode):
                a[i] = None
            else:
                a[i] = 0
    if merge_similar_symbols:
        selection.splice(extras_index)

    columns = settings.columns.map(lambda i:{
        const row = !Array.isArray(i) ? [i] : i;
        const column = new Column(row);
        const finalHeader = column.header || column.name;
        return { ...column, header: finalHeader };
    })

    { adjustPrices, daysWithoutTrade, startDate, csv } = settings;
    shares = parse_shares(true)
    pi = ptot * 0.20 / selection.length
    stored_prices_merged = {}

    if merge_similar_symbols:
        for [, merge] in merges:
            codes = merge.map(lambda i: i.code)
            [latest] = codes
            stored_prices_merged[latest] = codes.map(lambda code: stored_prices[code]).reverse().filter(i:i).join('\n')

    get_instrument_prices = (instrument) => {
        { InsCode: inscode, Symbol: sym, SymbolOriginal: symOrig }  = instrument

        prices, ins_codes = None

        if sym_orig:
            if merge_similar_symbols:
                return MERGED_SYMBOL_CONTENT
            prices = stored_prices[ins_code]
            ins_codes = set(ins_code)
        else:
            is_root = merges.has(sym)
            prices = is_root ? stored_prices_merged[ins_code] : stored_prices[ins_code]
            ins_codes = is_root ? merges[sym].map(i => i.code) : set(ins_code)

        if not prices:
            return

        prices = prices.split('\n').map(lambda i : closing_price(i))

        if adjust_prices == 1 or adjust_prices == 2:
            prices = adjust(adjust_prices, prices, shares, ins_codes)
        
        if not days_without_trade:
            prices = prices.filter(i : float(i.ZTotTran) > 0)

        prices = prices.filter(i: float(i.DEven) > float(start_date))

        return prices
    }

    if csv:
        { csvHeaders, csvDelimiter } = settings
        headers = ''
        if csv_headers:
            columns.map(lambda i: i.header).join()+'\n'
        result.data = selection.map(lambda instrument: {
            if not Instrument:
                return
            res = headers

            prices = get_instrument_prices(instrument)
            if not prices:
                return res
            if prices == MERGED_SYMBOL_CONTENT:
                return prices
            res += prices.map(lambda i: get_cell(i.name, Instrument, price)).join(csv_delimiter).join('\n')

            if pf:
                pf(pn = pn+pi)
            return res
        })
    else:
        text_cols = set(['CompanyCode', 'LatinName', 'Symbol', 'Name'])

        result.data = selection.map(Instrument => {
            if not Instrument:
                return
            res = Object.fromEntries( columns.map(i => [i.header, []]) )

            prices = get_instrument_prices(Instrument)
            if not prices:
                return res
            if prices == MERGED_SYMBOL_CONTENT:
                return prices

            for price in prices:
                for {header, name} in columns:
                    cell = get_cell(name, instrument, price)
                    res[header].push((float(cell), cell)[text_cols.has(name)])
            if pf:
                pf(pn = pn+pi)
            return res
        })
    
    if pf and pn != ptot:
        pf(pn=ptot)
    
    return result

async def get_instruments(struct=true, arr=true, structKey='InsCode'):
    valids = object.keys(instrument(np.array(18).keys()).join(','))
    if valids.indexOf(struct_key) == -1:
        struct_key = 'InsCode'
    
    last_update = storage.get_item('tse.lastInstrumentUpdate')
    err = await update_instruments()
    if err and not last_update:
        raise err
    return parse_instruments(struct, arr, structKey)

# todo: ln1055 - ln1090 => intraday libs

stored = {}

zip, unzip = None
  zip   = str => gzipSync(str);
  unzip = buf => gunzipSync(buf).toString();

def objify(map, r={}):
    for k, v in map.items():
        if(Map.prototype.toString.call(v) == '[object Map]' or type(v) is array):
            r[k] = objify(v, r[k])
        else:
            r[k] = v
    return r

def parse_raw(separator, text):
    split_str = text.split(separator)[1].split('];', 1)[0]
    split_str = '[' + split_str.replace('\'', '"') + ']'
    arr = JSON.parse(split_str)
    return arr

async def extract_and_store(ins_code='', deven_text=[], should_cache):
    if not stored[ins_code]:
        stored[ins_code] = {}
    stored_instrument = stored[ins_code]

    for deven, text in deven_text.items:
        if text == 'N/A':
            stored_instrument[deven] = text
            continue
        ClosingPrice    = parse_raw('var ClosingPriceData=[', text)
        BestLimit       = parse_raw('var BestLimitData=[', text)
        IntraTrade      = parse_raw('var IntraTradeData=[', text)
        ClientType      = parse_raw('var ClientTypeData=[', text)
        InstrumentState = parse_raw('var InstrumentStateData=[', text)
        StaticTreshhold = parse_raw('var StaticTreshholdData=[', text)
        InstSimple      = parse_raw('var InstSimpleData=[', text)
        ShareHolder     = parse_raw('var ShareHolderData=[', text)
        
        coli = [12,2,3,4,6,7,8,9,10,11]
        price = ClosingPrice.map(lambda row: coli.map(lambda i: row[i]).join(',')).join('\n')

        coli = [0,1,2,3,4,5,6,7]
        order = BestLimit.map(lambda row: coli.map(lambda i: row[i]).join(',')).join('\n')

        coli = [1,0,2,3,4]
        trade = IntraTrade.map(lambda row: {
            let [h,m,s] = row[1].split(':');
            let timeint = (+h*10000) + (+m*100) + (+s) + '';
            row[1] = timeint;
            return coli.map(i => row[i]);
        }).sort((a,b)=>+a[0]-b[0]).map(i=>i.join(',')).join('\n')

        coli = [4,0,12,16,8,6,2,14,18,10,5,1,13,17,9,7,3,15,19,11,20]
        client = coli.map(lambda i: ClientType[i]).join(',')

        [a, b] = [InstrumentState, StaticTreshhold]
        state = ('', a[0][2])[a.length and a[0].length]
        day_min, day_max = None
        if(b.length and b[1].length):
            day_min = b[1][2]
            day_max = b[1][1]
        [flow, base_vol] = [4, 9].map(lambda i: inst_simple[i])
        misc = [basevol, flow, daymin, daymax, state].join(',')

        coli = [2,3,4,0,5]
        share_holder = ShareHolder.filter(i: i[4].map(lambda row: {
            row[4] = ({ArrowUp:'+', ArrowDown:'-'})[row[4]];
            row[5] = cleanFa(row[5]);
            return coli.map(i => row[i]).join(',');
        }).join('\n')))

        file = [price, order, trade, client, misc]
        if share_holder:
            file.push(share_holder)
        stored_instrument[deven] = zip(file.join('\n\n'))
    
    o = stored_instrument
    rdy = object.keys(o).filter(k: o[k] != true).reduce((r, k): r[k], r), {})
    if should_cache:
        return Storage.itd.set_item(ins_code, rdy)

# todo: ln1185 - ln1517 intraday libs

{
  getPrices,
  getInstruments,
  
  get API_URL() { return API_URL; },
  set API_URL(v) {
    if (typeof v !== 'string') return;
    let bad;
    try { new URL(v); } catch (e) { bad = true; throw e; }
    if (!bad) API_URL = v;
  },
  
  get UPDATE_INTERVAL() { return UPDATE_INTERVAL; },
  set UPDATE_INTERVAL(v) { if (Number.isInteger(v)) UPDATE_INTERVAL = v; },
  
  get PRICES_UPDATE_CHUNK() { return PRICES_UPDATE_CHUNK; },
  set PRICES_UPDATE_CHUNK(v) { if (Number.isInteger(v) && v > 0 && v < 60) PRICES_UPDATE_CHUNK = v; },
  
  get PRICES_UPDATE_CHUNK_DELAY() { return PRICES_UPDATE_CHUNK_DELAY; },
  set PRICES_UPDATE_CHUNK_DELAY(v) { if (Number.isInteger(v)) PRICES_UPDATE_CHUNK_DELAY = v; },
  
  get PRICES_UPDATE_RETRY_COUNT() { return PRICES_UPDATE_RETRY_COUNT; },
  set PRICES_UPDATE_RETRY_COUNT(v) { if (Number.isInteger(v)) PRICES_UPDATE_RETRY_COUNT = v; },
  
  get PRICES_UPDATE_RETRY_DELAY() { return PRICES_UPDATE_RETRY_DELAY; },
  set PRICES_UPDATE_RETRY_DELAY(v) { if (Number.isInteger(v)) PRICES_UPDATE_RETRY_DELAY = v; },
  
  get columnList() {
    return [...Array(15)].map((v,i) => ({name: cols[i], fname: colsFa[i]}));
  },
  
  getIntraday,
  getIntradayInstruments,
  
  get INTRADAY_URL() { return INTRADAY_URL; },
  set INTRADAY_URL(v) {
    if (typeof v !== 'function') return;
    let bad;
    try { new URL(v()); } catch (e) { bad = true; throw e; }
    if (!bad) INTRADAY_URL = v;
  },
  
  get INTRADAY_UPDATE_CHUNK_DELAY() { return INTRADAY_UPDATE_CHUNK_DELAY; },
  set INTRADAY_UPDATE_CHUNK_DELAY(v) { if (Number.isInteger(v)) INTRADAY_UPDATE_CHUNK_DELAY = v; },
  
  get INTRADAY_UPDATE_CHUNK_MAX_WAIT() { return INTRADAY_UPDATE_CHUNK_MAX_WAIT; },
  set INTRADAY_UPDATE_CHUNK_MAX_WAIT(v) { if (Number.isInteger(v)) INTRADAY_UPDATE_CHUNK_MAX_WAIT = v; },
  
  get INTRADAY_UPDATE_RETRY_COUNT() { return INTRADAY_UPDATE_RETRY_COUNT; },
  set INTRADAY_UPDATE_RETRY_COUNT(v) { if (Number.isInteger(v)) INTRADAY_UPDATE_RETRY_COUNT = v; },
  
  get INTRADAY_UPDATE_RETRY_DELAY() { return INTRADAY_UPDATE_RETRY_DELAY; },
  set INTRADAY_UPDATE_RETRY_DELAY(v) { if (Number.isInteger(v)) INTRADAY_UPDATE_RETRY_DELAY = v; },
  
  get INTRADAY_UPDATE_SERVERS() { return INTRADAY_UPDATE_SERVERS; },
  set INTRADAY_UPDATE_SERVERS(v) { if (Array.isArray(v) && !v.some(i => !Number.isInteger(i) || i < 0)) INTRADAY_UPDATE_SERVERS = v; },
  
  itdGroupCols
};


Object.defineProperty(instance, 'CACHE_DIR', {
    get: () => storage.CACHE_DIR,
    set: v => storage.CACHE_DIR = v
});
module.exports = instance;