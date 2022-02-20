# %% Configuration
API_URL = 'http:#service.tsetmc.com/tsev2/data/TseClient2.aspx'
TSE_CATCH_DIR = 'tse-catch'
PATH_FILE_NAME = '.tse'
PRICES_DIR = 'prices'
INTRADAY_DIR = 'intraday'
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



# %% Class storage
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile


class storage:
    def __init__(self) -> None:
        home = Path.home()
        default_dir = home / TSE_CATCH_DIR
        self._data_dir = default_dir
        path_file = home / PATH_FILE_NAME
        if path_file.is_file():
            with open(path_file, 'r', encoding='utf-8') as f:
                _data_dir = Path(f.readline())
                if _data_dir.is_dir:
                    self._data_dir = _data_dir
        else:
            with open(path_file, 'w+', encoding='utf-8') as f:
                f.write(str(self._data_dir))
    
    # todo: add use read_csv if possible
    def get_item(self, key: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            return f.read()

    def set_item(self, key: str, value: str):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        file_path = dir / (key + '.csv')
        with open(file_path, 'w+', encoding='utf-8') as f:
            f.write(value)

    def get_item_async(self, key: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        if zip:
            file_path = dir / (key + '.gz')
            with ZipFile(file_path) as zf:
                with open(zf, 'w+') as infile:
                    return infile.read()
        else:
            file_path = dir / (key + '.csv')
            with open(file_path, 'w+', encoding='utf-8') as f:
                return f.read()

    def set_item_async(self, key: str, value: str, zip=False):
        key = key.replace('tse.', '')
        dir = self._data_dir
        if key.startswith('prices.'):
            dir = self._data_dir / PRICES_DIR
        if zip:
            file_path = dir / (key + '.gz')
            with ZipFile(file_path, 'w') as zf:
                zf.write(value)
        else:
            file_path = dir / (key + '.csv')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(value)

    def get_items(self, keys: list) -> dict:
        result = {}
        dir = self._data_dir / PRICES_DIR
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.csv', '')
                if key in keys:
                    result[key] = self.get_item(key)
        return result

    # get intraday item
    # todo: line 83 tse.js is not correctly ported to python
    def itd_get_items(self, keys: list, full=False):
        result = {}
        dir = self._data_dir / INTRADAY_DIR
        p = dir.glob('**/*')
        for x in p:
            if x.is_file():
                key = x.name.replace('.gz', '').replace('.csv', '')
                if key in keys:
                    result[key] = self.itd_get_item(key, full)
        return result

    # set intraday item
    # todo: line 107 tse.js is not correctly ported to python
    def itd_set_item(self, key: str, obj: dict):
        key = key.replace('tse.', '')
        dir = self._data_dir / INTRADAY_DIR
        for k in obj:
            file_path = dir / (key + '.' + k + '.gz')
            with open(file_path, 'w+', encoding='utf-8') as f:
                f.write(obj[k])


    @property
    def catch_dir(self):
        return self._data_dir

    @catch_dir.setter
    def catch_dir(self, value: str):
        self._data_dir = Path(value)


# %% Class rq
from urllib.parse import urlparse, urlencode
import aiohttp
import asyncio


class rq:
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


class closing_price:
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


class column:
    def __init__(self, row=[]):
        len = row.length
        if(len > 2 or len < 1):
            raise Exception('Invalid column data')
            self.name = COLS[row[0]]
            self.fname = COLS_FA[row[0]]
            self.header = row[1]


class instrument:
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


class instrument_itd:
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


class share:
    def __init__(self, _row=''):
        row = row.split(',')
        if row.length != 5:
            raise Exception('Invalid share data')
        self.Idn = row[0];  # long
        self.InsCode = row[1];  # long
        self.DEven = row[2];  # int32
        self.NumberOfShareNew = int(row[3]);  # decimal
        self.NumberOfShareOld = int(row[4]);  # decimal


# %% utils
def parse_instruments(struct=False, arr=False, struct_key='InsCode', itd=False):
    instruments = None
    rows = storage.get_item('tse.instruments'+('', '.intraday')[itd])
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

class data_manager:
    def __init__(self):
        self.last_devens = {}
        self.stored_prices = {}
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