"""
confuguration file for the package
"""
# data files
storage = dict(
    TSE_CACHE_DIR='tse-cache',
    PATH_FILE_NAME='.tse',
    PRICES_DIR='prices',
    INTRADAY_DIR='intraday'
)

# data sructures
tse_instrument_info = ['InsCode', 'InstrumentID', 'LatinSymbol', 'LatinName',
                       'CompanyCode', 'Symbol', 'Name', 'CIsin', 'DEven',
                       'Flow', 'LSoc30', 'CGdSVal', 'CGrValCot', 'YMarNSC',
                       'CComVal', 'CSecVal', 'CSoSecVal', 'YVal']
tse_instrument_itd_info = ['InsCode', 'LVal30', 'LVal18AFC', 'FlowTitle',
                           'CGrValCotTitle', 'Flow', 'CGrValCot', 'CIsin',
                           'InstrumentID', 'ZTitad', 'BaseVol']
tse_share_info = ['Idn', 'InsCode', 'DEven',
                  'NumberOfShareNew', 'NumberOfShareOld']
tse_closing_prices_info = ['DEven', 'InsCode', 'PClosing', 'PDrCotVal', 'PriceFirst',
                           'PriceMax', 'PriceMin', 'PriceYesterday', 'QTotCap', 'QTotTran5J', 'ZTotTran']

# data services
UPDATE_INTERVAL = 1
PRICES_UPDATE_CHUNK = 50
PRICES_UPDATE_CHUNK_DELAY = 300
PRICES_UPDATE_RETRY_COUNT = 3
PRICES_UPDATE_RETRY_DELAY = 1
SYMBOL_RENAME_STRING = '-ق'
MERGED_SYMBOL_CONTENT = 'merged'
default_settings = {
    'columns': [0, 2, 3, 4, 5, 6, 7, 8, 9],
    'adjust_prices': 0,
    'days_without_trade': False,
    'start_date': '20010321',
    'merge_similar_symbols': True,
    'cache': True,
    'csv': False,
    'csv_headers': True,
    'csv_delimiter': ',',
    'on_progress': None,
    'progress_tot': 100
}
TRADING_SEASSON_END = 16

API_URL = 'http://service.tsetmc.com/tsev2/data/TseClient2.aspx'

# Column class
cols = ['date', 'dateshamsi', 'open', 'high', 'low', 'last', 'close', 'vol',
        'count', 'value', 'yesterday', 'symbol', 'name', 'namelatin', 'companycode']
cols_fa = ['تاریخ میلادی', 'تاریخ شمسی', 'اولین قیمت', 'بیشترین قیمت', 'کمترین قیمت', 'آخرین قیمت', 'قیمت پایانی',
           'حجم معاملات', 'تعداد معاملات', 'ارزش معاملات', 'قیمت پایانی دیروز', 'نماد', 'نام', 'نام لاتین', 'کد شرکت', ]

