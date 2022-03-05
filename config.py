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
                       'CComVal', 'CSecVal', 'CSoSecVal', 'YVal',
                       'SymbolOriginal']
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
PRICES_UPDATE_RETRY_DELAY = 1000
SYMBOL_RENAME_STRING = '-ق'
MERGED_SYMBOL_CONTENT = 'merged'
defaultSettings = {
    'columns': [0, 2, 3, 4, 5, 6, 7, 8, 9],
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
}
TRADING_SEASSON_END = 16
