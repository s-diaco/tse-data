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
tse_share_info = ['Idn', 'InsCode', 'DEven', 'NumberOfShareNew', 'NumberOfShareOld']
