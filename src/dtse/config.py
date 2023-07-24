"""
Confuguratios for the package
"""

# data files
from dataclasses import dataclass


storage = dict(
    TSE_CACHE_DIR="tse-cache",
    PATH_FILE_NAME=".tse",
    PRICES_DIR="prices",
    INTRADAY_DIR="intraday",
    DB_FILE_NAME="tse_data.sqlite3",
)

# data sructures
tse_instrument_info = [
    "InsCode",
    "InstrumentID",
    "LatinSymbol",
    "LatinName",
    "CompanyCode",
    "Symbol",
    "Name",
    "CIsin",
    "DEven",
    "Flow",
    "LSoc30",
    "CGdSVal",
    "CGrValCot",
    "YMarNSC",
    "CComVal",
    "CSecVal",
    "CSoSecVal",
    "YVal",
]
tse_instrument_itd_info = [
    "InsCode",
    "LVal30",
    "LVal18AFC",
    "FlowTitle",
    "CGrValCotTitle",
    "Flow",
    "CGrValCot",
    "CIsin",
    "InstrumentID",
    "ZTitad",
    "BaseVol",
]
tse_share_info = ["Idn", "InsCode", "DEven", "NumberOfShareNew", "NumberOfShareOld"]
tse_closing_prices_info = [
    "InsCode",
    "DEven",
    "PClosing",
    "PDrCotVal",
    "ZTotTran",
    "QTotTran5J",
    "QTotCap",
    "PriceMin",
    "PriceMax",
    "PriceYesterday",
    "PriceFirst",
]

# data services
UPDATE_INTERVAL = 1
PRICES_UPDATE_CHUNK = 50
PRICES_UPDATE_CHUNK_DELAY = 0.5
PRICES_UPDATE_RETRY_COUNT = 3
PRICES_UPDATE_RETRY_DELAY = 1
SYMBOL_RENAME_STRING = "-ق"
MERGED_SYMBOL_CONTENT = "merged"
default_settings = {
    "columns": [0, 2, 3, 4, 5, 6, 7, 8, 9],
    "adjust_prices": 0,
    "days_without_trade": False,
    "start_date": "20120321",
    "merge_similar_symbols": True,
    "cache": True,
    "csv": False,
    "csv_headers": True,
    "csv_delimiter": ",",
    "on_progress": None,
    "progress_tot": 100,
}
TRADING_SEASSON_END = 16

API_URL = "http://service.tsetmc.com/tsev2/data/TseClient2.aspx"

# Column class
cols = [
    "date",
    "dateshamsi",
    "open",
    "high",
    "low",
    "last",
    "close",
    "vol",
    "count",
    "value",
    "yesterday",
    "symbol",
    "name",
    "namelatin",
    "companycode",
]
cols_fa = [
    "تاریخ میلادی",
    "تاریخ شمسی",
    "اولین قیمت",
    "بیشترین قیمت",
    "کمترین قیمت",
    "آخرین قیمت",
    "قیمت پایانی",
    "حجم معاملات",
    "تعداد معاملات",
    "ارزش معاملات",
    "قیمت پایانی دیروز",
    "نماد",
    "نام",
    "نام لاتین",
    "کد شرکت",
]

renames = {
    "InsCode": "companycode",  # int64
    "DEven": "date",  # int32
    "PClosing": "close",
    "PDrCotVal": "last",
    "ZTotTran": "count",  # تعداد معاملات
    "QTotTran5J": "volume",  # حجم معاملات
    "QTotCap": "value",  # ارزش معاملات
    "PriceMin": "min",
    "PriceMax": "max",
    "PriceYesterday": "yesterday",
    "PriceFirst": "first",
}

tse_markets = {
    "N": "بورس",
    "Z": "فرابورس",
    "D": "فرابورس",
    "A": "پایه زرد",
    "P": "پایه زرد",
    "C": "پایه نارنجی",
    "L": "پایه قرمز",
    "W": "کوچک و متوسط فرابورس",
    "V": "کوچک و متوسط فرابورس",
}

# ---------------------------------------------------------------------------------------
# names for data columns of daily prices
# ---------------------------------------------------------------------------------------


@dataclass
class PriceColTypical:
    """
    Typical column names for prices
    """

    code: str = "companycode"
    date: str = "date"
    close: str = "close"
    last: str = "last"
    count: str = "count"
    volume: str = "volume"
    value: str = "value"
    min: str = "min"
    max: str = "max"
    yesterday: str = "yesterday"
    first: str = "first"


@dataclass
class PriceColOrig:
    """
    Column names for price files according to:
    http://old.tsetmc.com/Site.aspx?ParTree=111411111Z
    """

    code: str = "InsCode"
    date: str = "DEven"
    close: str = "PClosing"
    last: str = "PDrCotVal"
    count: str = "ZTotTran"
    volume: str = "QTotTran5J"
    value: str = "QTotCap"
    min: str = "PriceMin"
    max: str = "PriceMax"
    yesterday: str = "PriceYesterday"
    first: str = "PriceFirst"


@dataclass
class PriceColFa:
    """
    Column names for prices in persian
    """

    code: str = "کد شرکت"
    date: str = "تاریخ میلادی"
    close: str = "قیمت پایانی"
    last: str = "آخرین قیمت"
    count: str = "تعداد معاملات"
    volume: str = "حجم معاملات"
    value: str = "ارزش معاملات"
    min: str = "کمترین قیمت"
    max: str = "بیشترین قیمت"
    yesterday: str = "قیمت پایانی دیروز"
    first: str = "اولین قیمت"


PRICE_COL_NAMES = PriceColTypical
