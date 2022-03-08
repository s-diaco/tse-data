
"""
data structures
"""
import config as settings
from tse_utils import clean_fa


class TSEClosingPrice:
    """
    daily price class
    """
    def __init__(self, row='', **kwargs):
        """
        constructor for TSEClosingPrice class
        """
        if kwargs:
            self.__dict__.update(kwargs)
        else:
            row = row.split(',')
            if len(row) != 11:
                raise Exception('Invalid ClosingPrice row')
            self.ins_code = row[0]  # int64
            self.DEven = row[1]  # int32
            self.PClosing = row[2]  # close
            self.PDrCotVal = row[3]  # last
            self.ZTotTran = row[4]  # تعداد معاملات
            self.QTotTran5J = row[5]  # حجم معاملات
            self.QTotCap = row[6]  # ارزش معاملات
            self.PriceMin = row[7]  # low
            self.PriceMax = row[8]  # high
            self.PriceYesterday = row[9]  # yesterday
            self.PriceFirst = row[10]  # open

    def __str__(self):
        """
        string representation of TSEClosingPrice class
        """
        fields = ['    {}={!r}'.format(k, v)
                  for k, v in self.__dict__.items() if not k.startswith('_')]

        return '{}(\n{})'.format(self.__class__.__name__, ',\n'.join(fields))


class TSEColumn:
    """
    column class
    """
    def __init__(self, row):
        """
        constructor for TSEColumn class
        """
        row_len = len(row)
        if(row_len > 2 or row_len < 1):
            raise Exception('Invalid column data')
        self.name = settings.cols[row[0]]
        self.fname = settings.cols_fa[row[0]]
        self.header = row[1]


class TSEInstrument:
    """
    tse instrument class
    """
    def __init__(self, row=''):
        """
        constructor for TSEInstrument class
        """
        row = row.split(',')
        if not (len(row) in [18, 19]):
            raise Exception('Invalid instrument data')
        self.InsCode = row[0]  # int64 (کد داخلي نماد)
        self.InstrumentID = row[1]  # کد 12 رقمي لاتين نماد
        self.LatinSymbol = row[2]  # string
        self.LatinName = row[3]  # string
        self.CompanyCode = row[4]  # string
        self.Symbol = clean_fa(row[5]).trim()  # string
        self.Name = row[6]  # string
        self.CIsin = row[7]  # کد 12 رقمي شرکت
        self.DEven = row[8]  # int32 (date)
        self.Flow = row[9]  # 0,1,2,3,4,5,6,7 بازار
        self.LSoc30 = row[10]  # نام 30 رقمي فارسي شرکت
        self.CGdSVal = row[11]  # A,I,O نوع نماد
        self.CGrValCot = row[12]  # 00,11,1A,...25 کد گروه نماد
        self.YMarNSC = row[13]  # NO,OL,BK,BY,ID,UI کد بازار
        self.CComVal = row[14]  # 1,3,4,5,6,7,8,9 کد تابلو
        self.CSecVal = row[15]  # []62 کد گروه صنعت
        self.CSoSecVal = row[16]  # []177 کد زیر گروه صنعت
        self.YVal = row[17]  # string نوع نماد (شاخص صندوق اوراق ... )
        if row[18]:
            self.SymbolOriginal = clean_fa(row[18]).trim()  # string


class TSEInstrumentItd:
    """
    intraday instrument class
    """
    def __init__(self, row='') -> None:
        """
        constructor for TSEInstrumentItd class
        """
        row = row.split(',')
        if len(row) != 11:
            raise Exception('Invalid InstrumentItd data')
        self.InsCode = row[0]
        self.LVal30 = clean_fa(row[1]).trim()  # نام 30 رقمي فارسي نماد
        self.LVal18AFC = clean_fa(row[2]).trim()  # کد 18 رقمي فارسي نماد
        self.FlowTitle = clean_fa(row[3]).trim()
        self.CGrValCotTitle = clean_fa(row[4]).trim()
        self.Flow = row[5]
        self.CGrValCot = row[6]
        self.CIsin = row[7]
        self.InstrumentID = row[8]
        self.ZTitad = row[9]  # تعداد سھام
        self.BaseVol = row[10]  # حجم مبنا


class TSEShare:
    """
    TSE share class
    """
    def __init__(self, row='', **kwargs):
        """
        constructor for TSEShare class
        """
        if kwargs:
            self.__dict__.update(kwargs)
            self.NumberOfShareNew = int(self.NumberOfShareNew)  # decimal
            self.NumberOfShareOld = int(self.NumberOfShareOld)  # decimal
        else:
            row = row.split(',')
            if len(row) != 5:
                raise Exception('Invalid share data')
            self.Idn = row[0]  # long
            self.InsCode = row[1]  # long
            self.DEven = row[2]  # int32
            self.NumberOfShareNew = int(row[3])  # decimal
            self.NumberOfShareOld = int(row[4])  # decimal

    def __str__(self):
        """
        string representation of TSEShare class
        """
        fields = ['    {}={!r}'.format(k, v)
                  for k, v in self.__dict__.items() if not k.startswith('_')]

        return '{}(\n{})'.format(self.__class__.__name__, ',\n'.join(fields))
