"""
Manage TSE Data
"""


import numbers
import re

from . import config as cfg
from . import data_services as data_svs
from .data_structs import TSEColumn, TSEInstrument
from .price_update_helper import PricesUpdateHelper
from .setup_logger import logger as tse_logger
from .storage import Storage as strg
from .tse_parser import parse_instruments


# todo: complete
async def update_prices(self, selection=None, should_cache=None, percents=None):
    """
    update prices

    :selection: list, instruments to update
    :should_cache: bool, should cache prices in csv files
    :percents: dict, data needed for progress bar
    """
    last_devens = strg.get_item('tse.inscode_lastdeven')
    ins_codes = None
    if last_devens:
        ins_codes = last_devens[1:]
    else:
        last_devens = {}
    result = {"succs": [], "fails": [], "error": None, "pn": percents.pn}
    prog_fin = percents.pn+percents.ptot
    last_possible_deven = await data_svs.get_last_possible_deven()
    if last_possible_deven:
        result["error"] = last_possible_deven
        if callable(percents.progress_func):
            percents.prog_func(prog_fin)
        return result
    to_update = []
    first_possible_deven = cfg.default_settings["start_date"]
    for instrument in selection:
        ins_code = instrument.ins_code
        market = (instrument.YMarNSC != 'NO')
        if ins_code not in ins_codes:
            # doesn't have data
            to_update.append([ins_code, first_possible_deven, market])
        else:
            # has data
            last_deven = last_devens[ins_code]
            if not last_deven:
                to_update = None  # expired symbol
            if (data_svs.should_update(last_deven, last_possible_deven)):
                # has data but outdated
                to_update.append([ins_code, last_deven, market])
    if callable(percents.progress_func):
        percents.prog_func(percents.pn+percents.ptot*(0.01))

    sel_ins = list.map(lambda x: x.ins_code, selection)
    stored_ins = list.map(lambda x: x.keys, PricesUpdateHelper.stored_prices)
    if (not stored_ins) or (not sel_ins):
        await strg.get_items(sel_ins, PricesUpdateHelper.stored_prices)
    if callable(percents.progress_func):
        percents.prog_func(percents.pn+percents.ptot*(0.01))

    if to_update:
        progress_tuple = (percents.progress_func, percents.pn,
                          percents.ptot - percents.ptot*(0.02))
        manager_result = PricesUpdateHelper().start(
            update_needed=to_update,
            should_cache=should_cache,
            progress_tuple=progress_tuple
        )
        self.succs, self.fails = manager_result
        percents.pn = manager_result

        if self.succs and should_cache:
            await strg.set_items('tse.inscode_lastdeven', last_devens)
        result = (self.succs, self.fails)
    if callable(percents.progress_func) and percents.pn != prog_fin:
        percents.prog_func(prog_fin)
    result.pn = prog_fin

    return result


async def get_prices(symbols=None, conf=None):
    """
    get prices for symbols
    :symbols: list, symbols to get prices for
    :_settings: dict, settings to use
    :return: dict, prices for symbols
    """
    if not symbols:
        return {}
    settings = cfg.default_settings
    if conf:
        settings.update(conf)
    result = {"data": [], "error": None}
    prog_func = settings.get('on_progress')
    if not callable(prog_func):
        prog_func = None
    prog_tot = settings.get('progress_tot')
    if not isinstance(prog_tot, numbers.Number):
        prog_tot = cfg.default_settings['progress_tot']
    prog_n = 0
    await data_svs.update_instruments()
    if callable(prog_func):
        prog_n = prog_n+(prog_tot*0.01)
        prog_func(prog_n)
    instruments = await parse_instruments()
    selected_symbols = [sym for sym in symbols if sym in instruments['Symbol'].values]
    instrums_dict = {ins['Symbol']: TSEInstrument(ins) for _, ins in instruments.iterrows()}
    selection = [instrums_dict[sym] for sym in selected_symbols]
    if not selection:
        raise ValueError(f"No instruments found for symbols: {symbols}.")
    not_founds = [sym for sym in symbols if sym not in selected_symbols]
    if callable(prog_func):
        prog_n = prog_n+(prog_tot*0.01)
        prog_func(prog_n)
    if not_founds:
        tse_logger.warning("symbols not found: %s", not_founds)
        if callable(prog_func):
            prog_func(prog_tot)

    merge_similar_symbols = settings['merge_similar_symbols']
    merges = {}
    extras_index = -1

    if merge_similar_symbols:
        syms = instruments.keys
        roots = instruments['SymbolOriginal'][instruments['SymbolOriginal'].notna()]
        merges = list(map((lambda x: [x, []]), roots))

        for i in instruments.itertuples():
            orig = i.SymbolOriginal
            sym = i.Symbol
            code = i.InsCode
            renamed_or_root = orig or sym
            if not renamed_or_root in merges:
                return
            pattern = re.compile(cfg.SYMBOL_RENAME_STRING + r'\d+')
            order = int(pattern.match(sym)[1]) if orig else 1
            merges[renamed_or_root].append({sym, code, order})
            merges.sort(key=(lambda x: x.order))
            extras = selection - merges
            extras_index = len(selection)
            selection.extend(extras)

    update_result = await update_prices(selection,
                                        settings.cache,
                                        (prog_func, prog_n, prog_tot*0.78))
    succs, fails, error = update_result
    prog_n = update_result
    if error:
        err = (error.title, error.detail)
        result["error"] = (1, err)
        if callable(prog_func):
            prog_func(prog_tot)
        return result

    if fails:
        syms = [(i.ins_code, i.SymbolOriginal) for i in selection]
        title = "Incomplete Price Update"
        succs = list(map((lambda x: syms[x]), succs))
        fails = list(map((lambda x: syms[x]), fails))
        err = (title, succs, fails)
        result["error"] = (3, err)
        for v, i, a in selection:
            if fails.include(v.ins_code):
                a[i] = None
            else:
                a[i] = 0
    if merge_similar_symbols:
        selection = selection[:extras_index]

    columns = settings['columns']

    def col(col_name):
        row = col_name
        column = TSEColumn(row)
        final_header = column.header or column.name
        return {column, final_header}

    columns = list(map(col, settings['columns']))

    adjust_prices = settings['adjust_prices']
    days_without_trade = settings['days_without_trade']
    start_date = settings['start_date']
    csv = settings['csv']
    shares = await strg.read_tse_csv('tse.shares')
    pi = prog_tot * 0.20 / selection.length
    stored_prices_merged = {}

    if merge_similar_symbols:
        for merge in merges:
            codes = [i.code for i in merge.values()]
            stored_prices_merged[codes] = list(map((lambda x: PricesUpdateHelper.stored_prices[x]), codes)).reverse()

    if csv:
        csv_headers = settings['csv_headers']
        csv_delimiter = settings['csv_delimiter']
        headers = ''
        if csv_headers:
            headers = list(map((lambda i: i.header), columns)).join()+'\n'
            
        def map_selection(instrument):
            if not instrument:
                return
            res = headers
            prices = get_instrument_prices(instrument)
            if not prices:
                return res
            if prices == cfg.MERGED_SYMBOL_CONTENT:
                return prices

            res += list(map((lambda price: list(map((lambda i: data_svs.get_cell(i.name, instrument, price).join(csv_delimiter)),columns))), prices)).join('\n')
            if callable(prog_func):
                pn = pn+pi
                prog_func(pn)
            return res

        result["data"] = list(map(map_selection, selection))
    else:
        text_cols = set(['CompanyCode', 'LatinName', 'Symbol', 'Name'])

        def map_selection(instrument):
            if not instrument:
                return
            res = list(map((lambda x: [x.header, []]), columns))
            prices = get_instrument_prices(instrument)
            if not prices:
                return res
            if prices == cfg.MERGED_SYMBOL_CONTENT:
                return prices
            for price in prices:
                for header, name in columns:
                    cell = data_svs.get_cell(name, instrument, price)
                    res[header].push(cell if (name in text_cols) else float(cell))

        result["data"] = list(map(map_selection, selection))

    if prog_func and prog_n != prog_tot:
        prog_n = prog_tot
        prog_func(prog_n)

    return result


async def get_instruments(struct=True, arr=True, structKey='InsCode'):
    """
    get instruments
    :struct: bool, return structure
    :arr: bool, return array
    :structKey: str, key to use for structure
    :return: dict, instruments
    """
    valids = None
    # TODO: complete
    if valids.indexOf(struct_key) == -1:
        struct_key = 'InsCode'

    last_update = strg.get_item('tse.lastInstrumentUpdate')
    err = await data_svs.update_instruments()
    if err and not last_update:
        raise err
    return await strg.read_tse_csv('tse.instruments')


def get_instrument_prices(instrument,
                          start_date,
                          shares,
                          merge_similar_symbols=True,
                          merges=None, settings=None,
                          stored_prices=None,
                          stored_prices_merged=None,
                          adjust_prices=True,
                          days_without_trade=0):
    """
    get instrument prices
    :instrument: str, instrument to get prices for
    :merge_similar_symbols: bool, merge similar symbols
    :merges: dict, merges to use
    :settings: dict, settings to use
    :stored_prices: dict, stored prices to use
    :stored_prices_merged: dict, stored prices merged to use
    :adjust_prices: bool, adjust prices
    :days_without_trade: int, days without trade
    :return: dict, prices for instrument
    """
    ins_code, sym, sym_orig = instrument

    prices, ins_codes = None

    if sym_orig:
        if merge_similar_symbols:
            return settings["MERGED_SYMBOL_CONTENT"]
        prices = stored_prices[ins_code]
        ins_codes = set(ins_code)
    else:
        is_root = merges.has(sym)
        prices = (stored_prices[ins_code],
                  stored_prices_merged[ins_code])[is_root]
        ins_codes = (set(ins_code), merges[sym].map(lambda i: i.code))[is_root]

    if not prices:
        return

    if adjust_prices == 1 or adjust_prices == 2:
        prices = data_svs.adjust(adjust_prices, prices, shares, ins_codes)

    if not days_without_trade:
        prices = prices.filter(lambda i: float(i.ZTotTran) > 0)

    prices = prices.filter(lambda i: float(i.DEven) > float(start_date))

    return prices
