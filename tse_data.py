import numbers
import re

import config as cfg
import data_services as data_svs
from price_update_helper import PricesUpdateHelper
from storage import Storage as strg


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
        manager_result = PricesUpdateHelper(to_update, should_cache, (pf, pn, ptot: ptot - ptot*(0.02)))
        self.succs, self.fails = manager_result
        pn = manager_result

        if self.succs and should_cache:
            await strg.set_items('tse.inscode_lastdeven', last_devens)
        result = (self.succs, self.fails)
    if callable(percents.progress_func) and pn != prog_fin:
        percents.prog_func(prog_fin)
    result.pn = prog_fin

    return result


async def get_prices(symbols=None, _settings=None):
    """
    get prices for symbols
    :symbols: list, symbols to get prices for
    :_settings: dict, settings to use
    :return: dict, prices for symbols
    """
    if not symbols:
        return {}
    settings = _settings if _settings else cfg.default_settings
    result = {"data": [], "error": None}
    prog_func = settings['on_progress']
    if not callable(prog_func):
        prog_func = None
    prog_tot = settings['progress_tot']
    if not isinstance(prog_tot, numbers.Number):
        prog_tot = cfg.default_settings.progress_total
    pn = 0
    err = await data_svs.update_instruments()
    if callable(prog_func):
        pn = pn+(prog_tot*0.01)
        prog_func(pn)
    if err:
        title, detail = err
        result["error"] = (1, err)
        if callable(prog_func):
            prog_func(prog_tot)
        return result

    instruments = await strg.read_tse_csv('tse.instruments')
    selection = list(map((lambda x: instruments[x]), symbols))
    not_founds = set(symbols) - set(selection)
    if callable(prog_func):
        pn = pn+(prog_tot*0.01)
        prog_func(pn)
    if not_founds:
        title, detail = "Symbols not found", not_founds
        result.error = (2, err)
        if callable(prog_func):
            prog_func(prog_tot)
        return result

    merge_similar_symbols = settings['merge_similar_symbols']
    merges = {}
    extras_index = -1

    if merge_similar_symbols:
        syms = instruments.keys
        ins = [instruments[k] for k in syms]
        syms_with_roots =  list(filter((lambda x: x.SymbolOriginal), ins))
        roots = list(map((lambda x: x.SymbolOriginal), syms_with_roots))

        merges = list(map((lambda x: [x, []]), roots))

        for i in ins:
            orig = i.SymbolOriginal
            sym = i.Symbol
            code = i.InsCode
            renamed_or_root = orig or sym
            if not renamed_or_root in merges:
                return
            pattern = re.compile(cfg.SYMBOL_RENAME_STRING + r'\d+')
            order = int(pattern.match(sym)[1]) if orig else 1
            merges[renamed_or_root].append({sym, code, order})
        
        # TODO: complete

    update_result = await update_prices(selection, settings.cache, (prog_func, pn, prog_tot*0.78))
    succs, fails, error = update_result
    pn = update_result
    if error:
        title, detail = error
        result.error = (1, title, detail)
        if callable(prog_func):
            prog_func(prog_tot)
        return result
    if fails:
        syms = [(i.ins_code, i.SymbolOriginal) for i in selection]
        result.error = (3, 'Incomplete Price Update',
                        fails.map(lambda k: syms[k]),
                        succs.map(lambda k: syms[k])
                        )
        for v, i, a in selection:
            if fails.include(v.incode):
                a[i] = None
            else:
                a[i] = 0
    if merge_similar_symbols:
        selection = selection[, extras_index]

    columns = settings.columns
    # TODO: complete

    adjustPrices, daysWithoutTrade, startDate, csv = settings
    shares = await strg.read_tse_csv('tse.shares')
    pi = prog_tot * 0.20 / selection.length
    stored_prices_merged = {}

    if merge_similar_symbols:
        for merge in merges:
            codes = [i.code for i in merge.values()]
            [latest] = codes
            stored_prices_merged[latest] = codes
            # TODO: complete

    if csv:
        csv_headers, csv_delimiter = settings
        headers = ''
        if csv_headers:
            headers = columns.map(lambda i: i.header).join()+'\n'
        else:
            headers = ''
        result.data = selection
        # TODO: complete
    else:
        text_cols = set(['CompanyCode', 'LatinName', 'Symbol', 'Name'])

        result.data = selection
        # TODO: complete

    if prog_func and pn != prog_tot:
        pn = ptot
        pf(pn)

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
