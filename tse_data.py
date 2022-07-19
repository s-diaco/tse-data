import config as cfg
import numbers
import data_services as data_svs
from storage import Storage as strg
from price_update_helper import PricesUpdateHelper


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
                to_update = None # expired symbol
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
    if symbols == None:
        symbols = []
    if not symbols.length:
        return
    settings = cfg.default_settings, _settings
    result = {"data": [], "error": None}
    prog_func, prog_tot = settings
    if not callable(prog_func):
        prog_func = None
    if not isinstance(prog_tot, numbers.Number):
        prog_tot = cfg.default_settings.progress_total
    pn = 0
    err = await data_svs.update_instruments()
    if callable(prog_func):
        pn=pn+(prog_tot*0.01)
        prog_func(pn)
    if err:
        title, detail = err
        result.error = (1, title, detail)
        if callable(prog_func):
            prog_func(prog_tot)
        return result
    
    instruments = await strg.read_tse_csv('tse.instruments')
    selection = symbols.map(lambda i: instruments[i])
    not_founds = symbols.filter(v,i : !selection[i])
    not_founds = [instruments[x] for x in symbols if instruments[x] not in selection]
    # TODO: remove this line
    # not_founds = set(symbols) - set(selection)
    if not_founds:
        result.error = (2, "Symbols not found", not_founds)
        if callable(prog_func):
            prog_func(prog_tot)
        return result
    if callable(prog_func):
        pn=pn+(prog_tot*0.01)
        prog_func(pn)
    result.data = selection
    if callable(prog_func):
        prog_func(prog_tot)
    return result
    if callable(pf):
        pf(pn= pn+(ptot*0.01))
    if not_founds.length:
        result.error = { code: 2, title: 'Incorrect Symbol', symbols: notFounds };
        if callable(pf):
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
        if callable(pf):
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

            if callable(pf):
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
            if callable(pf):
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
