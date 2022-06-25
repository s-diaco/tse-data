import config as cfg
import numbers


async def get_prices(symbols=[], _settings={}):
    if not symbols.length:
        return
    settings = {cfg.default_settings, _settings}
    result = {"data": [], "error": None}
    { onprogress: pf, progressTotal: ptot } = settings
    if not callable(pf):
        pf = None
    if not isinstance(ptot, numbers.Number):
        ptot = cfg.default_settings.progress_total
    pn = 0
    err = await update_instruments()
    if callable(pf):
        pf(pn=pn+(ptot*0.01))
    if err:
        {title, detail} = err
    result.error = { code: 1, title, detail }
    if callable(pf):
        pf(ptot)
        return result
    
    instruments = parse_instruments(true, undefined, 'Symbol')
    selection = symbols.map(lambda i: instruments[i])
    not_founds = symbols.filter(v,i : !selection[i])
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
