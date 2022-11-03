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
