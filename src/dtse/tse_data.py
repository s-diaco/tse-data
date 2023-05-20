"""
Manage TSE Data
"""


import numbers
import re

from pandas import DataFrame

from . import config as cfg
from . import data_services as data_svs
from .data_structs import TSEColumn, TSEInstrument
from .price_update_helper import PricesUpdateHelper
from .progress_bar import ProgressBar
from .setup_logger import logger as tse_logger
from .storage import Storage
from .tse_parser import parse_instruments


# todo: complete
async def _update_prices(selection, should_cache, progressbar: ProgressBar):
    """
    update prices

    :selection: list, instruments to update
    :should_cache: bool, should cache prices in csv files
    :percents: dict, data needed for progress bar
    """
    strg = Storage()
    last_devens = strg.read_tse_csv_blc("tse.inscode_lastdeven")
    ins_codes = []
    if not last_devens.empty:
        ins_codes = last_devens[0]
    result = {"succs": [], "fails": []}
    """
    prog_fin = progressbar.pn + progressbar.ptot
    """
    try:
        last_possible_deven = await data_svs.get_last_possible_deven()
    except Exception as ex:
        result["error"] = ex
        """
        if callable(progressbar.progress_func):
            progressbar.progressbar.prog_func(prog_fin)"""
        return result

    to_update = []
    first_possible_deven = cfg.default_settings["start_date"]
    for instrument in selection.to_dict("records"):
        ins_code = instrument["InsCode"]
        market = instrument["YMarNSC"] != "NO"
        if ins_code not in ins_codes:
            # doesn't have data
            to_update.append([ins_code, first_possible_deven, market])
        else:
            # has data
            # TODO: last_devens[ins_code] doesn't work. it's a dataframe
            last_deven = last_devens[ins_code]
            # if not last_deven: expired symbol
            if last_deven:
                if data_svs.should_update(last_deven, last_possible_deven):
                    # has data but outdated
                    to_update.append([ins_code, last_deven, market])
    """
    if callable(progressbar.progress_func):
        progressbar.progressbar.prog_func(progressbar.pn + progressbar.ptot * (0.01))
    """
    sel_ins = selection["InsCode"]
    price_manager = PricesUpdateHelper()
    # TODO: there is no stored_prices implemented
    stored_ins = price_manager.stored_prices
    # TODO: WTF is this?
    if (not stored_ins) or (not sel_ins):
        price_manager.stored_prices = await strg.get_items(sel_ins)
    """
    if callable(progressbar.progress_func):
        progressbar.progressbar.prog_func(progressbar.pn + progressbar.ptot * (0.01))
    """
    if to_update:
        """
        progress_tuple = (
            progressbar.progress_func,
            progressbar.pn,
            progressbar.ptot - progressbar.ptot * (0.02),
        )"""
        manager_result = await price_manager.start(
            update_needed=to_update, progressbar=progressbar, should_cache=should_cache
        )
        succs = manager_result["succs"]
        fails = manager_result["fails"]
        """
        progressbar.pn = progressbar.prog_n
        """
        # TODO: price update helper Should update inscode_lastdeven file
        # with new cached instruments in _on_result or do not read it from this file
        if succs and should_cache:
            await strg.write_tse_csv(f_name="tse.inscode_lastdeven", data=last_devens)
        result = (succs, fails)
    """
    if callable(progressbar.progress_func) and progressbar.pn != prog_fin:
        progressbar.progressbar.prog_func(prog_fin)
    result.pn = prog_fin"""

    return result


async def get_prices(symbols=None, conf=None):
    """
    get prices for symbols

    :symbols: list, symbols to get prices for
    :_settings: dict, settings to use

    :return: dict, prices for symbols
    """

    progressbar = ProgressBar()
    if not symbols:
        return
    settings = cfg.default_settings
    if conf:
        settings.update(conf)
    result = {"data": [], "error": None}
    """
    progressbar.prog_func = settings.get("on_progress")
    if not callable(progressbar.prog_func):
        progressbar.prog_func = None
    progressbar.prog_tot = settings.get("progress_tot")
    if not isinstance(progressbar.prog_tot, numbers.Number):
        progressbar.prog_tot = cfg.default_settings["progress_tot"]
    progressbar.prog_n = 0
    """
    await data_svs.update_instruments()
    """
    if callable(progressbar.prog_func):
        progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
        progressbar.prog_func(progressbar.prog_n)
    """
    instruments = await parse_instruments()

    selected_syms_df = instruments[instruments["Symbol"].isin(symbols)]
    if not len(selected_syms_df):
        raise ValueError(f"No instruments found for symbols: {symbols}.")
    not_founds = [sym for sym in symbols if sym not in instruments["Symbol"].values]
    if not_founds:
        tse_logger.warning(f"symbols not found: {not_founds}")
    """
    if callable(progressbar.prog_func):
        progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
        progressbar.prog_func(progressbar.prog_n)
        if callable(progressbar.prog_func):
            progressbar.prog_func(progressbar.prog_tot)
    """
    merge_similar_symbols = settings["merge_similar_symbols"]
    if merge_similar_symbols:
        # TODO: doesn't work
        _merge_similars(syms=instruments, selected_syms=selected_syms_df)

    update_result = await _update_prices(
        selected_syms_df,
        settings["cache"],
        progressbar,
    )
    succs, fails = update_result
    """
    progressbar.prog_n = update_result
    if error:
        err = (error.title, error.detail)
        result["error"] = (1, err)
        if callable(progressbar.prog_func):
            progressbar.prog_func(progressbar.prog_tot)
        return result

    if fails:
        syms = [(i.ins_code, i.SymbolOriginal) for i in selected_syms_df]
        title = "Incomplete Price Update"
        succs = list(map((lambda x: syms[x]), succs))
        fails = list(map((lambda x: syms[x]), fails))
        err = (title, succs, fails)
        result["error"] = (3, err)
        for v, i, a in selected_syms_df:
            if fails.include(v.ins_code):
                a[i] = None
            else:
                a[i] = 0
    if merge_similar_symbols:
        selected_syms_df = selected_syms_df[:extras_index]

    columns = settings["columns"]

    def col(col_name):
        row = col_name
        column = TSEColumn(row)
        final_header = column.header or column.name
        return {column, final_header}

    columns = list(map(col, settings["columns"]))
    """

    columns = settings["columns"]
    adjust_prices = settings["adjust_prices"]
    days_without_trade = settings["days_without_trade"]
    start_date = settings["start_date"]
    csv = settings["csv"]
    strg = Storage()
    shares = strg.read_tse_csv_blc("tse.shares")
    """
    pi = progressbar.prog_tot * 0.20 / selected_syms_df.length
    """
    stored_prices_merged = {}

    prices_manager = PricesUpdateHelper()
    if merge_similar_symbols:
        for merge in merges:
            codes = [i.code for i in merge.values()]
            stored_prices_merged[codes] = list(
                map((lambda x: prices_manager.stored_prices[x]), codes)
            ).reverse()

    if csv:
        csv_headers = settings["csv_headers"]
        csv_delimiter = settings["csv_delimiter"]
        headers = ""
        if csv_headers:
            headers = list(map((lambda i: i.header), columns)).join() + "\n"

        def map_selection(instrument):
            if not instrument:
                return
            res = headers
            prices = _get_instrument_prices(instrument)
            if not prices:
                return res
            if prices == cfg.MERGED_SYMBOL_CONTENT:
                return prices

            res += list(
                map(
                    (
                        lambda price: list(
                            map(
                                (
                                    lambda i: data_svs.get_cell(
                                        i.name, instrument, price
                                    ).join(csv_delimiter)
                                ),
                                columns,
                            )
                        )
                    ),
                    prices,
                )
            ).join("\n")
            if callable(progressbar.prog_func):
                pn = pn + pi
                progressbar.prog_func(pn)
            return res

        result["data"] = list(map(map_selection, selected_syms_df))
    else:
        text_cols = set(["CompanyCode", "LatinName", "Symbol", "Name"])

        def map_selection(instrument):
            if not instrument:
                return
            res = list(map((lambda x: [x.header, []]), columns))
            prices = _get_instrument_prices(instrument)
            if not prices:
                return res
            if prices == cfg.MERGED_SYMBOL_CONTENT:
                return prices
            for price in prices:
                for header, name in columns:
                    cell = data_svs.get_cell(name, instrument, price)
                    res[header].push(cell if (name in text_cols) else float(cell))

        result["data"] = list(map(map_selection, selected_syms_df))
    """
    if progressbar.prog_func and progressbar.prog_n != progressbar.prog_tot:
        progressbar.prog_n = progressbar.prog_tot
        progressbar.prog_func(progressbar.prog_n)
    """

    return result


async def get_instruments(struct=True, arr=True, structKey="InsCode"):
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
        struct_key = "InsCode"

    last_update = strg.get_item("tse.lastInstrumentUpdate")
    err = await data_svs.update_instruments()
    if err and not last_update:
        raise err
    return await strg.read_tse_csv("tse.instruments")


def _get_instrument_prices(
    instrument,
    start_date,
    shares,
    merge_similar_symbols=True,
    merges=None,
    settings=None,
    stored_prices=None,
    stored_prices_merged=None,
    adjust_prices=True,
    days_without_trade=0,
):
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
        prices = (stored_prices[ins_code], stored_prices_merged[ins_code])[is_root]
        ins_codes = (set(ins_code), merges[sym].map(lambda i: i.code))[is_root]

    if not prices:
        return

    if adjust_prices == 1 or adjust_prices == 2:
        prices = data_svs.adjust(adjust_prices, prices, shares, ins_codes)

    if not days_without_trade:
        prices = prices.filter(lambda i: float(i.ZTotTran) > 0)

    prices = prices.filter(lambda i: float(i.DEven) > float(start_date))

    return prices


def _merge_similars(syms: DataFrame, selected_syms: DataFrame) -> DataFrame:
    """
    merge similar symbols with the same 'Symbol' name

    :syms: DataFrame, all available symbols from api response
    :selected_syms: DataFrame, symbols that their prices are being requested

    :return: DataFrame, merged data from the selected symbols and their older data
    """
    # TODO: implement
    """syms = instruments.keys
    roots = instruments["SymbolOriginal"][instruments["SymbolOriginal"].notna()]
    merges = list(map((lambda x: [x, []]), roots))

    for i in instruments.itertuples():
        orig = i.SymbolOriginal
        sym = i.Symbol
        code = i.InsCode
        renamed_or_root = orig or sym
        if not renamed_or_root in merges:
            return
        pattern = re.compile(cfg.SYMBOL_RENAME_STRING + r"\d+")
        order = int(pattern.match(sym)[1]) if orig else 1
        merges[renamed_or_root].append({sym, code, order})
        merges.sort(key=(lambda x: x.order))
        extras = selected_syms_df - merges
        extras_index = len(selected_syms_df)
        selected_syms_df.extend(extras)"""
    return selected_syms


def _procc_similar_syms(instrums_df: DataFrame) -> DataFrame:
    """
    Process similar symbols an add "SymbolOriginal" column to DataFrame

    :param instrums_df: pd.DataFrame, instruments dataframe

    :return: pd.DataFrame, processed dataframe
    """
    """sym_groups = [x for x in instrums_df.groupby("Symbol")]
    dups = [v for v in sym_groups if len(v[1]) > 1]
    for dup in dups:
        dup_sorted = dup[1].sort_values(by="DEven", ascending=False)
        for i in range(1, len(dup_sorted)):
            instrums_df.loc[
                dup_sorted.iloc[i].name, "SymbolOriginal"
            ] = instrums_df.loc[dup_sorted.iloc[i].name, "Symbol"]
            postfix = cfg.SYMBOL_RENAME_STRING + str(i)
            instrums_df.loc[dup_sorted.iloc[i].name, "Symbol"] += postfix"""
    return instrums_df


"""
 3833483672193514,IRO7SMGP0001,SMGP1, Simorgh Co.,SMGP,سيمرغ-ق2,سيمرغ,IRO7SMGP0003,20180721,2,سيمرغ,A,P1,NO,7,01 ,0141,309,سيمرغ

28450080638096732,IRO1SMRG0001,SMRG1,Seamorgh Co.,SMRG,سيمرغ,سيمرغ,IRO1SMRG0007,20221008,1,سيمرغ,A,N1,NO,3,01 ,0122,300
"""
