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
async def _update_prices(selection, settings, progressbar: ProgressBar):
    """
    update prices if there is no cached data for them or if cached data is outdated

    :selection: list, instruments to update
    :settings: dict, should_cache & merge_similar_symbols & ...
    :percents: dict, progress bar data
    """

    result = {"succs": [], "fails": []}
    price_manager = PricesUpdateHelper()
    price_manager.update_stored_prices(selection["InsCode"].astype(str).values)
    # TODO: Ensure it returns the last value not the first one.
    # is it good to store last_devens in a file?
    last_devens = DataFrame(
        {
            prc_df.astype(str).iloc[-1]["InsCode"]: prc_df.astype(str).iloc[-1]["DEven"]
            for prc_df in price_manager.stored_prices.values()
        }.items()
    )
    if not last_devens.empty:
        last_devens.columns = ["InsCodes", "DEven"]
    # TODO: merge last_devens & selection to find out witch syms need an update
    ins_codes = []
    if last_devens:
        ins_codes = list(last_devens.keys())
    """
    prog_fin = progressbar.pn + progressbar.ptot
    """
    last_possible_deven = await data_svs.get_last_possible_deven()
    to_update = []
    first_possible_deven = settings["start_date"]
    syms_not_cached = selection[~selection["InsCode"].isin(ins_codes)][
        ["InsCode", "YMarNSC"]
    ]
    syms_not_cached["DEven"] = first_possible_deven
    syms_cached = selection[selection["InsCode"].isin(ins_codes)][
        ["InsCode", "DEven", "YMarNSC"]
    ]
    syms_cached["should_update"] = syms_cached["DEven"].map(
        lambda x: data_svs.should_update(x, last_possible_deven)
    )
    syms_cached = syms_cached[syms_cached["should_update"]][
        ["InsCode", "DEven", "YMarNSC"]
    ]
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
    stored_ins = price_manager.stored_prices
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
            update_needed=to_update, progressbar=progressbar, settings=settings
        )
        succs = manager_result["succs"]
        fails = manager_result["fails"]
        """
        progressbar.pn = progressbar.prog_n
        """
        # TODO: price update helper Should update inscode_lastdeven file
        # with new cached instruments in _on_result or do not read it from this file
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
    result = {"succs": None, "fails": None}
    """
    progressbar.prog_func = settings.get("on_progress")
    if not callable(progressbar.prog_func):
        progressbar.prog_func = None
    progressbar.prog_tot = settings.get("progress_tot")
    if not isinstance(progressbar.prog_tot, numbers.Number):
        progressbar.prog_tot = cfg.default_settings["progress_tot"]
    progressbar.prog_n = 0
    """
    """
    if callable(progressbar.prog_func):
        progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
        progressbar.prog_func(progressbar.prog_n)
    """

    # check if names in symbols are valid symbol names
    selected_syms = await get_valid_syms(symbols)
    if not len(selected_syms):
        raise ValueError(f"No instruments found for symbols: {symbols}.")
    not_founds = [sym for sym in symbols if sym not in selected_syms["Symbol"].values]
    if not_founds:
        tse_logger.warning(f"symbols not found: {not_founds}")
    """
    if callable(progressbar.prog_func):
        progressbar.prog_n = progressbar.prog_n + (progressbar.prog_tot * 0.01)
        progressbar.prog_func(progressbar.prog_n)
        if callable(progressbar.prog_func):
            progressbar.prog_func(progressbar.prog_tot)
    """

    update_result = await _update_prices(
        selected_syms,
        settings,
        progressbar,
    )
    result = update_result
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
    """
    pi = progressbar.prog_tot * 0.20 / selected_syms_df.length
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


async def get_valid_syms(syms: list[str]) -> DataFrame:
    """
    check if names in symbols are valid symbol names

    :syms: list[str], list of symbol names to be validated

    :return: DataFrame, codes and names of the valid symbols
    """

    await data_svs.update_instruments()
    instruments = await parse_instruments()
    selected_syms = instruments[instruments["Symbol"].isin(syms)]
    return selected_syms
