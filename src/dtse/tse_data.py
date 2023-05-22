"""
Manage TSE Data
"""


import numbers
import re

from pandas import DataFrame

from dtse.cache_manager import TSECachedData

from . import config as cfg
from . import data_services as data_svs
from .data_structs import TSEColumn, TSEInstrument
from .price_update_helper import PricesUpdateHelper
from .progress_bar import ProgressBar
from .setup_logger import logger as tse_logger
from .storage import Storage
from .tse_parser import parse_instruments


# todo: complete
async def _filter_expired_prices(
    selection, settings, progressbar: ProgressBar, cache_manager: TSECachedData
) -> DataFrame:
    """
    check if there is no updated cached data for symbol and return a dataframe
    containing that symbols (InsCode, DEven, YMarNSC)

    :selection: list, instruments to check
    :settings: dict, should_cache & merge_similar_symbols & ...
    :percents: dict, progress bar data

    :return: DataFrame, symbols to update
    """

    cache_mngr = cache_manager
    # TODO: do not convert to string (remove astype(str))
    # TODO: stored_prices shouldn't be in PricesUpdateHelper Class
    to_update = selection[["InsCode", "DEven", "YMarNSC"]]
    # TODO: Ensure it returns the last value not the first one.
    # is it good to store last_devens in a file?
    last_devens = DataFrame(
        {
            prc_df.iloc[-1]["InsCode"]: prc_df.iloc[-1]["DEven"]
            for prc_df in cache_mngr.stored_prices.values()
        }.items(),
        columns=["InsCode", "cached_DEven"],
    ).astype("int64")
    first_possible_deven = settings["start_date"]
    if not last_devens.empty:
        last_possible_deven = await data_svs.get_last_possible_deven()
        # merge selection with last_devens (from cached data) to find out witch syms need an update
        sel_merged = selection.merge(last_devens, how="left", on="InsCode")
        # symbol doesn't have data
        sel_merged.cached_DEven = sel_merged.cached_DEven.fillna(
            first_possible_deven
        ).astype("int64")
        # symbol has data but outdated
        sel_merged["need_upd"] = sel_merged["cached_DEven"].map(
            lambda deven: data_svs.should_update(str(deven), last_possible_deven)
        )
        to_update = sel_merged[sel_merged["need_upd"]][
            ["InsCode", "cached_DEven", "YMarNSC"]
        ].rename({"cached_DEven": "DEven"})
    """
    prog_fin = progressbar.pn + progressbar.ptot
    """
    """
    if callable(progressbar.progress_func):
        progressbar.progressbar.prog_func(progressbar.pn + progressbar.ptot * (0.01))
    """
    """
    progress_tuple = (
        progressbar.progress_func,
        progressbar.pn,
        progressbar.ptot - progressbar.ptot * (0.02),
    )"""
    """
    progressbar.pn = progressbar.prog_n
    """
    # TODO: price update helper Should update inscode_lastdeven file
    # with new cached instruments in _on_result or do not read it from this file
    """
    if callable(progressbar.progress_func) and progressbar.pn != prog_fin:
        progressbar.progressbar.prog_func(prog_fin)
    result.pn = prog_fin"""

    return to_update


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
    if selected_syms.empty:
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
    cache_manager = TSECachedData(selected_syms)
    to_update = await _filter_expired_prices(
        selected_syms, settings, progressbar, cache_manager
    )
    price_manager = PricesUpdateHelper(cache_manager=cache_manager)
    manager_result = await price_manager.start(
        update_needed=to_update, progressbar=progressbar, settings=settings
    )
    result = manager_result
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

    # TODO: does it return the full list before 8:30 a.m.?
    await data_svs.update_instruments()
    instruments = await parse_instruments()
    selected_syms = instruments[instruments["Symbol"].isin(syms)]
    return selected_syms
