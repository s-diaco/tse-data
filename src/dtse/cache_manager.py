"""
manage cached data
"""

from datetime import date
from pathlib import Path

import pandas as pd
from rich.progress import track

# fmt: off
from sqlalchemy import (BigInteger, Column, Engine, Integer, MetaData, Table,
                        create_engine, inspect, select)
# fmt: on

from sqlalchemy.dialects.sqlite import insert

from dtse.logger import logger as tse_logger
from dtse.tse_utils import to_jalali_date


class TSECache:
    """
    Manage TSE data cached in memory and/or Database
    """

    def __init__(self, settings) -> None:
        """
        Initialize TSECache class

        :settings: dict, Configuration from config file and user input.
        """

        self.settings = settings
        self._instruments: pd.DataFrame | None = None
        self._splits: pd.DataFrame | None = None
        self._prices: pd.DataFrame | None = None
        self._prices_merged: pd.DataFrame | None = None
        self._last_devens: pd.DataFrame | None = None
        self._engine: Engine | None = None
        self._last_possible_deven: str = ""
        self._last_instrument_update: str = ""
        self.cache_to_db = (
            self.settings["cache_to_db"] if "cache_to_db" in self.settings else True
        )
        self._init_cache_dir()
        self._read_metadata()
        self._read_instrums()
        self._read_splits()

    def _read_metadata(self):
        # get last_instrument_update and last_possible_deven from database if available.

        table_name = "metadata"
        metadata = self._read_table(table_name=table_name, index_col=["index"])
        if metadata is not None and (not metadata.empty):
            if "last_possible_deven" in metadata.columns:
                self._last_possible_deven = metadata.loc[:, "last_possible_deven"].iloc[
                    -1
                ]
            if "last_inst_upd" in metadata.columns:
                self._last_instrument_update = metadata.loc[:, "last_inst_upd"].iloc[-1]

    def _init_cache_dir(self):
        # create cache dir and database file if needed.

        if "tse_dir" in self.settings:
            self._data_dir = Path(self.settings["tse_dir"])
        else:
            data_dir = Path(self.settings["TSE_CACHE_DIR"])
            home = Path.home()
            self._data_dir = home / data_dir
        if self.cache_to_db:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            tse_logger.info("data dir: %s", self._data_dir)
        if (
            self.cache_to_db
            or (self._data_dir / self.settings["DB_FILE_NAME"]).is_file()
        ):
            self._engine = create_engine(
                "sqlite:///" + str(self._data_dir / self.settings["DB_FILE_NAME"])
            )

            last_deven_sql = Table(
                "last_devens",
                MetaData(),
                Column("InsCode", BigInteger, primary_key=True),
                Column("LastDEven", Integer),
            )
            last_deven_sql.create(checkfirst=True, bind=self._engine)

    @property
    def cache_dir(self):
        """
        :return: cache dir
        """
        return self._data_dir

    @cache_dir.setter
    def cache_dir(self, value: str):
        self._data_dir = Path(value)

    @property
    def instruments(self):
        """
        All instruments and their details.
        """
        return self._instruments

    @instruments.setter
    def instruments(self, value: pd.DataFrame):
        if not value.empty:
            self._instruments = value
            self._set_last_inst_upd()

    def _set_last_inst_upd(self):
        # update last_instrument_update
        today = date.today().strftime("%Y%m%d")
        if self._last_instrument_update != today:
            self.last_instrument_update = today

    @property
    def splits(self):
        """Data about stock splits"""
        return self._splits

    @splits.setter
    def splits(self, value: pd.DataFrame):
        if not value.empty:
            self._splits = value

    @property
    def prices(self):
        """Price data indexed by code and date. Can be None or pd.DataFrame"""
        if self._prices is not None:
            if not self._prices.empty:
                return self._prices.sort_index()
        return None

    @property
    def prices_merged(self):
        """Price data indexed by symbol an date. Can be None or pd.DataFrame"""
        if self._prices_merged is not None:
            return self._prices_merged.sort_index()
        return None

    def add_to_prices(self, dfs: list[pd.DataFrame]) -> bool:
        """
        Adds a list of dataframes to "prices" property.

        :dframes: list[pd.DataFrame], list of dataframes to add to prices property.

        :return: bool, True if value added to prices, else False.
        """

        dfs = [data for data in dfs if not data.empty]

        if dfs:
            new_prices = pd.concat(dfs)
            if self._prices is None:
                self._prices = new_prices
            else:
                self._prices = pd.concat([new_prices, self._prices])
            if self.cache_to_db:
                self._prices_to_db(new_prcs=new_prices)
            return True
        else:
            return False

    @property
    def last_instrument_update(self):
        """last date of updating list of instrument and splits"""
        return self._last_instrument_update

    @last_instrument_update.setter
    def last_instrument_update(self, value: str):
        if value:
            self._last_instrument_update = value

    def read_prices(self, codes: list[int]):
        """
        updates a dicts of prices for ins_codes in self.prices
        """

        if self._engine:
            prices = self._read_prc(codes=codes)
            if (prices is not None) and (not prices.empty):
                self._prices = prices.sort_index()

    def _read_prc(self, codes: list[int]) -> pd.DataFrame | None:
        """
        Read selected instruments files from the cache dir and return a pd.DataFrame.

        :codes: list[int], list of codes to read from.

        :return: pd.DataFrame
        """

        table_name = "daily_prices"
        if inspect(self._engine).has_table(table_name):
            prcs_table = Table(table_name, MetaData(), autoload_with=self._engine)
            qry = select(prcs_table).where(
                prcs_table.c.InsCode.in_(codes) & prcs_table.c.DEven
                >= int(self.settings["start_date"])
            )
            with self._engine.connect() as conn:
                data = pd.read_sql_query(qry, conn, index_col=["InsCode", "DEven"])
            return data
        else:
            return None

    def _read_instrums(self):
        # read list of all cached instruments from db and update "instruments"

        ins_tbl_name = "instruments"
        instrums = self._read_table(table_name=ins_tbl_name, index_col=["InsCode"])
        if (instrums is not None) and (not instrums.empty):
            self._instruments = instrums
        lds_tbl_name = "last_devens"
        last_devens = self._read_table(table_name=lds_tbl_name, index_col=["InsCode"])
        if (last_devens is not None) and (not last_devens.empty):
            self._last_devens = last_devens

    def _read_splits(self):
        # read stock splits from database and update splits property.

        table_name = "splits"
        splits = self._read_table(table_name=table_name, index_col=["InsCode", "DEven"])
        if (splits is not None) and (not splits.empty):
            self._splits = splits

    @property
    def last_possible_deven(self):
        """
        last update date
        """
        return self._last_possible_deven

    @last_possible_deven.setter
    def last_possible_deven(self, value: str):
        if value:
            self._last_possible_deven = value

    def _upd_metadata(self):
        # update "metadata" table in db

        if self.cache_to_db:
            t_name = "metadata"
            metadata = pd.DataFrame.from_dict(
                {
                    "last_possible_deven": [self._last_possible_deven],
                    "last_inst_upd": [self._last_instrument_update],
                }
            )
            with self._engine.connect() as conn:
                metadata.to_sql(t_name, conn, if_exists="replace")

    def _read_table(self, table_name: str, index_col: list[str]) -> pd.DataFrame | None:
        # read a table from db and return its data

        if self._engine is not None and inspect(self._engine).has_table(table_name):
            with self._engine.connect() as conn:
                data = pd.read_sql_table(
                    table_name=table_name, con=conn, index_col=index_col
                )
            return data
        else:
            return

    def prices_by_symbol(self, symbols: list[str], cols: list[str]) -> dict:
        """
        get cached prices by list of symbols.

        :symbols: list[str], symbols to get prices for
        :settings: dict, app config from the config file or user input

        :return: dict, dict with symbols as keys and prices (DataFrame) as values
        """

        if self._prices is None or self._instruments is None:
            raise AttributeError("Some required data is missing in cache.")
        if not cols or "date_jalali" in cols:
            self._prices.loc[:, "date_jalali"] = self._prices.index.get_level_values(
                "DEven"
            ).map(to_jalali_date)

        symbol_dict = {
            symbol: list(
                self.instruments[self.instruments["Symbol"].isin([symbol])].index
            )
            for symbol in symbols
        }

        prices_merged_lst = []
        for sym, sym_codes in symbol_dict.items():
            prices_merged_lst.append(
                pd.concat(
                    {
                        sym: self.adjust(
                            self.settings["adjust_prices"], sym_codes
                        ).reset_index(level=[0])
                    },
                    names=["Symbol"],
                )
            )
        self._prices_merged = pd.concat(prices_merged_lst).sort_index()
        if not self.settings["days_without_trade"]:
            self._prices_merged = self._prices_merged[
                self._prices_merged["ZTotTran"] > 0
            ]
        if cols:
            self._prices_merged = self._prices_merged[cols]

        grouper = "Symbol"
        prices = dict(tuple(self._prices_merged.groupby(grouper)))
        return prices

    def adjust(self, cond: int, ins_codes: list[int]) -> pd.DataFrame:
        """
        Adjust closing prices according to the condition
        0: make no adjustments
        1: adjust according to dividends and splits
        2: adjust according to splits
        3: adjust according to cash dividends

        :cond: int, price adjust type. can be 0 (no adjustment), 1, 2 or 3
        :ins_codes: list, instrument codes

        :return: pd.DataFrame, adjusted closing prices

        :raises: ValueError, if there is no price data in cache
        """

        if self.prices is not None:
            prices = self.prices[
                self.prices.index.isin(ins_codes, level="InsCode")
            ].sort_index(level=1)
            cp_len = len(prices)
            if cp_len < 2:
                return prices
            price_cols = list(prices.columns)
            if len(ins_codes) > 1:
                # find stock moves between markets
                # and adjust for nominal price in the new market, if necessary
                prices["ShiftedClose"] = prices["PClosing"].shift(1)
                nom_idxs = prices.groupby("InsCode").head(1).index[1:]
                default_prs = self.settings["NOMINAL_PRICES"]
                prices["nom_pr"] = (
                    prices["PClosing"].isin(default_prs)
                    & prices.index.isin(nom_idxs)
                    & (prices["ShiftedClose"] != prices["PriceYesterday"])
                )
                while not prices[prices["nom_pr"]].empty:
                    # index of first and last row with nominal price
                    first_idx = prices[prices["nom_pr"]].index[0]
                    cond_1 = prices.index.isin([first_idx[0]], level="InsCode")
                    cond_2 = prices["QTotTran5J"] != 0
                    last_idx = prices.loc[cond_1 & cond_2].index[0]

                    # the price to replace nominal price
                    rep_pr = prices.loc[first_idx, "ShiftedClose"]

                    prices.loc[first_idx:last_idx, "PClosing"].iloc[:-1] = rep_pr
                    prices.loc[first_idx:last_idx, "PriceYesterday"] = rep_pr
                    prices.loc[first_idx:last_idx, "nom_pr"].iloc[0] = False
            if not cond:
                return prices
            if cond in [1, 3]:
                prices["ShiftedYDay"] = prices["PriceYesterday"].shift(-1)
                prices["AdjMultiplr"] = (
                    prices["ShiftedYDay"] / prices["PClosing"]
                ).fillna(1)
            if cond in [2, 3]:
                if (self._splits is not None) and (not self._splits.empty):
                    filtered_splits = self._splits.loc[ins_codes].eval(
                        expr="SplitMultiplr = NumberOfShareOld / NumberOfShareNew"
                    )
                    prices = prices.join(filtered_splits[["SplitMultiplr"]]).fillna(1)
                else:
                    prices["SplitMultiplr"] = 1
            if cond == 1:
                prices["AdjMultiplrCumProd"] = (
                    prices.AdjMultiplr.iloc[::-1].cumprod().iloc[::-1]
                )
                prices["AdjPClosing"] = (
                    prices.AdjMultiplrCumProd * prices.PClosing
                ).map(round)
            elif cond == 2:
                prices["SplitMultiplrCumProd"] = (
                    prices.SplitMultiplr.iloc[::-1]
                    .cumprod()
                    .iloc[::-1]
                    .shift(-1, fill_value=1)
                )
                prices["AdjPClosing"] = (
                    prices.SplitMultiplrCumProd * prices.PClosing
                ).map(round)
            elif cond == 3:
                prices["DividMultiplr"] = prices["AdjMultiplr"]
                prices.loc[
                    ~prices["SplitMultiplr"].shift(-1).isin([1]),
                    "DividMultiplr",
                ] = 1
                prices["DividMultiplrCumProd"] = (
                    prices.DividMultiplr.iloc[::-1].cumprod().iloc[::-1]
                )
                prices["AdjPClosing"] = (
                    prices.DividMultiplrCumProd * prices.PClosing
                ).map(round)
            price_cols.append("AdjPClosing")
            return prices[price_cols]
        else:
            raise ValueError("No price data available.")

    def write_prc_csv(
        self,
        prices: dict,
    ) -> None:
        """
        Write price data to a csv file or a list of csv files.

        :prices: dict, dict of price data for symbols
        """

        if self.prices is not None:
            tse_logger.info("Writing data to csv files.")
            for sym, sym_prcs in track(prices.items(), description="Writing data"):
                prc_data = sym_prcs
                f_name = sym
                self._write_tse_csv(
                    f_name=f_name,
                    data=prc_data,
                    subdir=self.settings["PRICES_DIR"],
                )
            tse_logger.info("writing to csv finished")

    def _write_tse_csv(self, f_name: str, data: pd.DataFrame, **kwargs):
        """
        Write data to csv file.

        :f_name: str, File name
        :data: pd.DataFrame, Stock price data
        """

        if "subdir" in kwargs:
            tse_dir = self._data_dir / str(kwargs.get("subdir"))
        else:
            tse_dir = self._data_dir
        if not tse_dir.is_dir():
            tse_dir.mkdir(parents=True, exist_ok=True)
        if len(data) == 0:
            return
        file_path = tse_dir / f"{f_name}.csv"
        data.to_csv(file_path, encoding="utf-8")

    def _prices_to_db(self, new_prcs):
        # write cached price data to database.

        def sqlite_upsert(table, conn, keys, data_iter):
            """
            update columns on primary key conflict
            """
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_stmt = insert(table.table).values(data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=table.index,
                set_={
                    c.name: c for c in insert_stmt.excluded if not c.name in table.index
                },
            )
            result = conn.execute(upsert_stmt)
            return result.rowcount

        if self._prices is not None and not self._prices.empty:
            t_name = "daily_prices"
            with self._engine.connect() as conn:
                new_prcs.to_sql(
                    name=t_name,
                    con=conn,
                    if_exists="append",
                    index=True,
                    method="multi",
                    chunksize=5000,
                    index_label=["InsCode", "DEven"],
                )

        if self._last_devens is not None:
            t_name = "last_devens"
            with self._engine.connect() as conn:
                self.last_devens.to_sql(
                    name=t_name,
                    con=conn,
                    if_exists="append",
                    method=sqlite_upsert,
                    chunksize=4000,
                    index_label=["InsCode"],
                )

    def instruments_to_db(self):
        """
        write cached instruments and splits data to database file
        """

        if self._instruments is not None and not self._instruments.empty:
            t_name = "instruments"
            with self._engine.connect() as conn:
                self._instruments.to_sql(
                    name=t_name,
                    con=conn,
                    if_exists="replace",
                    index=True,
                    method="multi",
                    index_label="InsCode",
                )
        if self._splits is not None and not self._splits.empty:
            t_name = "splits"
            with self._engine.connect() as conn:
                self._splits.to_sql(
                    name=t_name,
                    con=conn,
                    if_exists="replace",
                    index=True,
                    method="multi",
                    index_label=["InsCode", "DEven"],
                )
        self._upd_metadata()

    @property
    def last_devens(self):
        """
        get last cheched date for each price data.
        """
        return self._last_devens

    def update_last_devens(self, codes: list[int]):
        """
        update last_devens table

        :param codes: list[int], codes which their last_devens has to be added/updated.
        """
        if codes:
            today = date.today().strftime("%Y%m%d")
            if self._last_devens is None:
                self._last_devens = pd.DataFrame(
                    today, index=codes, columns=["LastDEven"]
                )
            else:
                self._last_devens = self._last_devens.reindex(
                    codes + list(self._last_devens.index)
                )
                self._last_devens.loc[codes, "LastDEven"] = today
