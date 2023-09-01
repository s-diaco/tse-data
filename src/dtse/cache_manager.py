"""
manage cached data
"""

from datetime import datetime
from pathlib import Path
from sqlite3 import Connection, connect

import pandas as pd
from sqlalchemy import BIGINT, INTEGER, Column, Integer, UniqueConstraint

from dtse.setup_logger import logger as tse_logger


class TSECache:
    """
    Manage TSE data cached in memory and/or files
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
        self.merged_instruments: pd.DataFrame | None = None
        self._cnx: Connection | None = None
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
        """
        Reads last_instrument_update from cache dir
        and reads read_last_possible_deven from database if available.
        """

        if self._cnx:
            table_name = "metadata"
            metadata = self._read_table(table=table_name, index_col=["index"])
            if metadata is not None and (not metadata.empty):
                if "last_possible_deven" in metadata.columns:
                    self._last_possible_deven = metadata.loc[
                        :, "last_possible_deven"
                    ].iloc[-1]
                # TODO: 'last_inst_upd' is never updated (always "0")
                if "last_inst_upd" in metadata.columns:
                    self._last_instrument_update = metadata.loc[
                        :, "last_inst_upd"
                    ].iloc[-1]

    def _init_cache_dir(self) -> None:
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
            self._cnx = connect(self._data_dir / self.settings["DB_FILE_NAME"])

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
            # TODO: is "_save_last_inst_upd" called too many times?
            self._save_last_inst_upd()

    def _save_last_inst_upd(self):
        today = datetime.now().strftime("%Y%m%d")
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
            self._save_last_inst_upd()

    @property
    def prices(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._prices is not None:
            if not self._prices.empty:
                return self._prices.sort_index()
        return None

    # TODO: delete?
    @property
    def prices_merged(self):
        """Price data. Can be None or pd.DataFrame"""
        if self._prices_merged is not None:
            return self._prices_merged.sort_index()
        return None

    def add_to_prices(self, dfs: list[pd.DataFrame]) -> bool:
        """
        Adds a list of dataframes to "prices" property.

        :dframes: list[pd.DataFrame], list of dataframes to add to prices property.

        :return: bool, True if value added to prices, else False.
        """

        # TODO: fix empty data frames price at first non trading days of each InsCode
        dfs = [data for data in dfs if not data.empty]

        if dfs:
            if self._prices is None:
                self._prices = pd.concat(dfs)
            else:
                tot_prices = [self._prices]
                tot_prices.extend(dfs)
                self._prices = pd.concat(tot_prices)
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

    def read_prices(self, selected_syms: pd.DataFrame):
        """
        updates a dicts of prices for ins_codes in
        self.prices and self.prices_merged
        """

        if self._cnx:
            prices = self._read_prc(codes=selected_syms.index.tolist())
            if not prices.empty:
                self._prices = prices.sort_index()

    def _read_prc(self, codes: list[str]) -> pd.DataFrame:
        """
        Reads selected instruments files from the cache dir and returns a dict.

        :codes: list[str], list of codes to read from.

        :return: pd.DataFrame
        """

        table = "daily_prices"
        # check if table exists in db
        check_query = (
            f"SELECT name FROM sqlite_schema WHERE type='table' AND name='{table}';"
        )
        find_table = pd.read_sql(sql=check_query, con=self._cnx)
        if find_table.empty:
            return find_table
        else:
            # TODO: is it a safe query?
            codes = ", ".join(str(code) for code in codes)
            query = f"SELECT * FROM {table} WHERE InsCode In ({codes});"
            data = pd.read_sql(sql=query, con=self._cnx, index_col=["InsCode", "DEven"])
            return data

    def _read_instrums(self):
        """
        reads list of all cached instruments
        and updates "instruments" and "merged_instruments" properties
        """

        if self._cnx:
            inst_table_name = "instruments"
            instrums = self._read_table(table=inst_table_name, index_col=["InsCode"])
            if (instrums is not None) and (not instrums.empty):
                self._instruments = instrums
                # TODO: are these columns needed?
                self._instruments["Duplicated"] = self._instruments[
                    "Symbol"
                ].duplicated(keep=False)
                self._instruments["IsRoot"] = ~self._instruments["Symbol"].duplicated(
                    keep="first"
                )
            ld_table_name = "last_devens"
            last_devens = self._read_table(table=ld_table_name, index_col=["InsCode"])
            if (last_devens is not None) and (not last_devens.empty):
                self._last_devens = last_devens

    def _read_splits(self):
        """
        reads stock splits from database and updates splits property
        """

        if self._cnx:
            table_name = "splits"
            splits = self._read_table(table=table_name, index_col=["InsCode", "DEven"])
            if splits is not None and (not splits.empty):
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
        if self.cache_to_db:
            t_name = "metadata"
            metadata = pd.DataFrame.from_dict(
                {
                    "last_possible_deven": [self._last_possible_deven],
                    "last_inst_upd": [self._last_instrument_update],
                }
            )
            metadata.to_sql(t_name, self._cnx, if_exists="replace")

    def _read_table(self, table: str, index_col: list[str]) -> pd.DataFrame | None:
        """
        reads a table from database and returns its data

        :table: str, name of the table

        :return: pd.DataFrame, data read from the table.
        returns 'None' if the table doesn't exist.
        """

        # check if table exists in db
        check_query = (
            f"SELECT name FROM sqlite_schema WHERE type='table' AND name='{table}';"
        )
        find_table = pd.read_sql(sql=check_query, con=self._cnx)
        if find_table.empty:
            return None
        else:
            query = f"SELECT * FROM {table}"
            data = pd.read_sql(sql=query, con=self._cnx, index_col=index_col)
            return data

    # TODO: this is not called anywhere
    def get_instrum_prcs(self, symbols: list[str], settings: dict) -> dict:
        """
        get cached instrument prices

        :symbols: list[str], symbols to get prices for
        :settings: dict, app config from the config file or user input

        :return: dict, prices for symbols
        """

        if self._prices is None or self._instruments is None:
            raise AttributeError("Some required data is missing in cache.")
        symbol_dict = {
            symbol: self.instruments[self.instruments["Symbol"].isin([symbol])]
            for symbol in symbols
            if symbol in self.instruments["Symbol"].unique()
        }

        # TODO: delete
        """
        merges = self._instruments[self._instruments["Duplicated"]]
        prices_merged = self._prices_merged

        prices = pd.DataFrame()
        ins_codes = []

        if not instrument["IsRoot"]:
            # Old and inactive instruments
            if settings["merge_similar_symbols"]:
                return pd.DataFrame()
            prices = self.prices[ins_code]
            ins_codes = [ins_code]
        else:
            # Active instrument with similar inactive symbols
            is_head = instrument["InsCode"] in merges.InsCode.values
            if is_head and settings["merge_similar_symbols"]:
                # TODO: why is this "if" needed?
                if ins_code in prices_merged:
                    prices = prices_merged[ins_code]
                ins_codes = merges[merges["Symbol"] == instrument["Symbol"]][
                    "InsCode"
                ].values
            else:
                # TODO: why is this "if" needed?
                if ins_code in self._prices:
                    prices = self._prices[ins_code]
                    ins_codes = [ins_code]
        """

        prices = {
            sym: self.adjust(settings["adjust_prices"], list(sym_codes.index))
            for sym, sym_codes in symbol_dict.items()
        }
        if not prices:
            return prices

        # TODO: delete
        # prices = self.adjust(settings["adjust_prices"], ins_codes)

        for sym in prices:
            prices[sym] = prices[sym][
                prices[sym].index.levels[1] > int(settings["start_date"])
            ]
            if not settings["days_without_trade"]:
                prices[sym] = prices[sym][prices[sym]["ZTotTran"] > 0]

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
            for sym, sym_prcs in prices.items:
                prc_data = sym_prcs
                f_name = sym
                self.write_tse_csv(
                    f_name=f_name,
                    data=prc_data,
                    subdir=self.settings["PRICES_DIR"],
                )

    def write_tse_csv(self, f_name: str, data: pd.DataFrame, **kwargs) -> None:
        """
        Write data to csv file.

        :f_name: str, File name
        :data: pd.DataFrame, Stock price data
        """

        # TODO: file names should change to symbol

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

    def prices_to_db(self):
        """
        write cached price data to database file
        """

        from sqlalchemy import create_engine
        from sqlalchemy.dialects.sqlite import insert
        from sqlalchemy.orm import declarative_base

        def sqlite_upsert(table, conn, keys, data_iter):
            """
            update columns on primary key conflict
            """
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_stmt = insert(table.table).values(data)
            # create update statement for excluded fields on conflict
            # update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["InsCode"],
                set_=dict(LastDEven=insert_stmt.excluded["LastDEven"]),
            )
            result = conn.execute(upsert_stmt)
            return result.rowcount

        if self._prices is not None and not self._prices.empty:
            t_name = "daily_prices"
            self.prices.to_sql(
                name=t_name,
                con=self._cnx,
                if_exists="append",
                index=True,
                method="multi",
                chunksize=5000,
                index_label=["InsCode", "DEven"],
            )

        engine = create_engine(
            "sqlite:///" + str(self._data_dir / self.settings["DB_FILE_NAME"])
        )
        Base = declarative_base()

        class Model(Base):
            __tablename__ = "model"

            InsCode = Column(BIGINT, primary_key=True, unique=True)
            # will create "model_num_key" UNIQUE CONSTRAINT, btree (num)
            # num = Column(Integer, unique=True)
            # same with UniqueConstraint:
            LastDEven = Column(INTEGER)
            __table_args__ = (UniqueConstraint("InsCode", name="model_InsCode_key"),)
            # for multiple columns:
            # __table_args__ = (UniqueConstraint("num", "LastDEven", name="two_columns"),)

        Model.metadata.reflect(engine)
        if self._last_devens is not None:
            t_name = "last_devens"
            self.last_devens.reset_index().to_sql(
                name=t_name,
                con=engine,
                if_exists="append",
                method=sqlite_upsert,
                index=False,
            )

    def instruments_to_db(self):
        """
        write cached instruments and splits data to database file
        """

        # TODO: should "if_exists" be "replace"?
        if self._instruments is not None and not self._instruments.empty:
            t_name = "instruments"
            self._instruments.to_sql(
                name=t_name,
                con=self._cnx,
                if_exists="replace",
                index=True,
                method="multi",
                index_label="InsCode",
            )
        if self._splits is not None and not self._splits.empty:
            t_name = "splits"
            self._splits.to_sql(
                name=t_name,
                con=self._cnx,
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
        """
        if codes:
            today_str = datetime.today().strftime("%Y%m%d")
            if self._last_devens is None:
                self._last_devens = pd.DataFrame(
                    today_str, index=codes, columns=["LastDEven"]
                )
            else:
                self._last_devens.loc[codes, "LastDEven"] = today_str
