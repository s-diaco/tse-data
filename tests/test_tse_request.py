"""test tse_request.py"""

import re
from io import StringIO

import pandas as pd
import pytest

from dtse.tse_request import TSERequest


@pytest.mark.vcr()
async def test_closing_prices():
    """
    test closing_prices
    """
    pattern = re.compile(r"^[\d.,;@-]+$")
    instance = TSERequest()
    resp = await instance.closing_prices("67126881188552864,20220321,0")
    assert pattern.match(resp)


@pytest.mark.vcr()
async def test_instrument():
    """
    test instruments
    """
    instance = TSERequest()
    resp = await instance.instrument("20230321")
    instrums_df = pd.read_csv(
        StringIO(resp),
        lineterminator=";",
    )
    assert instrums_df.empty is not True
    assert len(instrums_df.columns) == 18
    assert len(instrums_df.index) > 500


@pytest.mark.vcr()
async def test_last_possible_deven():
    """
    test last_possible_deven
    """

    pattern = re.compile(r"^\d{8};\d{8}$")
    instance = TSERequest()
    resp = await instance.last_possible_deven()
    assert pattern.match(resp)


@pytest.mark.vcr()
async def test_instrument_and_share():
    """
    test instrument_and_share
    """
    instance = TSERequest()
    resp = await instance.instruments_and_share("20230502", 2732)
    splits = pd.read_csv(
        StringIO(resp.split("@")[1]),
        lineterminator=";",
    )
    assert splits.empty is not True
    assert len(splits.columns) == 5
    assert len(splits.index) > 100
