"""
test tse_request.py
"""
import pytest
from dtse.tse_request import TSERequest


@pytest.mark.vcr()
async def test_closing_prices():
    """
    test closing_prices
    """
    instance = TSERequest()
    # TODO: await not needed
    resp = await instance.closing_prices("67126881188552864,20010321,0")
    assert resp != "OK"


@pytest.mark.vcr()
async def test_instrument():
    """
    test instruments
    """
    instance = TSERequest()
    # TODO: await not needed
    resp = await instance.instrument("20210321")
    assert resp != "OK"


@pytest.mark.vcr()
async def test_last_possible_deven():
    """
    test last_possible_deven
    """
    instance = TSERequest()
    resp = await instance.last_possible_deven()
    assert resp != "OK"


@pytest.mark.vcr()
async def test_instrument_and_share():
    """
    test instrument_and_share
    """
    instance = TSERequest()
    resp = await instance.instruments_and_share("20230502", 2732)
    assert isinstance(resp, str)
