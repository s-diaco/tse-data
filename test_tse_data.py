"""
tests for tse_data.py
"""
import tse_data


def test_get_instruments():
    instruments = tse-data.get_instruments()
    assert len(instruments) > 0
    assert instruments[0] == 'AAPL'
    assert instruments[-1] == 'ZNGA'
