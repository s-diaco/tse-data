"""
test tse_parser.py
"""
from dtse import tse_parser as parser


async def test_parse_instruments():
    """
    test parse_instruments
    """

    cached_instruments = parser.parse_instruments()
    assert len(cached_instruments) > 0


async def test_parse_shares():
    """
    test parse_instruments
    """

    cached_shares = parser.parse_shares()
    assert len(cached_shares) > 0
