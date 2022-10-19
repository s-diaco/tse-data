"""
test tse_parser.py
"""
from dtse import tse_parser as parser


async def test_parse_instruments():
    """
    test parse_instruments
    """
    cached_instruments = await parser.parse_instruments()
    assert len(cached_instruments) > 0
