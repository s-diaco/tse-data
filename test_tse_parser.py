"""
test tse_parser.py
"""
import tse_parser as parser


async def test_parse_instruments():
    """
    test parse_instruments
    """
    res = await parser.parse_instruments()
    assert len(res) > 0
