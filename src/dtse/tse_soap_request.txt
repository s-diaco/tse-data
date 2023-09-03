"""
send api request to "service.tsetmc.com/WebService/" and return string data
"""

import base64
import struct
import zlib

import requests


async def dec_and_get_closing_prices(ins_codes: str):
    """
    Fetch historical price of stocks and indices

    :param ins_codes: str, List of instuments to request
        InsCode,LastStoredDate,0 or 1;...
        0 = stock
        1 = index
        i.e.: "2318736941376687,20210901,0;32097828799138957,20210901,1"
        see "http://service.tsetmc.com/WebService/TseClient.asmx" for more info.

    :return: str, daily price data
    """

    if not ins_codes:
        return ""

    compressor = zlib.compressobj(wbits=(16 + zlib.MAX_WBITS))
    compressed = base64.b64encode(
        struct.pack("<L", len(ins_codes))
        + compressor.compress(bytes(ins_codes, "ascii"))
        + compressor.flush()
    )

    url = "http://service.tsetmc.com/WebService/TseClient.asmx"

    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 6.0; MS Web Services Client Protocol 2.0.50727.9151)",
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": '"http://tsetmc.com/DecompressAndGetInsturmentClosingPrice"',
        "Connection": "close",
    }

    body = '<?xml version="1.0" encoding="utf-8"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><soap:Body><DecompressAndGetInsturmentClosingPrice xmlns="http://tsetmc.com/"><insCodes>{}</insCodes></DecompressAndGetInsturmentClosingPrice></soap:Body></soap:Envelope>'

    response = requests.post(
        url,
        data=body.format(compressed.decode("ascii")),
        headers=headers,
    )
    data = ""
    if response.status_code == 200:
        data = response.text  # xml response
    return data
