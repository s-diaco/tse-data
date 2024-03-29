"""
send api request to tse and return string data
"""

import aiohttp

from dtse import config as settings
from dtse.logger import logger as tse_logger


class TSERequest:
    """
    send api request to tse and return string data
    """

    async def instrument(self, last_date: str):
        """
        request instrument data from tsetmc API

        :param last_date: str, last date of the price update

        :return: str, instrument data
        """
        tse_logger.info("Getting list of instruments.")
        params = {"t": "Instrument", "a": last_date}
        return await self._make_request(params)

    async def instruments_and_share(self, last_date: str, last_id: int = 0):
        """
        Request instrument and their share data from tsetmc API

        :param last_date: str, last date of the price update
        :param last_id: int, id of the last instrument

        :return: str, instrument and their share data
        """
        tse_logger.info("Checking for new instruments…")
        params = {"t": "InstrumentAndShare",
                  "a": last_date, "a2": str(last_id)}
        return await self._make_request(params)

    async def last_possible_deven(self):
        """
        request date for the last possible update from tsetmc api

        :return: str, date for the last possible update
        """
        tse_logger.info("Checking if server has new data…")
        params = {"t": "LastPossibleDeven"}
        return await self._make_request(params)

    async def closing_prices(self, ins_codes):
        """
        request closing prices from tsetmc api

        :param ins_codes: list, instrument codes

        :return: str, closing prices
        """
        tse_logger.info("Getting prices…")
        params = {"t": "ClosingPrices", "a": str(ins_codes)}
        return await self._make_request(params)

    async def _make_request(self, params: dict):
        """
        send request to tsetmc api and return response

        :param params: dict, request parameters

        :return: str, response

        :raise: aiohttp.ClientResponseError, if request failed
        """

        async with aiohttp.ClientSession() as session:
            async with session.get(settings.API_URL, params=params) as response:
                if response.status != 200:
                    response.raise_for_status()
                return await response.text()
