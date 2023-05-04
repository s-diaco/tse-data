"""
send api request to tse and return string data
"""

import aiohttp

from . import config as settings
from .setup_logger import logger as tse_logger


class TSERequest:
    """
    send api request to tse and return string data
    """

    def instrument(self, last_date: str):
        """
        request instrument data from tsetmc API

        :param last_date: str, last date of the price update

        :return: str, instrument data
        """
        params = {
            't': 'Instrument',
            'a': last_date
        }
        return self._make_request(params)

    def instrument_and_share(self, last_date: str, last_id: int = 0):
        """
        Request instrument and their share data from tsetmc API

        :param last_date: str, last date of the price update
        :param last_id: int, id of the last instrument

        :return: str, instrument and their share data
        """
        params = {
            't': 'InstrumentAndShare',
            'a': last_date,
            'a2': str(last_id)
        }
        return self._make_request(params)

    def last_possible_deven(self):
        """
        request date for the last possible update from tsetmc api

        :return: str, date for the last possible update
        """
        params = {
            't': 'LastPossibleDeven'
        }
        return self._make_request(params)

    def closing_prices(self, ins_codes):
        """
        request closing prices from tsetmc api

        :param ins_codes: list, instrument codes

        :return: str, closing prices
        """
        params = {
            't': 'ClosingPrices',
            'a': str(ins_codes)
        }
        return self._make_request(params)

    async def _make_request(self, params: dict):
        """
        send request to tsetmc api and return response

        :param params: dict, request parameters

        :return: str, response

        :raise: aiohttp.ClientError, if request failed
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(settings.API_URL, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        tse_logger.error(str(response.status) +
                                         ' ' + str(response.reason))
                        raise aiohttp.ClientError
        except aiohttp.ClientError as e:
            tse_logger.error(e)
            raise
