"""
send api request to tse and return string data
"""

import aiohttp

import config as settings
from setup_logger import logger as tse_logger


class TSERequest:
    """
    send api request to tse and return string data
    """

    def instrument(self, last_date: str) -> str:
        """
        request instrument data from tsetmc api

        :param last_date: str, last date of the price update

        :return: str, instrument data
        """
        params = {
            't': 'Instrument',
            'a': last_date
        }
        return self._make_request(params)

    def instrument_and_share(self, last_date: str, last_id: int = 0) -> str:
        """
        request instrument and their share data from tsetmc api

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

    def last_possible_deven(self) -> str:
        """
        request date for the last possible update from tsetmc api

        :return: str, date for the last possible update
        """
        params = {
            't': 'LastPossibleDeven'
        }
        return self._make_request(params)

    def closing_prices(self, ins_codes) -> str:
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

    async def _make_request(self, params: dict) -> str:
        """
        send api request to tse and return string data

        :param params: dict, api parameters

        :return: str, response data

        :raises: aiohttp.ClientError
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(settings.API_URL, params=params) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        tse_logger.error(response.status +
                                         ' ' + response.reason)
                        raise aiohttp.ClientError
        except Exception as e:  # pylint: disable=broad-except
            tse_logger.error(e)
            raise
