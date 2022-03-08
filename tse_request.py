import aiohttp
import config as settings


class TSERequest:
    def instrument(self, DEven: str):
        params = {
            't': 'Instrument',
            'a': DEven
        }
        return self._make_request(params)

    def instrument_and_share(self, DEven: str, LastID: int = 0):
        params = {
            't': 'InstrumentAndShare',
            'a': DEven,
            'a2': str(LastID)
        }
        return self._make_request(params)

    def last_possible_deven(self):
        params = {
            't': 'LastPossibleDeven'
        }
        return self._make_request(params)

    def closing_prices(self, ins_codes):
        params = {
            't': 'ClosingPrices',
            'a': str(ins_codes)
        }
        return self._make_request(params)

    async def _make_request(self, params: dict):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(settings.API_URL, params=params) as response:
                    if response.status == 200:
                        ret_val = await response.text()
                    else:
                        ret_val = response.status + ' ' + response.reason
        except Exception as e:
            ret_val = str(e)
        return ret_val
