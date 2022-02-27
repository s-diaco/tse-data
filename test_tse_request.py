import pytest

from tse_request import TSERequest


@pytest.mark.asyncio
async def test_instrument():
    instance = TSERequest()
    # resp = await instance.instrument('20220223')
    resp = await instance.closing_prices('67126881188552864,20010321,0')
    assert resp != 'OK'
