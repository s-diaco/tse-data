import utils


def test_clean_fa():
    text = 'ی\u200B\u200C\u200D\uFEFFكي'
    res = utils.clean_fa(text)
    assert res == 'ی‌کی'
