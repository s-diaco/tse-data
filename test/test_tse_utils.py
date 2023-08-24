"""
test tse_utils.py
"""
import pytest

from dtse import tse_utils

sample_texts = [
    ("ی\u200B\u200C\u200D\uFEFFكي", "ی کی"),
    ("السلام عليكم", "السلام علیکم"),
    ("كيك", "کیک"),
    (
        "ظ ط ذ د ز ر و ، . ش س ي ب ل ا ت ن م ك ض ص ث ق ف غ ع ه خ ح ؟",
        "ظ ط ذ د ز ر و ، . ش س ی ب ل ا ت ن م ک ض ص ث ق ف غ ع ه خ ح ؟",
    ),
    ("HI ي", "HI ی"),
]


@pytest.mark.parametrize("text_res", sample_texts)
def test_clean_fa(text_res):
    """
    test clean_fa
    """
    text, exp_res = text_res
    res = tse_utils.clean_fa(text)
    assert res == exp_res

    with pytest.raises(TypeError):
        tse_utils.clean_fa(12345)
