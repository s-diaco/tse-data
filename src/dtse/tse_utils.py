"""
helper functions to parse tse data
"""


import re

from jdatetime import date as jdate


def convert_to_shamsi(date) -> str:
    """
    convert gregorian date to jalali date

    :param date: str or date, date object or srting in form yyyymmdd

    :return: str, date in jalali calendar formatted like yyyy/mm/dd
    """

    date = str(date)
    return jdate.fromgregorian(
        day=int(date[-2:]),
        month=int(date[4:6]),
        year=int(date[:4]),
    ).strftime("%Y/%m/%d")


def _replace(string, dictionary) -> str:
    if not isinstance(string, str):
        raise TypeError("accept string type")

    pattern = re.compile("|".join(dictionary.keys()))
    return pattern.sub(lambda x: dictionary[x.group()], string)


def clean_fa(text) -> str:
    """
    clean persian texts

    :param text: str, text to clean

    :return: str, cleaned text
    """

    characters_map = {
        "\u200B": "",  # zero-width space
        "\u200C": " ",  # zero-width non-joiner
        "\u200D": "",  # zero-width joiner
        "\uFEFF": "",  # zero-width no-break space
        "ي": "ی",
        "ك": "ک",
    }
    text = _replace(text, characters_map).strip()
    return text
