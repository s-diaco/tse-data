"""
helper functions to parse tse data
"""


import re


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
