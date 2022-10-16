"""
helper functions to parse tse data
"""


def clean_fa(text) -> str:
    """
    clean persian texts

    :param text: str, text to clean

    :return: str, cleaned text
    """
    # todo: add chars to config file
    text = text.replace('\u200B', '')  # zero-width space
    text = text.replace('\u200C', ' ')  # zero-width non-joiner
    text = text.replace('\u200D', '')  # zero-width joiner
    text = text.replace('\uFEFF', '')  # zero-width no-break space
    text = text.replace('ك', 'ک')
    text = text.replace('ي', 'ی')
    text = text.strip()
    return text
