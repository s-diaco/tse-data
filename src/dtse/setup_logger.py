"""
config logger settings
"""
import logging
import sys


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("tse")
logging.getLogger("chardet.charsetprober").disabled = True

# logger.info("Testing logger for num %d and string %s", 4, "HelloWorld")
