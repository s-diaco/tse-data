"""
Set some settings for logger.
"""

import logging
import sys

# TODO: some cleaning neede here.
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("dtse")
logging.getLogger("chardet.charsetprober").disabled = True

# logger.info("Testing logger for num %d and string %s", 4, "HelloWorld")
