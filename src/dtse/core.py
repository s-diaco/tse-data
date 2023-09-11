"""client for tsetmc api"""

import asyncio
from pathlib import Path
import sys
from time import sleep

from rich.progress import track

from dtse.tse_data import TSE
from dtse.config import storage as settings
from dtse.logger import logger as tse_logger


def update_daily_prices(symbols: list[str], **kwconf):
    """
    run get_prices async
    """

    async def update():
        """
        create get_prices task
        """
        get_prices = TSE().get_prices(symbols, **kwconf)
        await get_prices

    asyncio.run(update())


def main():
    """client for tsetmc api"""
    args = sys.argv
    if len(args) == 1:
        print("you have to pass an option:")
        print(" get all\n update \n upgrade \n redownload \n reset cache")
        sys.exit()
    if args[1] == "get all":
        print("Not implemented yet!")
    elif args[1] == "update":
        if len(args) > 2:
            update_daily_prices(symbols=args[2][1:-1].split(sep=","))
        else:
            update_daily_prices(symbols=["همراه"])
    elif args[1] == "upgrade":
        for _ in track(range(100)):
            sleep(0.05)

    elif args[1] == "reset":
        if "TSE_CACHE_DIR" in settings:
            data_dir = Path(settings["TSE_CACHE_DIR"])
            home = Path.home()
            abs_data_dir = home / data_dir
            db_file = abs_data_dir / Path(settings["DB_FILE_NAME"])
            if db_file.is_file():
                db_file.unlink()
                tse_logger.info("database is reset.")
            else:
                tse_logger.error("database not found.")
        else:
            tse_logger.warning("cache dir not found")

    elif args[1] == "redownload":
        print("Not implemented yet!")
