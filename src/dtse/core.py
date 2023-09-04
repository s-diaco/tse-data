"""client for tsetmc api"""

import asyncio
import sys
from time import sleep

from rich.progress import track

from dtse.tse_data import TSE


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

        async def main():
            """
            create get_prices task
            """
            get_prices = TSE().get_prices(symbols=["همراه"])
            await get_prices

        asyncio.run(main())

    elif args[1] == "upgrade":
        for char in track(range(100)):
            sleep(0.05)

    elif args[1] == "reset cache":
        print("Not implemented yet!")

    elif args[1] == "redownload":
        print("Not implemented yet!")
