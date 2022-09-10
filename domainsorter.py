import asyncio
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict

import easygui
import uvloop
from tqdm import tqdm


async def main() -> None:
    '''This is the main function that runs the whole program'''
    pbar = tqdm(total=nlines, desc="Sorting emails",
                unit="lines", unit_scale=True, colour="green")
    tasks = filereadentirefile(filenameis, pbar, nlines)
    await asyncio.gather(*tasks)
    pbar.close()
    for domain in domainsset:
        with open(f"sorteddomains/{domain}.txt", "a+") as f:
            f.writelines(domainsset[domain])
    return


async def worker(email: str, pbar: tqdm) -> None:
    '''This worker func does all the work that im wanting to do'''
    temp = await checkifstrcontainsat(email)
    if temp:
        try:
            domain = email.split(":", 1)[0].split("@", 1)[-1]
            domainsset[domain].add(f"{email.strip()}\n")
        except Exception as e:
            print(f"{email.casefold()}:{e}")
    pbar.update()
    return


def filereadentirefile(filenameis: str, pbar: tqdm, numoflines: int) -> set:
    """
    Reads a file and returns a queue of all the lines in the file
    """

    with open(filenameis, "r", errors='backslashreplace') as file:
        placeholder = {asyncio.create_task(
            worker(line, pbar)) for line in tqdm(file, total=numoflines, unit_scale=True, unit=" lines", desc="Reading file & create a task for each line", colour="green")}

    return placeholder


async def checkifstrcontainsat(email: str) -> bool:
    """checks if a string contains an @ symbol"""
    return "@" in email


def ncounter(filenameis: str) -> int:
    """
    Count all the lines in a file and return an int
    """
    # with tqdm(unit_scale=True, unit=" lines", desc="Counting lines") as ncounterbar:
    with tqdm(unit=" lines", desc="Counting lines") as ncounterbar:

        ncounter: int = 0
        with open(filenameis, "r", errors='backslashreplace') as f:
            for _ in f:
                ncounterbar.update()
                ncounter += 1
    return ncounter


if __name__ == "__main__":
    uvloop.install()

    Path("sorteddomains").mkdir(exist_ok=True)

    domainsset: DefaultDict[str, set[str]] = defaultdict(set)

    filenameis = Path(easygui.fileopenbox(msg="Choose a file", ))

    nlines = ncounter(filenameis)

    asyncio.run(main())
