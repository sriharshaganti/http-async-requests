import re
import sys
import pathlib
from typing import IO

import asyncio
import logging
import urllib.error
import urllib.parse
import aiofiles
import aiohttp
import async_timeout
import aiobotocore
import botocore

from aiohttp import ClientSession,ClientError
from aiohttp.http_exceptions import HttpProcessingError

# Total Image url set
result_url = set()

# file path
FILE_PATH = pathlib.Path(__file__).parent

# Output file path
OUTPUT_FILE_PATH = FILE_PATH.joinpath("resulturls")

# Logger configuration
logger = logging.getLogger(__name__)

async def fetch_response(url: str, session: ClientSession, **kwargs) -> str:
  
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    response = await resp.json()
    return response

async def parse(url: str, session: ClientSession, **kwargs) -> set:
  
    found = set()
    try:
        response = await fetch_response(url=url, session=session, **kwargs)
    except ( ClientError,HttpProcessingError) as e:
        logger.error("aiohttp exception for %s [%s]: %s",url,getattr(e, "status", None), getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.error("Non-aiohttp exception occured:  %s", (e))
        return found
    return found.update(response)

async def url_status_validate(file: IO, url: str, session) -> None:

    res = await parse(url,session)
    logger.info('Url: %s | Number of image urls:  %s', url, len(res))
    if not res:
        return None
    result_url.update(res)
        

async def http_status_url_validate(file: IO, urls: set, **kwargs) -> None:
  
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(url_status_validate(file, url, session, **kwargs))
        await asyncio.gather(*tasks)


def main():
    """ Main script execution
    """
    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    # Configure logging on console
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=logging.INFO,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    logging.getLogger("chardet.charsetprober").disabled = True
    urls = set()
    for url in open('../url-data'):
        urls.add(url)   
    httploop = asyncio.get_event_loop()    
    httploop.run_until_complete(http_status_url_validate(file=OUTPUT_FILE_PATH, urls=urls))
    logger.info('Total number of urls : %s', len(result_url))
    with open(OUTPUT_FILE_PATH, 'w') as f:
        for item in result_url:
            f.write("%s\n" % item)
  
            
if __name__ == "__main__":
    main()