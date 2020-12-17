import os
import asyncio
import logging
from pyppeteer import launch
from bs4 import BeautifulSoup
sem = asyncio.BoundedSemaphore(4)

logger = logging.getLogger()
fhandler = logging.FileHandler(filename='google.log', mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.INFO)


async def request(url):
    global browser
    page = await browser.newPage()
    await page.goto(url)
    await asyncio.sleep(5)
    content = await page.content()
    soup = BeautifulSoup(content)
    return soup

async def start_broswer():
    global browser
    print('browser init')
    browser = await launch(headless = True)

async def close_browser():
    global browser
    print('browser closed')
    await browser.close()

async def downloader(url):
    async with sem:
        global browser
        page = await browser.newPage()
        await page.setRequestInterception(True)
        page.on('request', lambda req: asyncio.ensure_future(intercept(req)))
        headers = {
          'Referer': 'https://www.lieferando.de/en/',
          'Upgrade-Insecure-Requests': '1',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/',
          'Host': 'www.lieferando.de'
        }
        await page.setExtraHTTPHeaders(headers)
        await page.setViewport(viewport={'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(enabled=True)
        try:
            await page.goto(url, {'timeout': 20000})
            await page.waitForNavigation()
        except Exception as e:
            print(e)
            content = await page.content()
            soup = BeautifulSoup(content, 'lxml')
        content = await page.content()
        soup = BeautifulSoup(content, 'lxml')
        page.close()
        return soup
