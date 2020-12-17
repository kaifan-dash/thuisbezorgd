import pandas as pd
import pyppeteer
import json
import asyncio
from downloader import request
from pyppeteer import launch
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
from utils import color
from parser import _parse_restaurant, _parse_meals
sem = asyncio.BoundedSemaphore(4)
from utils import color, bprint
import ast

async def parse_restaurant(browser, url):
    global prefix
    async with sem:
        # with open('finished.txt', 'a') as f:
        #     f.write(f'{url}\n')
        page = await browser.newPage()
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
        }
        await page.setExtraHTTPHeaders(headers)
        await page.setViewport(viewport={'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(enabled=True)
        try:
            await page.goto(url, {'timeout': 10000, 'waitUntil': 'networkidle0'})
            # await page.waitForNavigation()
        except Exception as e:
            print (color.RED + str(e) + color.END)
            pass
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        meal_items = _parse_meals(soup, url)
        if len(meal_items) > 0:
            with open(f'{prefix}_meals.txt', 'a') as f:
                for item in meal_items:
                    f.write(str(item) + '\n')
            bprint.blue(f'{len(meal_items)} meal items in {url}')

def run(urls):
    global prefix
    # try:
    #     with open(f'{prefix}_results.txt', 'r') as f:
    #         lines = f.readlines()
    #     # lines = [x.strip('\n') for x in lines]
    #     completed = [ast.literal_eval(x)['url'] for x in lines]
    #     incomplete = list(set(urls) - set(completed))
    #     print (color.GREEN + f'{len(completed)} completed\n{len(incomplete)} incomplete' + color.END)
    # except FileNotFoundError:
    #     print (color.RED + f'{prefix}_results.txt not found, running for the first time' + color.END)
    # # results = []
    n = round(len(urls)/100)
    loop = asyncio.get_event_loop()
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True))
        loop.run_until_complete(asyncio.sleep(3))
        _urls = urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
            tasks.append(parse_restaurant(browser, url))
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.run_until_complete(browser.close())
    loop.close()

def main():
    global prefix
    prefix = sys.argv[1]
    date = sys.argv[2]

    print (color.GREEN + 'loading data' + color.END)
    data = pd.read_parquet(f's3://dashmote-product/thuisbezorgd/{date}/{prefix}_outlet_information.parquet.gzip', engine = 'fastparquet')
    print (color.GREEN + 'data loaded' + color.END)
    urls = data['url'].to_list()
    print (color.GREEN + f'{len(urls)} urls' + color.END)

    run(urls)
    # with open


main()
