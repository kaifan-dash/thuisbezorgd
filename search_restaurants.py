import pandas as pd
import pyppeteer
import json
import asyncio
from downloader import request
from parser import _parse_home_page, _parse_area_page, _parse_back_page
from pyppeteer import launch
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
from utils import color
from parser import _parse_restaurant
sem = asyncio.BoundedSemaphore(4)
from utils import bprint
import ast
import os
import datetime
today = datetime.date.today().strftime('%Y%m')

async def get_restaurants(browser, url):
    global prefix
    async with sem:
        page = await browser.newPage()
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
        }
        await page.setExtraHTTPHeaders(headers)
        await page.setViewport(viewport={'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(enabled=True)
        try:
            await page.goto(url, {'waitUntil': 'networkidle0'})
#             await page.waitForNavigation()
        except Exception as e:
            bprint.red(e)
            pass
        try:
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            core_url = url.split('/')[-3]
            core_url = f'https://{core_url}'
            restaurants = _parse_back_page(soup, core_url)

            sub_area = url.split('/')[-1]
            bprint.blue(f'{len(restaurants)} in {url}')
            for restaurant in restaurants:
                _url = restaurant['url']
                with open(f'{prefix}_{today}/{prefix}_restaurant_urls.txt', 'a') as f:
                    f.write(_url + '\n')
                _cuisine = restaurant['cuisine']
                with open(f'{prefix}_{today}/{prefix}_url_cuisine.txt', 'a') as f:
                        f.write(f'{restaurant}\n')
        except Exception as e:
            bprint.red(url)
            bprint.red(e)
            

def main():
    global prefix
    prefix = sys.argv[1]
    with open(f'{prefix}_{today}/{prefix}_results.txt', 'r') as f:
        lines = f.readlines()
#     outlets = pd.read_parquet(f's3://dashmote-product/thuisbezorgd/{today}/{prefix}_outlet_information.parquet.gzip')
    # lines = [x.strip('\n') for x in lines]
    # back_urls = [ast.literal_eval(x)['back_url'] for x in lines if 'back_url' in ast.literal_eval(x).keys()]
    back_urls = []
    for line in lines:
        try:
            if 'back_url' in ast.literal_eval(line).keys():
                back_urls.append(ast.literal_eval(line)['back_url'])
        except:
            print (f'malformed string: {line}')
                
    back_urls = list(set(back_urls))
    back_urls = [x.replace('en/en/', 'www.lieferando.at/en/') for x in back_urls]
    back_urls = [x.replace('https://be-en', 'https://www.takeaway.com') for x in back_urls]
    bprint.green(f'{len(back_urls)} sub areas in {prefix}')

    n = round(len(back_urls)/100)
    loop = asyncio.get_event_loop()
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
        loop.run_until_complete(asyncio.sleep(3))
        _urls = back_urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
            if '///' not in url:
                tasks.append(get_restaurants(browser, url)) ## calling requests
        loop.run_until_complete(asyncio.gather(*tasks))

        loop.run_until_complete(browser.close())
    loop.close()

main()
