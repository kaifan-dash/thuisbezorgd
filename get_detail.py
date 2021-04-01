import pandas as pd
import pyppeteer
import json
import asyncio
from downloader import request
from parser import _parse_home_page, _parse_area_page
from pyppeteer import launch
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
from utils import color
from parser import _parse_restaurant, _parse_meals
sem = asyncio.BoundedSemaphore(4)
from utils import color, bprint
import ast
import os
import datetime
today = datetime.date.today().strftime('%Y%m')

async def parse_restaurant(browser, url):
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
            res = await page.goto(url)
            await page.waitForNavigation({'waitUntil': 'networkidle0'})
        except Exception as e:
#             await page.waitForNavigation()
#             print (color.RED + str(e) + color.END)
            pass
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        try:
            item = _parse_restaurant(soup, url)
            print(color.BLUE + item['name'] + color.END)
            with open(f'{prefix}_{today}/{prefix}_results.txt', 'a') as f:
                f.write(str(item) + '\n')
        except Exception as e:
            bprint.red(e)
            
        try:
            if res.status == 200:
                try:
                    filename = url.split('/')[-1]
                    with open(f'{cache_dir}/{filename}.html', 'w') as f:
                        f.write(content)
                except Exception as e:
                    bprint.red(f'ERROR CACHING FILE: {e}')
            else:
                bprint.red(res.status)
        except Exception as e:
            bprint.red(f'ERROR CACHING FILE: {e}')

#         try:
#             meal_items = _parse_meals(soup, url)
#             if len(meal_items) > 0:
#                 with open(f'{prefix}/{prefix}_meals.txt', 'a') as f:
#                     for item in meal_items:
#                         f.write(str(item) + '\n')
#                 bprint.blue(f'{len(meal_items)} meal items in {url}')
#         except:
#             pass


def run(urls):
    global prefix

    n = round(len(urls)/100)
    loop = asyncio.get_event_loop()
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
        loop.run_until_complete(asyncio.sleep(3))
        _urls = urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
            tasks.append(parse_restaurant(browser, url))
        loop.run_until_complete(asyncio.gather(*tasks))

        loop.run_until_complete(browser.close())
    loop.close()

def compare_urls(completed, to_run):
    completed_df = pd.DataFrame([{'url': url} for url in completed])
    to_run_df = pd.DataFrame([{'url': url} for url in to_run])
    completed_df['suffix'] = completed_df['url'].apply(lambda x: x.split('/')[-1].replace('#info', ''))
    to_run_df['suffix'] = to_run_df['url'].apply(lambda x: x.split('/')[-1].replace('#info', ''))
    final = to_run_df[~to_run_df['suffix'].isin(completed_df['suffix'].to_list())]['url'].to_list()
    return final
    
def main():
    global prefix
    global cache_dir
    prefix = sys.argv[1]
    date = sys.argv[2]
    cache_dir = f'{prefix}_{today}/html_cache/'
    try:
        os.listdir(f'{prefix}_{today}')
    except:
        os.mkdir(f'{prefix}_{today}')
    
    try:
        os.mkdir(cache_dir)
    except:
        pass

    print (color.GREEN + 'loading data' + color.END)
    data = pd.read_parquet(f's3://dashmote-product/thuisbezorgd/{date}/{prefix}_outlet_information.parquet.gzip', engine = 'fastparquet')
    print (color.GREEN + 'data loaded' + color.END)
    urls = data['url'].to_list()
    
    completed = os.listdir(f'{prefix}_{today}/html_cache/')
    if len(completed) > 0:
        urls = compare_urls(completed, urls)
    
    print (color.GREEN + f'{len(urls)} urls to run\n{len(completed)} urls completed' + color.END)

    run(urls)

main()
