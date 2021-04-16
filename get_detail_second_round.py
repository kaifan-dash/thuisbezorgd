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
    global save_dir
    cache_dir = f'{save_dir}/html_cache'
    async with sem:
        filename = url.split('/')[-1]
        try:
            os.mkdir(cache_dir)
            bprint.blue(f'first time running, creating cache folder {cache_dir}')
        except:
            pass
        #check cache
        if os.path.exists(f'{cache_dir}/{filename}.html'):
            bprint.blue('cache found')
            with open(f'{cache_dir}/{filename}.html', 'r') as f:
                html_soup = BeautifulSoup(f.read(), 'html.parser')
            return html_soup
        
        page = await browser.newPage()
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
        }
        await page.setExtraHTTPHeaders(headers)
        await page.setViewport(viewport={'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(enabled=True)
        try:
#             url = url + '#info'
#             url = url.replace('https://www.lieferando.at/speisekarte/', 'https://www.lieferando.at/en/menu/')
#             print (url)
            res = await page.goto(url, {'waitUntil': 'networkidle0'})
#             await page.waitForNavigation()
        except Exception as e:
#             print (color.RED + str(e) + color.END)
            pass
        try:
            content = await page.content()
        except:
            return
        soup = BeautifulSoup(content, 'html.parser')
        
#         try:
#             if res.status == 200:
#                 # bprint.blue(f'status: 200')
#                 with open(f'{cache_dir}/{filename}.html', 'w') as f:
#                     f.write(content)
#             elif res.status:
#                 bprint.red(res.status)
#         except Exception as e:
#             bprint.red(f'ERROR CACHING FILE: {e}')
        try:
            if res.status == 200:
                try:
                    with open(f'{cache_dir}/{filename}.html', 'w') as f:
                        f.write(content)
                except Exception as e:
                    bprint.red(f'ERROR CACHING FILE: {e}')
                item = _parse_restaurant(soup, url)
                meal_items = _parse_meals(soup, url)
                try:
                    print(color.BLUE + item['name'] + color.END)
                    with open(f'{save_dir}/{prefix}_results.txt', 'a') as f:
                        f.write(str(item) + '\n')
                except:
                    pass
                if len(meal_items) > 0:
                    with open(f'{save_dir}/{prefix}_meals.txt', 'a') as f:
                        for item in meal_items:
                            f.write(str(item) + '\n')
                    bprint.blue(f'{len(meal_items)} meal items in {url}')
            else:
                bprint.red(res.status)
        except Exception as e:
            bprint.red(f'ERROR CACHING FILE: {e}')

def run(urls):
    global prefix
    global save_dir
    
    # results = []
    n = round(len(urls)/100)
    loop = asyncio.get_event_loop()
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
        loop.run_until_complete(asyncio.sleep(3))
        _urls = urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
#             url.replace('https://www.lieferando.at/', 'https://www.lieferando.at/en/')
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
    final = [x+'#info' for x in final]
    return final
    
def main():
    global prefix
    global save_dir
    prefix = sys.argv[1]
    save_dir = f'{prefix}_{today}'
    try:
        os.listdir(save_dir)
    except:
        bprint.red(f'DIRECTORY [{save_dir}] NOT FOUND')
        return

    print (color.GREEN + 'loading data' + color.END)
    with open(f'{save_dir}/{prefix}_restaurant_urls.txt', 'r') as f:
        urls = f.readlines()
    urls = list(set([x.strip('\n') for x in urls]))
    print (color.GREEN + f'{len(urls)} urls' + color.END)
    urls = [x.replace('www.lieferando.at/speisekarte/', 'www.lieferando.at/en/menu/') for x in urls]
    try:
        with open(f'{save_dir}/{prefix}_results.txt', 'r') as f:
            lines = f.readlines()
        completed = []
        for line in lines:
            try:
                completed.append(eval(line.replace('nan', 'None'))['url'])
            except:
                print (line)
        
        print (completed[:5])
        print (urls[:5])
        if len(completed) > 0:
            urls = compare_urls(completed, urls)
#         urls = list(set(urls) - set(completed))
        print (color.GREEN + f'{len(completed)} completed\n{len(urls)} incomplete' + color.END)
    except FileNotFoundError:
        print (color.RED + f'{prefix}_results.txt not found, running for the first time' + color.END)
#         incomplete = urls

    run(urls)
    # with open


main()
