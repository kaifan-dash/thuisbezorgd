import pandas as pd
import pyppeteer
import json
import asyncio
from downloader import request
from pyppeteer import launch
from bs4 import BeautifulSoup
from tqdm import tqdm
import sys
sem = asyncio.BoundedSemaphore(4)
from bprint import bprint as print
import ast
import os
import datetime
import fuckit
from utils import download
today = datetime.date.today().strftime('%Y%m')

def run(urls):
    global prefix
    global cache_dir

    n = round(len(urls)/100)
    loop = asyncio.get_event_loop()
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
        loop.run_until_complete(asyncio.sleep(3))
        _urls = urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
            tasks.append(download(browser, url, cache_dir))
        loop.run_until_complete(asyncio.gather(*tasks))

        loop.run_until_complete(browser.close())
    loop.close()

def compare_urls(completed, to_run):
    completed_df = pd.DataFrame([{'url': url} for url in completed])
    to_run_df = pd.DataFrame([{'url': url} for url in to_run])
    completed_df['suffix'] = completed_df['url'].apply(lambda x: x.split('/')[-1].replace('#info', '').replace('.html', ''))
    to_run_df['suffix'] = to_run_df['url'].apply(lambda x: x.split('/')[-1].replace('#info', ''))
    final = to_run_df[~to_run_df['suffix'].isin(completed_df['suffix'].to_list())]['url'].to_list()
    return final

def main():
    global prefix
    global cache_dir
    prefix = sys.argv[1]
    date = sys.argv[2]

    proj_dir = f'{prefix}_{today}'
    cache_dir = f'{proj_dir}/html_cache/'

    with fuckit:
        os.mkdir(proj_dir)
        print (f'Create project folder: {proj_dir}')

    with fuckit:
        os.mkdir(cache_dir)
        print (f'Created cache folder: {cache_dir}')

    print (f'loading data from {date}', 'blue')
    data = pd.read_parquet(f's3://dashmote-product/thuisbezorgd/{date}/{prefix}_outlet_information.parquet.gzip', engine = 'fastparquet', columns = ['url'])
    print (f'data loaded', 'green')

    urls = data['url'].to_list()

    completed = os.listdir(cache_dir)
    if len(completed) > 0:
        urls = compare_urls(completed, urls)

    print (f'{len(urls)} urls to run\n{len(completed)} urls completed', 'blue')
    run (urls)

main()