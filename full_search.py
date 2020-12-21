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
from utils import *
from parser import _parse_restaurant, _parse_meals
sem = asyncio.BoundedSemaphore(4)
from utils import color, bprint
import ast
import os
import datetime
today = datetime.date.today().strftime('%Y%m')

async def load_page(browser, url, cache=True):
    global save_dir
    cache_dir = f'{save_dir}/html_cache'
    filename = url.split('/')[-1]
    try:
        os.mkdir(cache_dir)
        bprint.blue(f'first time running, creating cache folder {cache_dir}')
    except:
        pass
    #check cache
    if cache is True:
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
        res = await page.goto(url, {'timeout': 10000, 'waitUntil': 'networkidle0'})
        await page.waitForNavigation()
        # write to cache
        if cache is True and res.status == 200:
            bprint.blue(f'status: 200')
            with open(f'{cache_dir}/{filename}.html', 'w') as f:
                f.write(content)
        elif res.status:
            bprint.red(res.status)

    except Exception as e:
        bprint.red(str(e))
        pass

    content = await page.content()

    html_soup = BeautifulSoup(content, 'html.parser')
    await page.close()
    return html_soup

async def parse_province_page(browser, url):
    page = await browser.newPage()
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
    }
    await page.setExtraHTTPHeaders(headers)
    await page.setViewport(viewport={'width': 1280, 'height': 800})
    await page.setJavaScriptEnabled(enabled=True)
    try:
        await page.goto(url, {'timeout': 50000, 'waitUntil': 'networkidle0'})
        # await page.waitForNavigation()
    except Exception as e:
        bprint.red(str(e))
        pass
    content = await page.content()
    html_soup = BeautifulSoup(content, 'html.parser')


def run(url):
    global loop
    global province_urls
    province_urls = []
    browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
    html_soup = loop.run_until_complete(load_page(browser, url, cache=False))
    for div in html_soup.findAll('div', {'class': 'footer-nav'}):
        title_div = div.find('div', {'class': 'title'})
        # if title_div:
        #     print(title_div.text.strip())
        if title_div and title_div.text.strip() in [
                'Provincies', 'Regionen', 'Steden', 'Provinces'#, 'Cities', 'Communities', 'Cantons'
        ]:
            bprint.blue(title_div.text.strip())
            for a in div.findAll('a', {'class': 'keywordslink'}):
                # if _should_parse_city_or_province(a.text):
                area_page_url = '/'.join(url.split('/')[0:-2]) + a['href']
                print(area_page_url)
                province_urls.append(area_page_url)
    loop.run_until_complete(browser.close())

def run_provinces(urls):
    global loop
    global sub_area_urls
    sub_area_urls = []
    browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
    for url in urls:
        html_soup = loop.run_until_complete(load_page(browser, url))
        sub_areas = html_soup.findAll('div', {'class': 'delarea'})
        # headers['Referer'] = response.url
        for sub_area in sub_areas:
            for a in sub_area.find_all('a', href=True):
                sub_area_page_url = '/'.join(url.split('/')[0:-2]) + a['href']
                # print (sub_area_page_url)
                sub_area_urls.append(sub_area_page_url)
    loop.run_until_complete(browser.close())

def parse_sub_area(browser, url):
    global loop
    global save_dir
    global prefix
    html_soup = loop.run_until_complete(load_page(browser, url))

    restaurants = html_soup.find_all('a', {'class': 'restaurantname'})
    if len(restaurants) > 0:
        bprint.blue(f'{url}\nfound {len(restaurants)} restaurants')
        for restaurant in restaurants:
            # print (restaurant)
            suffix = restaurant['href']
            if suffix != '{{RestaurantUrl}}':
                restaurant_page_url = '/'.join(url.split('/')[0:-2]) + suffix
                print (restaurant_page_url)
                with open(f'{save_dir}/{prefix}_restaurant_urls.txt', 'a') as f:
                    f.write(f'{restaurant_page_url}\n')
    # else:
    #     bprint.red(f'{url}\nfound {len(restaurants)} restaurants')

    streets = html_soup.find_all('div', {'class': 'delarea'})
    if len(streets) > 0:
        bprint.blue(f'{url}\nfound {len(streets)} streets')
        for street in streets:
            _url = '/'.join(url.split('/')[0:-2]) + street.find('a')['href']
            bprint.yellow(f'redirect to: {_url}')
            parse_sub_area(browser, _url)


def run_sub_areas(urls):
    global loop
    n = round(len(urls)/100)
    bprint.green(f'{n+1} processes')
    for i in tqdm(range(n+1)):
        browser = loop.run_until_complete(launch(headless = True, dumpio = True, args=['--no-sandbox', '--disable-setuid-sandbox']))
        _urls = urls[i*100: i*100+100]
        tasks = []
        for url in _urls:
            tasks.append(parse_sub_area(browser, url))
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.run_until_complete(browser.close())

def main():
    global loop
    global prefix
    global province_urls
    global sub_area_urls
    global save_dir
    prefix = sys.argv[1]
    home_url = sys.argv[2]

    save_dir = f'{prefix}_{today}'
    try:
        os.listdir(save_dir)
    except:
        os.mkdir(save_dir)

    bprint.blue(f'scraping for {prefix}\nurl: {home_url}')

    loop = asyncio.get_event_loop()

    run(home_url)
    run_provinces(province_urls)
    bprint.blue(f'Got {len(sub_area_urls)} sub-area urls')
    run_sub_areas(sub_area_urls)

    loop.close()

main()
