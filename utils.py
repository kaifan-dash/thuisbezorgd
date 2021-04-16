import asyncio
import pyppeteer
from bs4 import BeautifulSoup
from bprint import bprint as print
sem = asyncio.BoundedSemaphore(4)

async def download(browser, url, cache_dir):
    async with sem:
        page = await browser.newPage()
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
        }
        await page.setExtraHTTPHeaders(headers)
        await page.setViewport(viewport={'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(enabled=True)
        # await page.setDefaultNavigationTimeout(0)

        try:
            res = await page.goto(url, timeout=0)
            # await page.waitForNavigation({'waitUntil': 'networkidle0'})
        except Exception as e:
            print (e, 'red', tag = 'warning')
            pass

        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')

        try:
            if res.status == 200:
                print (url, 'green', tag = str(res.status))
                try:
                    filename = url.split('/')[-1]
                    with open(f'{cache_dir}/{filename}.html', 'w') as f:
                        f.write(content)
                except Exception as e:
                    print(f'\n{url}\nERROR CACHING FILE: {e}', 'red', tag = 'warning')
            else:
                print (url, 'red', tag = str(res.status))
        except Exception as e:
            print (f'\n{url}\n{e}', 'red', tag='warning')

        await page.close()
