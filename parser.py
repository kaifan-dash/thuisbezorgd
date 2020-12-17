from bs4 import BeautifulSoup
import re
import json
import fuckit
from unidecode import unidecode
from utils import color

def _parse_home_page(html_soup, url):
    """Parses the home page, i.e. thuisbezorgd.nl, crawling for area
    pages, i.e. provinces/cities.

    Args:
        response (scrapy.http.Response): Response of fetching the home
            page.
    """
    url_list = []
    for div in html_soup.find_all('div', {'class': 'footer-nav'}):
        title_div = div.find('div', {'class': 'title'})
        if title_div:
            print(title_div.text.strip())
        # print ([x.text.strip() for x in title_div])
        if title_div and title_div.text.strip() in [
                'Provincies', 'Regionen', 'Steden', 'Provinces', 'Cities', 'Communities', 'Cantons'
        ]:
            for a in div.find_all('a', {'class': 'keywordslink'}):
                area_page_url = url + (a['href'])
                print(area_page_url)
                url_list.append(area_page_url)
    return url_list

def _parse_area_page(html_soup, url):
    """Parses an area page, e.g.
    https://www.thuisbezorgd.nl/eten-bestellen-amsterdam or
    https://www.thuisbezorgd.nl/eten-bestellen-noord-holland.

    Args:
        response (scrapy.http.Response): Response of fetching an area page.
    """
    # url = 'https://www.lieferando.de'
    sub_area_urls = []
    restaurant_urls = []
    sub_areas = html_soup.find_all('div', {'class': 'delarea'})
    for sub_area in sub_areas:
        for a in sub_area.find_all('a', href=True):
            sub_area_page_url = url + a['href']
            sub_area_urls.append(sub_area_page_url)

    restaurants = html_soup.find_all('a', {'class': 'restaurantname'})
    for a in restaurants:
        if a['href'] != '{{RestaurantUrl}}':
            restaurant_page_url = url + a['href']
            restaurant_urls.append(restaurant_page_url)

    return sub_area_urls, restaurant_urls

def _parse_restaurant(soup, url):
    item = {}
    item['id_source'] = url
    item['source'] = 'thuisbezorgd'
    head = soup.find_all('head')[0]
    title = head.find_all('title')[0].text

    item['url'] = url
    try:
        item['cuisine'] = title.split(' - ')[1] \
            .replace(',', ';').replace('"', "'")
    except:
        pass
    with fuckit:
        back_url = soup.find('a', {'class': 'go-back-button js-go-back-button'})['href']
        core_url = url.split('/')[-3]
        back_url = f'https://{core_url}{back_url}'
        item['back_url'] = back_url
    item['category'] = "meal-delivery;restaurant"
    try:
        text = soup.find_all(
            'script',
            {'type': 'application/ld+json'}
        )[0].contents[0]
    except Exception as e:
        print (color.RED + f'invalid url: {url}' + color.END)
        # print (item)
        return item
    item['name'] = title.split(' - ')[0].replace('"', "'")
    # print (text)
    text = re.sub(r", *,", ",", text.replace('\n', ''))

    json_text = json.loads(text)

    item['lat'] = json_text.get('geo', {}).get('latitude', 0)
    item['lon'] = json_text.get('geo', {}).get('longitude', 0)
    
    item['phone'] = None

    with fuckit:
        item['phone'] = json_text['telephone']

    item['rating'] = json_text.get('aggregateRating', {}) \
        .get('ratingValue', 'NULL')
    item['reviews_nr'] = json_text.get('aggregateRating', {}) \
        .get('reviewCount', 0)
    item['street'] = json_text.get('address', {}).get('streetAddress')
    with fuckit:
        city = json_text.get('address', {}).get('addressLocality')
        city_normal=city.replace('(','').replace(')','')
        city_decode=unidecode(city_normal)
        item['city']=city_decode
    with fuckit:
        item['postal_code'] = json_text.get('address', {}).get('postalCode')
    item['country'] = json_text.get('address', {}).get('addressCountry')
    item['price_level'] = None
    item['address'] = item['street'] + ' ' + item['city']
    return item

def _parse_back_page(soup, url):
    restaurant_urls = []
    restaurants = soup.find_all('a', {'class': 'restaurantname'})
    for a in restaurants:
        if a['href'] != '{{RestaurantUrl}}':
            restaurant_page_url = url + a['href']
            restaurant_urls.append(restaurant_page_url)
    return restaurant_urls

def _parse_meals(soup, url):
    meal_items = []
    meals=soup.findAll('div','meal-container')
    for meal in meals:
        item = {}
        item['id_source'] = url
        item['source'] = 'thuisbezorgd'
        item['category'] = 'group'
        name = meal.find('span',{'data-product-name':True}).get('data-product-name')

        item['name'] = re.sub(
            r"  *",
            " ",
            name.replace('\n', '')
        ).replace('"', "'").strip()
        price=meal.find('div',{'class':'meal__price'}).get_text()
        item['price'] = re.sub(
            r"  *",
            " ",
            price.replace('\n', '')
        ).replace('€', '').replace(',', '.').strip()
        info = meal.find('div',{'class':'meal__description-additional-info'})
        if info is not None:
            item['item_description'] = re.sub(
                r"  *",
                " ",
                info.get_text().replace('\n', '')
            ).replace('"', "'").strip()
        else:
            item['item_description'] = ''
        choices  = meal.find('div',{'class':'meal__description-choose-from'})
        if choices is not None:
            item['choices'] = re.sub(
                r"  *",
                " ",
                choices.get_text().replace('\n', '')
            ).replace('"', "'").strip()
        else:
            item['choices'] = ''
        image_url = meal.find('img',{'class':'meal__product-image'})
        if image_url is not None:
            item['image_url'] = image_url.get('data-src').replace('//', '')
        else:
            item['image_url'] = ''
        meal_items.append(item)

    return meal_items
