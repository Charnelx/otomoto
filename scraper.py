import aiohttp
import argparse
import asyncio
import datetime
from datetime import date
from decimal import Decimal
from hashlib import md5
import json
import logging
from lxml import html
from math import ceil, floor
import pytz
import re

from models import CarArticle, Base, Phone, MetaInfo
from session import GSession
from utils import setup_db, tear_up, meta_get_or_create


def db_fill(session: object, data: list):
    articles = []
    phones = []
    ids_pool = []

    logger.info('{} articles found in general.'.format(len(data) * 32))

    db_meta = meta_get_or_create(session, MetaInfo)
    db_meta.last_start = datetime.datetime.now(tz=pytz.utc)
    db_meta.status = False
    session.commit()

    ins_counter = 0
    for collection in data:
        if not isinstance(collection, list):
            logger.warning('No articles found in collection.')
            continue
        for article in collection:
            link = article['link']
            article_id = md5(link.encode()).hexdigest()

            if article_id not in ids_pool:
                ids_pool.append(article_id)
            else:
                logger.debug('Article {} with id {} already present in DB.'.format(link, article_id))
                continue

            if not session.query(CarArticle).get(article_id):
                ins_counter += 1

                brutto = True if 'brutto' in article['price_detail'] else False
                netto = True if 'netto' in article['price_detail'] else False
                negotiation = True if 'donegocjacji' in article['price_detail'] else False
                vat = True if 'fakturavat' in article['price_detail'] else False

                car_obj = CarArticle(article_id,
                                     article['name'],
                                     article['manufacturer'],
                                     article['item_year'],
                                     article['item_mileage'],
                                     article['item_engine_capacity'],
                                     article['item_fuel_type'],
                                     str(article['price']),
                                     brutto,
                                     netto,
                                     negotiation,
                                     vat,
                                     article['currency'],
                                     article['seller_location'],
                                     link,
                                     article['seller_id'])

                articles.append(car_obj)

                for phone in article['phones']:
                    phone_obj = Phone(phone, car_obj)
                    phones.append(phone_obj)

    session.bulk_save_objects(articles)
    session.bulk_save_objects(phones)

    db_meta.status = True

    session.commit()

    logger.info('\nScraper finished successfully. {} new articles found and inserted.'.format(ins_counter))


def args_init():
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('--value_min', required=False, nargs=1,
                            default=0, help='Car minimum value')
    arg_parser.add_argument('--value_max', required=False, nargs=1,
                            default=0, help='Car maximum value')
    arg_parser.add_argument('--year_from', required=False,
                            default='1990', help='Production year - from')
    arg_parser.add_argument('--year_to', required=False,
                            default=str(date.today().year), help='Production year - to')
    arg_parser.add_argument('--mileage_min', required=False,
                            default=0, help='Minimum mileage')
    arg_parser.add_argument('--mileage_max', required=False,
                            default=0, help='Maximum mileage')
    arg_parser.add_argument('--fuel_type', required=False,
                            default='', help='Engine fuel type')
    arg_parser.add_argument('--damaged', required=False,
                            default=0, help='Add damaged cars to search query')

    args = arg_parser.parse_args()

    value_min = args.value_min
    value_max = args.value_max

    year_from = datetime.datetime.strptime(args.year_from, '%Y')
    year_to = datetime.datetime.strptime(args.year_to, '%Y')

    mileage_min = args.mileage_min
    mileage_max = args.mileage_max
    fuel_type = args.fuel_type

    damaged = args.damaged

    logger.info('Keys accepted.')

    return value_min, value_max, year_from, year_to, mileage_min, mileage_max, fuel_type, damaged


class FilterArticle(object):
    """
    This class used to filter car articles information for
    further insertion into DB
    """

    # patterns
    PATTERN_MANUFACTURER = re.compile(r'^([\w-]+)\s')
    PATTERN_ENGINE_CAPACITY = re.compile(r'^(\d+)')
    PATTERN_ID_FROM_URL = re.compile(r'.*ID([\w\d]+)\.html')

    # constants
    FILTER_FIELDS = (
        'name', 'manufacturer', 'price', 'currency',
        'price_detail', 'item_year', 'item_mileage', 'item_engine_capacity',
        'item_fuel_type', 'link', 'seller_location', 'seller_id'
    )

    @staticmethod
    def strip_all(string: str, concat_symb=' ') -> str:
        string_parts = string.split()

        for idx, part in enumerate(string_parts):
            if not part or part == ' ':
                string_parts.remove(idx)
            else:
                break

        string = concat_symb.join(string_parts)
        string = string.rstrip(' ')

        return string.lower()

    @staticmethod
    def float_round(num, places=0, direction=floor):
        return direction(num * (10 ** places)) / float(10 ** places)

    def get_dict(self, *args):
        dic = None
        try:
            fields = self.filter(*args)
            if fields:
                dic = {k: v for k, v in zip(self.FILTER_FIELDS, fields)}
        except Exception as err:
            logger.error('filtering article data failed. Reason: {}'.format(err))
        return dic

    def filter(self, *args):
        name, \
        price, \
        currency, \
        price_detail, \
        item_year, \
        item_mileage, \
        item_engine_capacity, \
        item_fuel_type, \
        link, \
        seller_location = args

        logger.debug('Filtering {}'.format(link))

        name = self.strip_all(name)
        manufacturer_matched = re.match(self.PATTERN_MANUFACTURER, name)
        if manufacturer_matched:
            manufacturer = manufacturer_matched.group(1)
            name = name.replace(manufacturer, '')[1:]  # remove manufacturer and trailing space
        else:
            manufacturer = 'Unknown'

        try:
            price_string = price.replace(' ', '')
            price_string = price_string.split(',')[0]
            price = Decimal(price_string).quantize(Decimal('.00'))
        except Exception as err:
            logger.warning('Unable to process price [{}] for article {}. Price set to zero.'.format(price, link))
            price = Decimal('0').quantize(Decimal('.00'))

        currency = self.strip_all(currency)

        try:
            price_detail = self.strip_all(price_detail, concat_symb='').split(',')
        except:
            logger.warning('Unable to process price details [{}] for article {}. Details set to "brutto".')
            price_detail = ['brutto']

        item_year_string = self.strip_all(item_year, concat_symb='')
        item_year = datetime.datetime.strptime(item_year_string, '%Y')

        # cut last two chars - kilometer abbreviation
        try:
            item_mileage = int(item_mileage.replace(' ', '')[:-2])
        except Exception as err:
            logger.warning('Unable to process mileage [{}] for article {}. Mileage set to 0'.format(item_mileage, link))
            item_mileage = 0


        if not item_engine_capacity:
            item_engine_capacity = '50 cm3'
        item_engine_capacity_string = self.strip_all(item_engine_capacity, concat_symb='')
        item_engine_capacity_string = re.search(self.PATTERN_ENGINE_CAPACITY, item_engine_capacity_string)
        if item_engine_capacity_string:
            item_engine_capacity_full = int(item_engine_capacity_string.group(1)) / 1000
        item_engine_capacity_rounded = self.float_round(item_engine_capacity_full, 1, direction=ceil)

        try:
            item_fuel_type = self.strip_all(item_fuel_type, concat_symb='')
        except Exception as err:
            logger.warning('Unable to process fuel type [{}] for article {}. '
                           'Fuel type set to benzyna'.format(item_mileage, link))
            item_fuel_type = 'benzyna'

        try:
            seller_location = self.strip_all(seller_location)
        except Exception as err:
            logger.warning('Unable to process seller location [{}] for article {}.'
                           ' Location set to "Unknown"'.format(seller_location, link))
            seller_location = 'Unknown'

        try:
            seller_id = re.match(self.PATTERN_ID_FROM_URL, link).group(1)
        except Exception as err:
            logger.warning('Unable to process seller id for article {}.'
                           ' This article would be skipped.'.format(link))
            return None

        try:
            link = link.split('#')[0]
        except:
            logger.warning('Unable to process link for article {}.'
                           ' This article would be skipped.'.format(link))

        return name, manufacturer, price, currency, price_detail, \
              item_year, item_mileage, item_engine_capacity_rounded, \
              item_fuel_type, link, seller_location, seller_id


class Scraper(object):

    BASE_URL = 'https://www.otomoto.pl/oferty/'
    ENTRY_URL = None

    def __init__(self, *args, **kwargs):
        assert len(args) == 7

        value_min, value_max, year_from, year_to, mileage_min, mileage_max, fuel_type, damaged = args

        self.value_min = value_min
        self.value_max = value_max
        self.year_from = year_from
        self.year_to = year_to
        self.mileage_min = mileage_min
        self.mileage_max = mileage_max
        self.fuel_type = fuel_type
        self.damaged = damaged

        async_limit = kwargs.get('limit', 50)
        self.semaphore = asyncio.Semaphore(async_limit)

        self.timeout = kwargs.get('timeout', 15)

        self.parse_limit = kwargs.get('pages_limit', 500)

        self.filter = FilterArticle()

    def start(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        result = loop.run_until_complete(self.run())

        loop.close()

        return result

    async def run(self):
        # create session
        self.session = await self.init_session()

        # get url's list
        urls = await self.prepare()

        coros = []
        for url in urls[:self.parse_limit]:
            coros.append(self.scrap_content(url))

        result = await asyncio.gather(*coros, return_exceptions=True)

        self.session.close()

        return result

    async def init_session(self):
        HEADERS = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'www.otomoto.pl',
            'Origin': 'https://www.otomoto.pl',
            'Referer': 'https://www.otomoto.pl/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/61.0.3163.100 Safari/537.36 OPR/48.0.2685.50',
        }
        session = GSession(headers=HEADERS)
        return session

    async def prepare(self) -> list:
        payload = self.form_payload()
        response = await self.session.post(self.BASE_URL, data=payload, semaphore=self.semaphore, timeout=self.timeout)
        if response.status == 200:
            # get pages urls
            self.ENTRY_URL = response.url
            urls = await self.parse_url_range(response.content)
            return urls
        else:
            logger.error('Prepare stage failed: response status {}'.format(response.status))
            return []

    def form_payload(self) -> str:
        payload = {
            'search[category_id]': 29,
            'search[filter_enum_make]': '',
            'search[filter_float_price:from]': self.value_min or '',
            'search[filter_float_price:to]': self.value_max or '',
            'search[filter_float_year:from]': self.year_from.year or '',
            'search[filter_float_year:to]': self.year_to.year or '',
            'search[filter_float_mileage:from]': self.mileage_min or '',
            'search[filter_float_mileage:to]': self.mileage_max or '',
            'search[filter_enum_fuel_type]': self.fuel_type or '',
            'search[filter_enum_damaged]:': self.damaged if self.damaged else '',
            'search[filter_enum_no_accident]': '1' if not self.damaged else '',
        }

        return payload

    async def scrap_content(self, url: str) -> list:
        logger.debug('Start of processing {}'.format(url))

        response = await self.session.get(url, semaphore=self.semaphore, timeout=self.timeout)
        if response.status == 200:
            content_data = await self.parse_content(response.content)
            return content_data
        else:
            logger.warning('Response status != 200 at {}'.format(url))
            return []

    async def parse_url_range(self, page_content: str) -> list:
        """
        This method is used for obtaining list if URL's with car articles.
        Each article contains up to 32 offers.

        :param page_content:
        :return:
        """
        root = html.fromstring(page_content)

        try:
            page_anchor = root.xpath('//ul[@class="om-pager rel"]')[0]
            page_count = page_anchor.xpath('./li[position() = last() - 1]/a/span/text()')[0]
        except Exception:
            logger.warning('No articles found on a page. Empty list will be returned')
            return []

        urls = []
        for i in range(1, int(page_count)):
            urls.append('{}&page={}'.format(self.ENTRY_URL, i))

        return urls

    async def parse_content(self, page_content: str) -> list:
        """
        This function parse content of items in search page.

        :param page_content: (str) page content
        :return:
        """
        articles_data = []

        root = html.fromstring(page_content)

        articles = root.xpath('//article')

        for article in articles:
            # TODO: take out offer-item__content to the outer scope
            name = article.findtext('./div[@class="offer-item__content"]/div[@class="offer-item__title"]/h2/a')

            link_element = article.find('./div[@class="offer-item__content"]/div[@class="offer-item__title"]/h2/a')
            link = link_element.attrib['href']

            # price details
            offer_detail = article.find('./div[@class="offer-item__content"]/div[@class="offer-item__price"]/'
                                 'div[@class="offer-price"]')
            price = offer_detail.findtext('./span[@class="offer-price__number"]')
            currency = offer_detail.findtext('./span[@class="offer-price__number"]/span[@class="offer-price__currency"]')
            price_detail = offer_detail.findtext('./span[@class="offer-price__details"]')

            # car parameters
            item_params = article.find('./div[@class="offer-item__content"]/ul[@class="offer-item__params"]')
            item_year = item_params.findtext('./li[@data-code="year"]/span')
            item_mileage = item_params.findtext('./li[@data-code="mileage"]/span')
            item_engine_capacity = item_params.findtext('./li[@data-code="engine_capacity"]/span')
            item_fuel_type = item_params.findtext('./li[@data-code="fuel_type"]/span')

            # location
            location_temp = article.xpath('./div[@class="offer-item__content"]'
                                    '/div[contains(@class, "offer-item__bottom-row ")]')
            location = ''
            if location_temp:
                location = location_temp[0].findtext('./span[@class="offer-item__location"]/h4')

            # logger.debug(name, price, currency, price_detail, item_year, item_mileage, item_engine_capacity, item_fuel_type, link)
            article_data = self.filter.get_dict(
                                            name,
                                            price,
                                            currency,
                                            price_detail,
                                            item_year,
                                            item_mileage,
                                            item_engine_capacity,
                                            item_fuel_type,
                                            link,
                                            location
            )

            if not article_data:
                logger.warning('failed to parse article at {}'.format(link))
            else:
                phones = await self.get_phones(article_data['seller_id'])
                article_data['phones'] = phones
                articles_data.append(article_data)

        return articles_data

    async def get_phones(self, seller_id: str) -> list:
        phones = []
        counter = 0
        while True:
            response = await self.session.get('https://www.otomoto.pl/ajax/misc/contact/multi_phone/{0}/{1}/'.format(
                seller_id,
                counter
            ), semaphore=self.semaphore)
            if response.status != 200:
                break
            phones.append(json.loads(response.content)['value'].replace(' ', ''))
            counter += 1
        return phones

if __name__ == '__main__':

    tear_up()

    session = setup_db()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_formatter = logging.Formatter('[%(asctime)s](App: %(name)s)<Level: %(levelname)s>: %(message)s')

    # console output handler
    con_handler = logging.StreamHandler()
    con_handler.setLevel(logging.INFO)
    con_handler.setFormatter(log_formatter)

    # file output handler
    file_handler = logging.FileHandler('log.txt')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_formatter)

    logger.addHandler(con_handler)
    logger.addHandler(file_handler)

    input_args = args_init()

    scraper = Scraper(*input_args, limit=50, pages_limit=500)

    logger.info('Scraping started.')

    data = scraper.start()

    db_fill(session, data)