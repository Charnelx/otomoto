import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import sqlite3
import json
import requests
import datetime
import pytz

from models import CarArticle, Base, Phone


#  DB utils
def setup_db(echo=False):
    engine = sqlalchemy.create_engine('sqlite:///db\\otomoto.db', echo=echo)

    Base.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def tear_up():
    if not os.path.exists('db\\otomoto.db'):
        conn = sqlite3.connect("db\\otomoto.db")
        conn.close()


def meta_get_or_create(session, model, **kwargs):
    instance = session.query(model).get(1)
    if instance:
        return instance
    else:
        instance = model()
        session.add(instance)
        return instance


# Currency utils
def get_pln_rates(base_cur='pln'):
    resp = requests.get('https://api.fixer.io/latest?base={}'.format(base_cur))
    if resp.status_code == 200:
        rates = json.loads(resp.text)

        def inner(amount, cur_to):
            rate = rates['rates'][cur_to.upper()]
            return round(rate * amount, 2)
        return inner
    raise AttributeError('Failed to get currency rates. Check connection.')


def get_ukr_rates():
    indexes = {'USD': 9, 'EUR': 8}
    resp = requests.get('http://bank-ua.com/export/exchange_rate_cash.json')
    if resp.status_code == 200:
        rates = json.loads(resp.text)

        def inner(amount, cur_from, buy=True):
            conversion_type = 'rateBuy'
            if buy:
                conversion_type = 'rateSale'
            rate = float(rates[indexes[cur_from.upper()]][conversion_type])
            return round(rate * amount, 2)
        return inner


def excises_benzine(capacity, year):
    cur_year = datetime.datetime.now(tz=pytz.utc).year

    rate = 100

    # number 7 chosen to limit discount rates by >= 2011 year
    if (cur_year - year) < 7:
        if capacity < 1.0:
            rate = 0.102
        elif 1.0 <= capacity <= 1.5:
            rate = 0.063
        elif 1.5 < capacity <= 2.2:
            rate = 0.267
        elif 2.2 < capacity <= 3:
            rate = 0.276
        else:
            # engine capacity over 3 liters
            rate = 2.209
    else:
        if capacity < 1.0:
            rate = 1.094
        elif 1.0 < capacity < 1.5:
            rate = 1.367
        elif 1.5 < capacity < 2.2:
            rate = 1.643
        elif 2.2 < capacity < 3:
            rate = 2.213
        else:
            # engine capacity over 3 liters
            rate = 3.329

    return (rate * capacity) * 1000


def excises_diesel(capacity, year):
    cur_year = datetime.datetime.now(tz=pytz.utc).year

    rate = 100

    # number 7 chosen to limit discount rates by >= 2011 year
    if (cur_year - year) < 7:
        if capacity < 1.5:
            rate = 0.103
        elif 1.5 <= capacity <= 2.5:
            rate = 0.327
        else:
            # engine capacity over 3 liters
            rate = 2.209
    else:
        if capacity < 1.5:
            rate = 1.367
        elif 1.5 <= capacity <= 2.5:
            rate = 1.923
        else:
            # engine capacity over 3 liters
            rate = 2.779

    return (rate * capacity) * 1000


def get_pension_fee(cur_obj, value):
    ua_value = cur_obj(value, 'eur')

    if ua_value < 267960:
        rate = 0.03
    elif 267960 <= ua_value <= 470960:
        rate = 0.04
    else:
        rate = 0.05

    return value * rate


def get_cc_value(value, capacity, engine_type, year, cur_obj):
    # TODO: add electrocars engine
    excises = {'benzine': excises_benzine, 'diesel': excises_diesel}
    # 10% for customs duty fee
    customs_duty = value * 0.1

    # excises
    excise = excises[engine_type](capacity, year)

    # 20% for VAT fee
    vat = (value + customs_duty + excise) * 0.2

    calculated_value = excise + customs_duty + vat
    pension_fee = get_pension_fee(cur_obj, value + excise + customs_duty)

    return calculated_value + pension_fee


# print(excises_benzine(1.4, 2011))
# print(excises_diesel(1.4, 2011))
#
# cur_pln = get_pln_rates()
# print(cur_pln(12900, 'eur'))
#
# cur_ukr = get_ukr_rates()
# print(cur_ukr(100, 'eur'))

# r = get_cc_value(7800, 1.4, 'benzine', 2011)
# print(r)