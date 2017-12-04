from sqlalchemy import Column, DateTime, Integer, Float, String, Date, Numeric, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils.types.choice import ChoiceType

Base = declarative_base()


class CarArticle(Base):

    __tablename__ = 'car_article'

    id = Column(String, primary_key=True)
    name = Column(String(128))
    manufacturer = Column(String(128))
    year = Column(Date)
    mileage = Column(Integer)
    engine_capacity = Column(Float)
    engine_type = Column(String)
    value = Column(String)
    brutto = Column(Boolean)
    netto = Column(Boolean)
    negotiation = Column(Boolean)
    vat = Column(Boolean)
    currency = Column(String)
    location = Column(String)
    link = Column(String)
    seller_id = Column(String(32))
    record_created = Column(DateTime(timezone=True), default=func.now())
    on_delete = Column(Boolean, default=False)


    def __init__(self, id, name, manufacturer, year,
                 mileage, engine_capacity, engine_type, value,
                 brutto, netto, negotiation, vat, currency,
                 location, link, seller_id):
        self.id = id
        self.name = name
        self.manufacturer = manufacturer
        self.year = year
        self.mileage = mileage
        self.engine_capacity = engine_capacity
        self.engine_type = engine_type
        self.value = value
        self.brutto = brutto
        self.netto = netto
        self.negotiation = negotiation
        self.vat = vat
        self.currency = currency
        self.location = location
        self.link = link
        self.seller_id = seller_id

    def __str__(self):
        return '[{}] {}'.format(self.id, self.name)


class Phone(Base):

    __tablename__ = 'phone_number'

    id = Column(Integer, primary_key=True)
    number = Column(String)

    car_id = Column(String, ForeignKey('car_article.id'))
    car = relationship("CarArticle", backref="phones")

    def __init__(self, number, car):
        self.number = number
        self.car = car

    def __str__(self):
        return self.number


class MetaInfo(Base):

    __tablename__ = 'meta_info'

    id = Column(Integer, primary_key=True)
    last_start = Column(DateTime(timezone=True))
    status = Column(Boolean, default=False)