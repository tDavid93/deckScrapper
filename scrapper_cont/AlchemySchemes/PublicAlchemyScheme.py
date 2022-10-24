# coding: utf-8
from sqlalchemy import Column, Integer, Text, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Deck(Base):
    __tablename__ = 'decks'

    id = Column(Integer, primary_key=True)
    url = Column(Text)
    author = Column(Text)
    fetched = Column(Integer)
    id_fetch = Column(Integer)
