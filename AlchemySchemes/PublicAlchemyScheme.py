# coding: utf-8
from sqlalchemy import Column, Integer, Text, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Deck(Base):
    __tablename__ = 'decks'

    id = Column(Integer, primary_key=True, server_default=text("nextval('decks_id_seq'::regclass)"))
    url = Column(Text)
    author = Column(Text)
    fetched = Column(Integer)
