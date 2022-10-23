from sqlalchemy import create_engine
import pandas as pd



engineP = create_engine('postgresql://postgres:mtgai@localhost/decks')
engineL = create_engine('sqlite:///.\AllPrintings.sqlite')
cards = pd.read_sql_table('cards',engineL)
cards.to_sql("printings",schema="CardData",con = engineP, chunksize=512, index=True )