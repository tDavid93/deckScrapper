import sqlalchemy
from sqlalchemy import create_engine
import AlchemySchemes.PublicAlchemyScheme as dSchem
import pandas as pd



engineP = create_engine('postgresql://postgres:mtgai@host.docker.internal/decks')
engineL = create_engine('sqlite:///AllPrintings.sqlite')
print("Engine Created")
engineP.execute("CREATE SCHEMA IF NOT EXISTS decks")
engineP.execute('CREATE SCHEMA IF NOT EXISTS "CardData"')
engineP.execute('CREATE SCHEMA IF NOT EXISTS "deck_f"')

engineP.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
print("Schema Created")
cards = pd.read_sql_table('cards',engineL)
print("JSON loaded")
cards.to_sql("printings",schema="CardData",con = engineP, chunksize=512, index=True,if_exists='replace' )
print("Cards saved to PostgreSQL")
dSchem.Base.metadata.create_all(engineP)
print("Deck table created")