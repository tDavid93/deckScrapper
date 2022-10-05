import scrapper as sc
from sqlalchemy.orm import sessionmaker
from AlchemySchemes.PublicAlchemyScheme import Deck
from sqlalchemy import create_engine


#TODO give this data through env variable

connectionString = 'postgresql://postgres:mtgai@localhost/decks'
engineP = create_engine(connectionString)

Session = sessionmaker(bind=engineP)
session = Session()

names = open('wordlist.txt', 'r')
Lines = names.readlines()
counter = 0

lenght = len(Lines)
for name in Lines:
  counter  = counter + 1
  print(":::::::: {0} // {1} ::::::::".format(counter,lenght)) 
  links = sc.searchForLinks(q=name)
  deck=[]
  for l in links:
    exists = session.query(Deck).filter_by(url=l).first()
    if not exists:
      deck.append(Deck(url=l,fetched=0))
  session.add_all(deck)
  session.commit()