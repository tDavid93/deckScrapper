import pandas as pd
import datetime
from sqlalchemy import create_engine
import concurrent.futures
import scrapper as sc
from io import StringIO
import numpy as np

#TODO give this data through env variable
connectionString = 'postgresql://postgres:mtgai@localhost/decks'
engineP = create_engine(connectionString)

nameIddf = pd.read_sql('select "name","scryfallOracleId" from "CardData".printings  ', engineP)
nameIddf = nameIddf.drop_duplicates()
nameIddf["name"] = nameIddf["name"].apply(lambda x: x.replace('//', '/'))
print(nameIddf.head)

fetch_id = datetime.datetime.now().strftime("%Y%m%d_%H:%M")

fetchLinks = pd.read_sql('SELECT id, url from decks where "fetched" = 0',engineP)
fetchLinks = np.array(fetchLinks)

allLinkCount = len(fetchLinks)
print(allLinkCount)
progress = 0
threadCount = 50
while len(fetchLinks) > 1:

 print("{prog}/{all} ::::: {len}".format(prog=progress,all=allLinkCount, len = len(fetchLinks)))
 if threadCount > len(fetchLinks):
  threadCount = len(fetchLinks)

 threadLinks = fetchLinks[0:threadCount]
 fetchLinks = fetchLinks[threadCount-1:]
 progress = progress + threadCount
 with concurrent.futures.ThreadPoolExecutor() as executor:
  deckLists = []
  for link in threadLinks:

   deckLists.append(executor.submit(sc.csvDownloader,link))
  for rDeck in concurrent.futures.as_completed(deckLists):
   dDeck = rDeck.result()
   try:
    deckdf = pd.read_csv(StringIO(dDeck[0]))
    sc.saveDeckToSql(connectionString,dDeck[1],deckdf,fetch_id )
   except:
    print("FETCH ERROR")