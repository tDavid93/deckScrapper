counter = 0
import pandas as pd
from sqlalchemy import create_engine
import scrapper as sc

connectionString = 'postgresql://postgres:mtgai@host.docker.internal/decks'
engineP = create_engine(connectionString)




nameIddf = pd.read_sql('select "name","scryfallOracleId" from "CardData".printings  ', engineP)
nameIddf = nameIddf.drop_duplicates()
nameIddf["name"] = nameIddf["name"].apply(lambda x: x.replace('//', '/'))
print("idFetcher: {0}".format(nameIddf.head))

while(True):
    
  tableNameDf = pd.read_sql("""SELECT table_name AS full_rel_name
    FROM information_schema.tables
  WHERE table_schema = 'decks';""",engineP)

  tableNameDf = tableNameDf.iloc[0:,:]

  all_t = tableNameDf.shape[0]
  for table in tableNameDf.to_numpy():
    print("idFetcher: {0}/{1}::{2}".format(counter,all_t,table[0]))
    counter = counter +1
    deckTemp = pd.read_sql(""" select * from "decks"."{0}";""".format(table[0]), engineP)
    #print(deckTemp["Name"].apply(lambda x : nameIddf.loc[nameIddf["name"] == difflib.get_close_matches(x, nameIddf["name"])[0]]).shape)
    deckTemp["OracleId"] = deckTemp["Name"].apply(sc.scrapper.resolve_name,args=(nameIddf,))
    
  
    
    deckTemp.to_sql(table[0],engineP,schema="deck_f",if_exists='replace')