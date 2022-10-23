import concurrent.futures
from os.path import isfile
import this
import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import psycopg2
import re
import difflib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import numpy as np


from concurrent.futures import thread


class scrapper():
    
  def __init__(self) -> None:
      connectionString = 'postgresql://postgres:mtgai@host.docker.internal/decks'
      engineP = create_engine(connectionString)
      super().__init__()


  def searchForLinks(q ,deck_format="edh", price_min="", price_max="", update=""):
      #https://tappedout.net/mtg-decks/search/
  # ?
  # q=lagrella
  # format=edh
  # price_min=
  # price_max=
  # o=-date_updated
  # submit=Filter+results
  # p=2
  # page=2
      baseURL = "https://tappedout.net/mtg-decks/search/?q={q}&format={deck_format}&price_min={price_min}&price_max={price_max}&o=-date_updated{update}&submit=Filter+results&p={p}&page={page}"
      req_succes = True
      page = 1
      links = []
      while req_succes:
          #print(baseURL.format(p=page, page=page, q=q, deck_format=deck_format, price_min=price_min,price_max=price_max, update=update))
          webp = requests.get(baseURL.format(p=page, page=page, q=q, deck_format=deck_format, price_min=price_min,price_max=price_max, update=update))
          if webp.status_code == 200:
              page += 1
              soup = BeautifulSoup(webp.content, "html.parser")
              h3 = soup.find_all("h3", class_="name deck-wide-header")
              for h in h3:
                  print(h.find("a")["href"])
                  links.append(h.find("a")["href"])
          else:
              req_succes = False
      return links


  def saveLinks(links, sqlConnector):
      sqlCursor = sqlConnector.cursor()

      for link in links:
          sqlCursor.execute("SELECT name, fetched FROM deck_names WHERE name=%s;", (link,))
          resp = sqlCursor.fetchall()
          
          if resp == []:
              sqlCursor.execute("INSERT INTO deck_names  (deck_id, name, fetched) values (%s,%s,%s);", (0,link,0))
              sqlConnector.commit()
      sqlCursor.close()

  def fetchDecks(sqlConnector):
      sqlCursor = sqlConnector.cursor()
      sqlCursor.execute("SELECT name FROM deck_names WHERE fetched=0;")
      fetchLinks = sqlCursor.fetchall()
      allLinkCount = len(fetchLinks)
      progress = 0
      for link in fetchLinks:
          progress = progress + 1
          print("{prog}/{all}".format(prog=progress,all=allLinkCount))
          deck = this.parseDeckList(this.getDeckList(link[0]),sqlConnector)
          card_expanded = []
          for card in deck:
              while card[1] != 0:
                  card_expanded.append(card[0])
                  card[1] = card[1]-1
          print(len(card_expanded))
          if len(card_expanded) == 100:  
              sqlCursor.execute('INSERT INTO decks ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;', card_expanded )
              sqlConnector.commit()
              rowId = sqlCursor.fetchone()
              
              sqlCursor.execute("UPDATE deck_names SET fetched=1, deck_id = %s WHERE name = %s", (rowId[0],link[0]))
              sqlConnector.commit()
          else:
              sqlCursor.execute("DELETE FROM deck_names WHERE name = %s",(link[0],))

  def fetchDecksMultithread(sqlConnector):
      sqlCursor = sqlConnector.cursor()
      sqlCursor.execute("SELECT name FROM deck_names WHERE fetched=0;")
      fetchLinks = sqlCursor.fetchall()
      allLinkCount = len(fetchLinks)
      progress = 0
      threadCount = 20
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
                  deckLists.append(executor.submit(this.getDeckListMultithread,link))
          print("decklist: {lenght}".format( lenght=len(deckLists)))
          for rDeck in concurrent.futures.as_completed(deckLists):
              dDeck = rDeck.result()
              deck = this.parseDeckList(dDeck[1],sqlConnector)        
              card_expanded = []
              for card in deck:
                  while card[1] != 0:
                      card_expanded.append(card[0])
                      card[1] = card[1]-1
              print(len(card_expanded))
              if len(card_expanded) == 100:  
                  sqlCursor.execute('INSERT INTO decks ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;', card_expanded )
                  sqlConnector.commit()
                  rowId = sqlCursor.fetchone()
                  sqlCursor.execute("UPDATE deck_names SET fetched=1, deck_id = %s WHERE name = %s;", (rowId[0],dDeck[0][0]))
                  sqlConnector.commit()
              else:
                  sqlCursor.execute("DELETE FROM deck_names WHERE name = %s;",(dDeck[0][0],))



  def getDeckList(url):
      baseURL= "https://tappedout.net{url}?fmt=txt"
      return requests.get(baseURL.format(url=url)).text

  def getDeckListMultithread(url):
      baseURL= "https://tappedout.net{url}?fmt=txt"
      return [url,requests.get(baseURL.format(url=url[0])).text]


  def parseDeckList(dlist, sqlConnector):
      sqlCur = sqlConnector.cursor()
      deck = []
      for line in dlist.splitlines():
          line = line.strip()
          if len(line) == 0:
              continue

          if not line[0] in '0123456789':
              continue

          count = line.split(' ', maxsplit=1)[0]
          name = line.split(' ', maxsplit=1)[1]
          name = this.clearName(name)
          sqlCur.execute("SELECT id FROM cards WHERE name LIKE %s",("%{}%".format(name),))
          id = sqlCur.fetchone()
          idt = 0
          if id != None:
              idt = id[0]
          else:
              sqlCur.execute("INSERT INTO cards (name) VALUES (%s)",(name,))
              sqlConnector.commit()
              id = sqlCur.execute("SELECT id FROM cards WHERE name= %s",(name,))
              id = sqlCur.fetchone()
              idt=id[0]


          card = [idt ,int(count), name ]
          deck.append(card)
      sqlCur.close()
      return deck


  def saveDeckToSql(connectionString,link,dfc,fetch_id ):

    engine = create_engine(connectionString)
    #print(dfc)
    dfc = dfc.drop([ "Printing",  "Foil", "Alter", "Signed","Condition" , "Language" ], axis=1)
    dfc =dfc.fillna(0)
    dfc.loc[dfc["Commander"] != 0,"Commander"] = 1 
    dfc["OracleId"] = np.nan
    dfc["Name"] = dfc["Name"].str.replace("'"," ")
    
    tablename = "deck_{0}_{1}".format(link[0], fetch_id)
    
    try:
        dfc.to_sql(tablename,engine,"decks")
    except:
        return
    with Session(this.engineP) as session:
    
        session.execute("update decks Set fetched = 1 where id={0}".format(link[0]))
        session.commit()


  def createDeckTables(sqlConnector):
      cur = sqlConnector.cursor()
      cur.execute("CREATE TABLE deck_names (id serial PRIMARY KEY, deck_id integer, name text, fetched integer)")
      sqlConnector.commit()
      cur.execute("CREATE TABLE decks (id serial PRIMARY KEY, {seq})".format(seq=', '.join(map(lambda x: '"'+x+'"'+" integer",map(str,range(0,100))))))
      sqlConnector.commit()
      cur.close()

  def createCardTable(sqlConnectordeck, sqlConnectorprints):
      dCur = sqlConnectordeck.cursor()
      pCur = sqlConnectorprints.cursor()
      unr = pCur.execute("SELECT DISTINCT name FROM cards ")
      uniquenames = unr.fetchall()
      uniquenames = list(map(lambda x: (this.clearName(x[0]),), uniquenames))
      
      dCur.execute("CREATE TABLE cards (id serial PRIMARY KEY, name text) ")
      dCur.executemany("INSERT INTO cards (name) VALUES (%s)", uniquenames)
      sqlConnectordeck.commit()
      dCur.close()
      pCur.close()

  def clearCardTable(sqlConnector):
      cur = sqlConnector.cursor()
      cur.execute("DROP TABLE cards")
      sqlConnector.commit()
      cur.close()

  def clearDB(sqlConnector):
      cur = sqlConnector.cursor()
      cur.execute("DROP TABLE decks")
      sqlConnector.commit()
      cur.execute("DROP TABLE deck_names")
      sqlConnector.commit()
      cur.close()
      this.createDeckTables(sqlConnector)

  def clearName(name):
      name = str.lower(name)
      name = re.sub('\W', '', name)
      return " ".join(name.split())
  
  def csvDownloader(link):
    baseURL= "https://tappedout.net{url}?fmt=csv"
 
    return [requests.get(baseURL.format(url=link[1])).text, link] 
  def resolve_name(x, nameIddf):
   result_n = nameIddf.loc[nameIddf["name"] == x]["scryfallOracleId"]
   
   if result_n.empty:
    closest = difflib.get_close_matches(x, nameIddf["name"])
    if closest:
        result_n = nameIddf.loc[nameIddf["name"] == closest[0]]["scryfallOracleId"]
    else:
        result_n = pd.Series([""])
    
   return result_n.values[0]

  def resolveCardOracleId(x, con):

    response = con.execute("""SELECT "scryfallOracleId" FROM "CardData".printings order By similarity(name, '{0}') DESC LIMIT 1""".format(x))
    data = response.first()[0]
    return data