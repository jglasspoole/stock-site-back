import importlib
import requests
from datetime import datetime, timedelta
import os
from os import listdir
from os.path import isfile, join
import re
import json
try: 
  from BeautifulSoup import BeautifulSoup
except ImportError:
  from bs4 import BeautifulSoup

# Database import
db = importlib.import_module("helpers.db.db")

# Trading view scrape URL
tvScrapeUrl = 'https://www.tradingview.com/markets/stocks-canada/highs-and-lows-52wk-high/'
	
# Scrape data dir
scrapeDataDir = 'scrapes/'

# cacheTvMovers: Turn this to True if you want to cache the data daily (not good for updating during trading day)
def run_tv_52wh_scrape(cacheTvMovers = False):
  returnData = ''
  errorData = ''

  fiftyTwoWkHighStub = 'TV52H'

  currDate = datetime.now().strftime('%Y-%m-%d')
  currDT = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

  # We need a datetime of 15 minutes ago to account for delay in price reading
  priceUpdateDTObj = datetime.now() - timedelta(minutes=15)
  todayFileSubstring = fiftyTwoWkHighStub + '-' + currDate 

  # Log file for scrape times
  if cacheTvMovers is True:
    print("TradingView 52WHs scrape CACHE: " + currDT)
  else:
    print("TradingView 52WHs scrape WEBREQ: " + currDT)

  with open("tv_scrape_test.txt", 'a') as tvScrapeTestFile:
    logFileText = "TradingView 52WHs Scrape "
    if cacheTvMovers is True:
      logFileText += "From Cache : "
    else:
      logFileText += "From WebReq : "
    logFileText += currDT + "\n"
    tvScrapeTestFile.write(logFileText)

  # First check if there is a cache version of this scrape on record
  dataDirFiles = [f for f in listdir(scrapeDataDir) if isfile(join(scrapeDataDir, f))]

  dataFileString = ''

  # If the data for the current day exists, fetch the file name, keep a list of old files
  todayFullFileName = ''
  oldDataFileList = []

  if cacheTvMovers is True:
    for dataFile in dataDirFiles:
      # Find a scrape that was done today
      if todayFileSubstring in dataFile:
        todayFullFileName = dataFile
      # Find older scrapes
      elif fiftyTwoWkHighStub in dataFile:
        oldDataFileList.append(dataFile)
  
  if len(todayFullFileName) > 0 and cacheTvMovers is True:
    # Retrieve the data we already queried and saved to today's file
    todayFileData = open(scrapeDataDir + todayFullFileName, "r")
    dataFileString = todayFileData.read()
  else:
    # Scrape trading view for new data
    response = requests.get(tvScrapeUrl)
    dataFileString = response.text

    # Write data to file
    saveFileName = scrapeDataDir + fiftyTwoWkHighStub + '-' + currDT + '.txt'
    saveFile = open(saveFileName, "w")
    saveFile.write("%s" % dataFileString)
    saveFile.close()

  # Delete the old scrape files once we have achieved fresh data
  if cacheTvMovers is True and len(oldDataFileList) > 0:
    for oldDataFile in oldDataFileList:
      if os.path.exists(scrapeDataDir + oldDataFile):
        os.remove(scrapeDataDir + oldDataFile)

  # Parse the HTML into a format we want
  fullHtmlSoup = BeautifulSoup(dataFileString, 'lxml')

  # Find the segment of code where the data sitting we want
  pattern = re.compile('(.*)window.initData.screener_data = (.*);(.*)')
  scripts = fullHtmlSoup.find_all("script")

  # TODO a unit test these components with good and bad data
  scriptDataString = ''
  for script in scripts:
    if(pattern.match(str(script.contents))):
      scriptDataString = str(script.contents)

  if len(scriptDataString) > 0:
    # Parse the resultant data retrieved into a format we understand
    choppedDataString = scriptDataString.split('window.initData.screener_data = ')[1]
    semicolonIndex = choppedDataString.find(';')
    choppedDataString = choppedDataString[:semicolonIndex]
    choppedDataString = choppedDataString.replace('\\', '') # get rid of all escapes
    choppedDataString = choppedDataString[1:len(choppedDataString) - 1] # get rid of surrounding quotes

    # Build the JSON object
    jsonDataObj = json.loads(choppedDataString)
    
    # totalCount = jsonDataObj['totalCount']
    dataArray = jsonDataObj['data']

    # Data Sample:
    # stockDataDict = {"ticker" : tickerSymbol, "exchange_name" : "", "company_name" : "", "last_price" : "", "change_amount" : "", "change_percent" : "", 
    # 					    "share_volume" : "", "fiftytwo_week_high" : "", "fiftytwo_week_low" : ""}

    stockDataList = [] # going to be a list of dictionaries
    for stockData in dataArray:
      dataEntry = {}
      symbolParts = stockData['s'].split(':')

      dataEntry['ticker'] = str(symbolParts[1])
      dataEntry['exchange_name'] = str(symbolParts[0])
      dataEntry['company_name'] = str(stockData['d'][11])
      dataEntry['last_price'] = str(stockData['d'][1])
      dataEntry['change_amount'] = str(stockData['d'][3])
      dataEntry['change_percent'] = str(stockData['d'][2])
      dataEntry['share_volume'] = str(stockData['d'][5])
      dataEntry ['equity_type'] = str(stockData['d'][13])
      dataEntry ['equity_class'] = str(stockData['d'][14])

      stockDataList.append(dataEntry)

    """ DEBUG OUTPUT
    for stockData in stockDataList:
      with open("tv_data_sample.txt", 'a') as tvDataFile:
        tvDataFile.write("Ticker : " + stockData["ticker"] + "\n")
        tvDataFile.write("Equity Type : " + stockData["equity_type"] + "\n")
        tvDataFile.write("Equity Class : " + stockData["equity_class"] + "\n")
        tvDataFile.write("------------\n")
    """

    # Ensure we are dealing with data from TSX & TSXV in a format we accept and add to valid stocks list (for saving)
    validStockList = []
    for stockData in stockDataList:
      validStock = False
      stockData["ticker"] = stockData["ticker"].replace('.', '-')
      if stockData["exchange_name"] == "TSX":
        stockData["ticker"] += ".TO"
        validStock = True
      elif stockData["exchange_name"] == "TSXV":
        stockData["ticker"] += ".V"
        validStock = True
      
      # For now, we are ignoring preferred stocks:
      if stockData["equity_class"] == "preferred":
        validStock = False

      if validStock is True:
        validStockList.append(stockData)

    dbData = ""
    errData = ""

    for stockDataToSave in validStockList:
      dbData, errData = db.update_ticker_data(stockDataToSave, "", priceUpdateDTObj)
    
    if len(dbData) > 0:
      returnData += dbData + "\n"
    
    if len(errData) > 0:
      errorData += errData + "\n"

    # Log and errors encountered
    if len(errorData) > 0:
      with open("thread_error_log.txt", 'a') as threadErrorFile:
        threadErrorFile.write("TradingView 52WH Thread Done: " + currDT + "\n")
        threadErrorFile.write("Error: " + errorData + "\n")

    # Only update the database with price data if the last price update was > 15min ago (or non existent)
    # Set a datetime thats 15 min back into the DB update method which compares with the current price_updated
  return returnData, errorData