import requests
from datetime import datetime
import os
from os import listdir
from os.path import isfile, join
import re
import json
try: 
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

# Trading view scrape URL
tvScrapeUrl = 'https://www.tradingview.com/markets/stocks-canada/highs-and-lows-52wk-high/'
	
# Scrape data dir
scrapeDataDir = 'scrapes/'

def scrape_tradingview():
	returnData = ''
	errorString = ''
	
	fiftyTwoWkHighStub = 'TV52H'
	
	# First check if there is a cache version of this scrape on record
	dataDirFiles = [f for f in listdir(scrapeDataDir) if isfile(join(scrapeDataDir, f))]
	
	currDate = datetime.now().strftime('%Y-%m-%d')
	todayFileSubstring = fiftyTwoWkHighStub + '-' + currDate 

	# If the data for the current day exists, fetch the file name, keep a list of old files
	todayFullFileName = ''
	oldDataFileList = []
	for dataFile in dataDirFiles:
		# Find a scrape that was done today
		if todayFileSubstring in dataFile:
			todayFullFileName = dataFile
		# Find older scrapes
		elif fiftyTwoWkHighStub in dataFile:
			oldDataFileList.append(dataFile)
	
	dataFileString = ''
	if len(todayFullFileName) > 0:
		# Retrieve the data we already queried and saved to today's file
		todayFileData = open(scrapeDataDir + todayFullFileName, "r")
		dataFileString = todayFileData.read()
	else:
		# Scrape trading view for new data
		response = requests.get(tvScrapeUrl)
		dataFileString = response.text
		currDT = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
		saveFileName = scrapeDataDir + fiftyTwoWkHighStub + '-' + currDT + '.txt'
		
		# Write data to file
		saveFile = open(saveFileName, "w")
		saveFile.write("%s" % dataFileString)
		saveFile.close()
	
	# Parse the HTML into a format we want
	fullHtmlSoup = BeautifulSoup(dataFileString, 'lxml')
	#tableDiv = fullHtmlSoup.find('div', id='js-screener-container')
	#returnData = ''.join(map(str, tableDiv.contents))

	# Find the segment of code where the data sitting we want
	pattern = re.compile('(.*)window.initData.screener_data = (.*);(.*)')
	scripts = fullHtmlSoup.find_all("script")
	
	# TODO a unit test these components with good and bad data
	scriptDataString = ''
	for script in scripts:
		if(pattern.match(str(script.contents))):
			scriptDataString = str(script.contents)
	
	if len(scriptDataString) > 0:
		choppedDataString = scriptDataString.split('window.initData.screener_data = ')[1]
		semicolonIndex = choppedDataString.find(';')
		choppedDataString = choppedDataString[:semicolonIndex]
		choppedDataString = choppedDataString.replace('\\', '') # get rid of all escapes
		choppedDataString = choppedDataString[1:len(choppedDataString) - 1] # get rid of surrounding quotes
		# build the JSON object
		jsonDataObj = json.loads(choppedDataString)
		stockDataList = [] # going to be a list of dictionaries
		
		sendTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		returnData = {'sent' : sendTime}
		
		# totalCount = jsonDataObj['totalCount']
		dataArray = jsonDataObj['data']
		
		for stockData in dataArray:
			dataEntry = {}
			symbolParts = stockData['s'].split(':')
			
			dataEntry['symbolId'] = stockData['s']
			dataEntry['exchange'] = symbolParts[0]
			dataEntry['ticker'] = symbolParts[1]
			dataEntry['lastPrice'] = stockData['d'][1]
			dataEntry['changePercent'] = stockData['d'][2]
			dataEntry['changeAmount'] = stockData['d'][3]
			dataEntry['volume'] = stockData['d'][5]
			dataEntry['companyName'] = stockData['d'][11]
			dataEntry['source'] = 'TV'
			stockDataList.append(dataEntry)
		
		returnData['stockData'] = stockDataList
		
	else:
		errorString = 'Could not parse TV data.'
	
	# Delete the old scrape files once we have achieved fresh data
	if len(oldDataFileList) > 0:
		for oldDataFile in oldDataFileList:
			if os.path.exists(scrapeDataDir + oldDataFile):
				os.remove(scrapeDataDir + oldDataFile)
	
	return returnData, errorString
	
	