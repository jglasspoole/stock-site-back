import logging
import threading
import importlib
import random
import requests
import time
import json
from datetime import datetime
import os
from os import listdir
from os.path import isfile, join
import signal
import sys

db = importlib.import_module("helpers.db.db")

tmxScrapeUrlTSX = 'https://web.tmxmoney.com/json/embed.json.php?type=mm&exgroup=tsx'
tmxScrapeUrlTSXV = 'https://web.tmxmoney.com/json/embed.json.php?type=mm&exgroup=tsv'

cacheTmxMovers = False # Turn this to True if you want to cache the movers daily (not good for updating during trading day)

# Scrape data dir
scrapeDataDir = 'scrapes/'

scrapeThread = None

def run_scrape_thread():

	# Append to scrape log
	with open("scrape_log.txt", 'a') as scrapeLogFile:
		currDT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		scrapeLogFile.write("Started scraping: " + currDT + "\n")

	scrapeThread = threading.Thread(target=thread_method_scraping)
		
	def signal_handler(sig, frame):
		if scrapeThread is not None:
			scrapeThread.do_run = False
		sys.exit(0)
		
	signal.signal(signal.SIGINT, signal_handler)
		
	scrapeThread.start()
	
	# Example thread
	# x = threading.Thread(target=thread_function, args=(1,))
	# x.start()
	# x.join()

def thread_method_scraping():
	returnData = ""
	errorData = ""
	
	currThread = threading.currentThread()
	scrapeExchange = "TSX"
	
	# Looping thread, scrape TSX, then TSXV for volume movers
	while getattr(currThread, "do_run", True):
		dbData = ""
		errData = ""
		
		if scrapeExchange == "TSX":
			dbData, errData = run_tmx_tsx_scrape()
			scrapeExchange = "TSXV" # switch to TSXV exchange for next loop
		elif scrapeExchange == "TSXV":
			dbData, errData = run_tmx_tsxv_scrape()
			scrapeExchange = "TSX" # switch back to TSX exchange for next loop
			
		if len(dbData) > 0:
			returnData += dbData + "\n"
			
		if len(errData) > 0:
			errorData += errData + "\n"
			
		# Wait for a period of time until next loop iteration 

		# PRODUCTION
		secsToWait = 220 + random.randint(1,62)

		# TESTING 
		# secsToWait = 20 + random.randint(5, 15)

		secsWaited = 0
		while secsWaited < secsToWait and getattr(currThread, "do_run", True):
			time.sleep(1)
			secsWaited += 1
		
	return returnData, errorData

def run_tmx_tsx_scrape():
	returnData = ""
	errorData = ""

	volMoversFileStub = 'TMX_TSX_VolMovers' # TODO do not cache this data during trading hours
	
	currDate = datetime.now().strftime('%Y-%m-%d')
	currDT = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
	todayFileSubstring = volMoversFileStub + '-' + currDate 

	# Log file for scrape times
	print("TMX TSX Volume scrape " + currDT)
	with open("tmx_scrape_test.txt", 'a') as tmxScrapeTestFile:
		tmxScrapeTestFile.write("TSX VolMover Scrape : " + currDT + "\n")
	
	# First check if there is a cache version of this scrape on record
	dataDirFiles = [f for f in listdir(scrapeDataDir) if isfile(join(scrapeDataDir, f))]
	
	dataFileString = ''
	
	# If the data for the current day exists, fetch the file name, keep a list of old files
	todayFullFileName = ''
	oldDataFileList = []
	
	if cacheTmxMovers is True:
		for dataFile in dataDirFiles:
			# Find a scrape that was done today
			if todayFileSubstring in dataFile:
				todayFullFileName = dataFile
			# Find older scrapes
			elif volMoversFileStub in dataFile:
				oldDataFileList.append(dataFile)
	
	if len(todayFullFileName) > 0 and cacheTmxMovers is True:
		# Retrieve the data we already queried and saved to today's cache file
		todayFileData = open(scrapeDataDir + todayFullFileName, "r")
		dataFileString = todayFileData.read()
	else:
		# Scrape trading view for new data
		response = requests.get(tmxScrapeUrlTSX)
		dataFileString = response.text
		
		if cacheTmxMovers is True:
			saveFileName = scrapeDataDir + volMoversFileStub + '-' + currDT + '.txt'
			
			# Write data to file
			saveFile = open(saveFileName, "w")
			saveFile.write("%s" % dataFileString)
			saveFile.close()
			
	# Delete the old scrape files once we have achieved fresh data
	if cacheTmxMovers is True and len(oldDataFileList) > 0:
		for oldDataFile in oldDataFileList:
			if os.path.exists(scrapeDataDir + oldDataFile):
				os.remove(scrapeDataDir + oldDataFile)
		
	# Convert string to JSON
	requestDataJson = json.loads(dataFileString)
	
	# Get the parsed stock information structure and featured stock structure
	symbolData, featuredData = populate_volume_mover_symbol_data(requestDataJson, 1)
	
	# Write each stock information structure to the database
	dbData = ""
	errData = ""
	for symbolDataItem in symbolData:
		dbData, errData = db.update_ticker_data(symbolDataItem)
	
	if len(dbData) > 0:
		returnData += dbData + "\n"
	
	if len(errData) > 0:
		errorData += errData + "\n"

	# Write each featured stock structure to the database
	dbData = ""
	errData = ""
	for featuredDataItem in featuredData:
		dbData, errData = db.update_featured_stock_data(featuredDataItem)
	
	if len(dbData) > 0:
		returnData += dbData + "\n"
	
	if len(errData) > 0:
		errorData += errData + "\n"
		
	# Log and errors encountered
	if len(errorData) > 0:
		with open("thread_error_log.txt", 'a') as threadErrorFile:
			threadErrorFile.write("TMX TSX VolMover Thread Done: " + currDT + "\n")
			threadErrorFile.write("Error: " + errorData + "\n")
	
	return returnData, errorData

def run_tmx_tsxv_scrape():
	returnData = ""
	errorData = ""

	volMoversFileStub = 'TMX_TSXV_VolMovers' # TODO do not cache this data during trading hours
	
	currDate = datetime.now().strftime('%Y-%m-%d')
	currDT = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
	todayFileSubstring = volMoversFileStub + '-' + currDate 

	# Log file for scrape times
	print("TMX TSXV Volume scrape " + currDT)
	with open("tmx_scrape_test.txt", 'a') as tmxScrapeTestFile:
		tmxScrapeTestFile.write("TSXV VolMover Scrape : " + currDT + "\n")

	# First check if there is a cache version of this scrape on record
	dataDirFiles = [f for f in listdir(scrapeDataDir) if isfile(join(scrapeDataDir, f))]
	
	dataFileString = ''
	
	# If the data for the current day exists, fetch the file name, keep a list of old files
	todayFullFileName = ''
	oldDataFileList = []
	
	if cacheTmxMovers is True:
		for dataFile in dataDirFiles:
			# Find a scrape that was done today
			if todayFileSubstring in dataFile:
				todayFullFileName = dataFile
			# Find older scrapes
			elif volMoversFileStub in dataFile:
				oldDataFileList.append(dataFile)
	
	if len(todayFullFileName) > 0 and cacheTmxMovers is True:
		# Retrieve the data we already queried and saved to today's cache file
		todayFileData = open(scrapeDataDir + todayFullFileName, "r")
		dataFileString = todayFileData.read()
	else:
		# Scrape trading view for new data
		response = requests.get(tmxScrapeUrlTSXV)
		dataFileString = response.text
		
		if cacheTmxMovers is True:
			saveFileName = scrapeDataDir + volMoversFileStub + '-' + currDT + '.txt'
			
			# Write data to file
			saveFile = open(saveFileName, "w")
			saveFile.write("%s" % dataFileString)
			saveFile.close()
	
	# Delete the old scrape files once we have achieved fresh data
	if cacheTmxMovers is True and len(oldDataFileList) > 0:
		for oldDataFile in oldDataFileList:
			if os.path.exists(scrapeDataDir + oldDataFile):
				os.remove(scrapeDataDir + oldDataFile)
		
	# Convert string to JSON
	requestDataJson = json.loads(dataFileString)
	
	# Get the parsed stock data structure
	symbolData, featuredData = populate_volume_mover_symbol_data(requestDataJson, 2)
		
	# Write each queried symbol to the database
	dbData = ""
	errData = ""
	for symbolDataItem in symbolData:
		dbData, errData = db.update_ticker_data(symbolDataItem)
	
	if len(dbData) > 0:
		returnData += dbData + "\n"
	
	if len(errData) > 0:
		errorData += errData + "\n"

	# Write each featured stock structure to the database
	dbData = ""
	errData = ""
	for featuredDataItem in featuredData:
		dbData, errData = db.update_featured_stock_data(featuredDataItem)
	
	if len(dbData) > 0:
		returnData += dbData + "\n"
	
	if len(errData) > 0:
		errorData += errData + "\n"
	
	if len(errorData) > 0:
		with open("thread_error_log.txt", 'a') as threadErrorFile:
			threadErrorFile.write("TMX TSXV VolMover Thread Done: " + currDT + "\n")
			threadErrorFile.write("Error: " + errorData + "\n")
	
	return returnData, errorData
	
def populate_volume_mover_symbol_data(requestDataJson, featureType):
	# Build the list of individual stock dictionaries
	symbolData = []
	featureData = []
	orderValue = 1

	# Parse the quote data into our struct
	for quoteItem in requestDataJson['quote']: # get each stock
		symbolDict = {}
		featureDict = {}
		for quoteKey, quoteItemDict in quoteItem.items(): 
			if quoteKey == "key":	
				for keyDictKey, keyDictVal in quoteItemDict.items(): #get inside "key"
					if keyDictKey == "symbol":
						symbolDict["ticker"] = keyDictVal
					if keyDictKey == "exShName":
						symbolDict["exchange_name"] = keyDictVal
			elif quoteKey == "equityinfo":
				for eqInfoKey, eqInfoVal in quoteItemDict.items(): #get inside equityinfo
					if eqInfoKey == "longname":
						symbolDict["company_name"] = eqInfoVal
			elif quoteKey == "pricedata": #get inside "pricedata"
				for priceInfoKey, priceInfoVal in quoteItemDict.items(): #get inside pricedata
					if priceInfoKey == "last":
						symbolDict["last_price"] = priceInfoVal
					elif priceInfoKey == "change":
						symbolDict["change_amount"] = priceInfoVal
					elif priceInfoKey == "changepercent":
						symbolDict["change_percent"] = priceInfoVal[:-1] # remove the % character
					elif priceInfoKey == "sharevolume":
						symbolDict["share_volume"] = priceInfoVal
			elif quoteKey == "fundamental":	#get inside "fundamental"
				for fundInfoKey, fundInfoVal in quoteItemDict.items():
					if fundInfoKey == "week52high":
						symbolDict["fiftytwo_week_high"] = fundInfoVal
					elif fundInfoKey == "week52low":
						symbolDict["fiftytwo_week_low"] = fundInfoVal
		# Update the ticker format to match API
		if symbolDict["ticker"][-3:] == ":CA":
			symbolStringNoEx = (symbolDict["ticker"][:-3]).replace(".", "-")
			# Add the appropriate exchange suffix based on the exchange queried
			if symbolDict["exchange_name"] == "TSX":
				symbolDict["ticker"] =  symbolStringNoEx + ".TO"
			elif symbolDict["exchange_name"] == "TSXV":
				symbolDict["ticker"] =  symbolStringNoEx + ".V"
		# Get rid of commas in the volume number
		if "share_volume" in symbolDict.keys():
			symbolDict["share_volume"] = symbolDict["share_volume"].replace(",", "")
		# Build featured data object for the ticker
		featureDict["ticker_ref"] = symbolDict["ticker"]
		featureDict["feature_type"] = featureType
		featureDict["order_value"] = orderValue

		symbolData.append(symbolDict)
		featureData.append(featureDict)

		orderValue += 1
		
	return symbolData, featureData