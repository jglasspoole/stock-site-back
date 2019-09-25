import requests
import json
import importlib
import configparser
from datetime import datetime
import os
from os import listdir
from os.path import isfile, join

db = importlib.import_module("helpers.db.db")

alpha_config = configparser.ConfigParser()
alpha_config.read('alpha_config.ini')

# API URL
avBaseUrl = 'https://www.alphavantage.co/'

# API key
avApiKey = alpha_config["alphavantageAPI"]["apiKey"]

# EXAMPLE QUERY
# https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=" + tickerSymbol + "&outputsize=full&apikey=" + QUOTE_API_KEY;

# Test data dir
testDataDir = 'data/'

def getTickerDailyData(tickerSymbol):
	returnData = ''
	errorString = ''
	if len(tickerSymbol) > 0:
		
		# First check if there is a cache version of this data on record
		dataDirFiles = [f for f in listdir(testDataDir) if isfile(join(testDataDir, f))]
		
		tickerFileStub = tickerSymbol.replace('.', '-')
		
		currDate = datetime.now().strftime('%Y-%m-%d')
		todayFileSubstring = tickerFileStub + '-' + currDate 
	
		# If the data for the current day exists, fetch the file name, keep a list of old files
		todayFullFileName = ''
		oldDataFileList = []
		for dataFile in dataDirFiles:
			if todayFileSubstring in dataFile:
				todayFullFileName = dataFile
			elif tickerFileStub in dataFile:
				oldDataFileList.append(dataFile)
			
		dataFileString = ''
		requestDataJson = None
		
		if len(todayFullFileName) > 0:
			# Retrieve the data we already queried and saved to today's file
			todayFileData = open(testDataDir + todayFullFileName, "r")
			dataFileString = todayFileData.read()
			requestDataJson = json.loads(dataFileString)
			
		else:
			currDT = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
			currDTIsoToMinute = datetime.now().strftime('%Y-%m-%d %H:%M:00')
			
			# Get the status of the API
			apiDataRow, errorData = db.get_alphavantage_date_stats()
			
			apiUpdateRow = {}
			
			if apiDataRow is not None: # updating an existing row for the date
				tooManyCallsThisMinute = False
				tooManyCallsToday = False
				apiMaxHitsPerMin = 4
				apiMaxHitsPerDay = 450
				
				apiCurrHitsThisMinute = 1
				apiCurrCallsToday = 1

				# Check if this is more than 1 API hit on the current minute so far
				if str(apiDataRow["last_call_minute"]) == currDTIsoToMinute:
					apiPreviousCallsThisMinute = int(apiDataRow["last_minute_count"])
					if apiPreviousCallsThisMinute >= apiMaxHitsPerMin: # Prevent too many calls on this minute
						tooManyCallsThisMinute = True
					else:
						apiCurrHitsThisMinute += apiPreviousCallsThisMinute
				
				# Check if there are too many calls for the day
				if str(apiDataRow["log_date"]) == currDate:
					apiPreviousCallsToday = int(apiDataRow["date_total"])
					if apiPreviousCallsToday >= apiMaxHitsPerDay: # Prevent too many calls on this day
						tooManyCallsToday = True
					else:
						apiCurrCallsToday += apiPreviousCallsToday
				
				apiUpdateRow["id"] = apiDataRow["id"]
				apiUpdateRow["log_date"] = apiDataRow["log_date"]
				apiUpdateRow["date_total"] = apiCurrCallsToday
				apiUpdateRow["last_call_minute"] = currDTIsoToMinute
				apiUpdateRow["last_minute_count"] = apiCurrHitsThisMinute
				
				# Handle over use of the API per minute or per day
				if tooManyCallsThisMinute == True:
					with open("alpha_api_overuse_log.txt", 'a') as overuseLogFile:
						overuseLogFile.write("Too many API requests for minute: " + currDTIsoToMinute + "\n")
					errorString += "Too many API Requests for Minute: " + currDTIsoToMinute + "<br />"
					return returnData, errorString
					
				if tooManyCallsToday == True:
					with open("alpha_api_overuse_log.txt", 'a') as overuseLogFile:
						overuseLogFile.write("Too many API requests for date: " + currDate + "\n")
					errorString += "Too many API Requests for Date: " + currDate + "<br />"
					return returnData, errorString
				
			else: # new row for the date
				apiUpdateRow["id"] = 0
				apiUpdateRow["log_date"] = currDate
				apiUpdateRow["date_total"] = 1
				apiUpdateRow["last_call_minute"] = currDTIsoToMinute
				apiUpdateRow["last_minute_count"] = 1
			
			dbData, errorData = db.update_alphavantage_date_stats(apiUpdateRow)
		
			# Use the Quote API to get and save new data
			avQueryUrl = avBaseUrl + "query"
			avQueryParams = {'function' : 'TIME_SERIES_DAILY_ADJUSTED', 'symbol' : tickerSymbol, 'outputsize' : 'full', 'apikey' : avApiKey}
			apiRequest = requests.get(avQueryUrl, avQueryParams)
			requestDataJson = apiRequest.json()
			
			# TODO validate the return from Quote API - see: alpha_api_error_ticker_sample for error ticker msg
			
			# Always use double quotes
			dataFileString = str(requestDataJson).replace("\'", "\"")
			saveFileName = testDataDir + tickerFileStub + '-' + currDT + '.txt'
			
			with open(saveFileName, 'w') as outfile:
				json.dump(requestDataJson, outfile)
	
		# TEST DATA 
		info = (dataFileString[:300] + '..') if len(dataFileString) > 300 else dataFileString
		returnData = 'old files: ' + str(oldDataFileList) + '<br />' + 'today file: ' + todayFullFileName + '<br />' + 'sample info: ' + info + '<br />'
	
		# Delete the old data files once we have achieved fresh data
		if len(oldDataFileList) > 0:
			for oldDataFile in oldDataFileList:
				if os.path.exists(testDataDir + oldDataFile):
					os.remove(testDataDir + oldDataFile)
					
		# Extract exchange name from known ticker suffixes
		exchangeName = ""
		if tickerSymbol[-3:] == ".TO":
			exchangeName = "TSX"
		elif tickerSymbol[-2:] == ".V":
			exchangeName = "TSXV"
			
		stockDataDict = {"ticker" : tickerSymbol, "exchange_name" : exchangeName, "company_name" : "", "last_price" : "", "change_amount" : "", "change_percent" : "", "fiftytwo_week_high" : "", "fiftytwo_week_low" : ""}

		currentPrice = ""
		lastClose = ""
		for dateDict in requestDataJson['Time Series (Daily)'].items():
			#for itemKey, itemVal in dateDict.items(): <-- this used to work...
			for itemKey, itemVal in dateDict[1].items():
				if itemKey == "4. close" and len(currentPrice) == 0:
					currentPrice = itemVal
					break
				elif itemKey == "4. close":
					lastClose = itemVal
					break
					
			if len(lastClose) > 0:
				break

		stockDataDict["last_price"] = currentPrice
		if len(lastClose) > 0:
			stockDataDict["change_percent"] = str(((float(currentPrice) / float(lastClose)) - 1.0) * 100.0)
			stockDataDict["change_amount"] = str(float(currentPrice) - float(lastClose))
			
		dbData, errorData = db.update_ticker_data(stockDataDict, dataFileString)
		
		if len(dbData) > 0:
			returnData += dbData + '<br />'
		
		if len(errorData) > 0:
			errorString += errorData + '<br />'
		
	else:
		errorString = 'Please provide a ticker symbol.'
	
	return returnData, errorString

