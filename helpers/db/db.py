import psycopg2
import importlib
import configparser
from datetime import datetime

db_config = configparser.ConfigParser()
db_config.read('db_config.ini')

def get_db_connection():
	conn = None
	errorString = None
	try:
		# connect to the postgresql database
		# build the db connection string
		# TODO... set port num?
		dbConnectString = 'host=' + db_config['postgresDB']['host'] + ' dbname=' + db_config['postgresDB']['db'] + ' user=' + db_config['postgresDB']['user'] + ' password=' + db_config['postgresDB']['pass'] 
		conn = psycopg2.connect(dbConnectString)
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString = str(error) + " db connection error" + dbConnectString
	
	return conn, errorString
	
def update_ticker_data(stockDataDict, tradeHistory = "", priceUpdateDTObj = None):
	printString = ''
	errorString = ''
	conn = None
	
	if not "ticker" in stockDataDict.keys():
		errorString += "Please provide the ticker field to be updated."
	if len(stockDataDict["ticker"]) == 0:
		errorString += "Please provide a ticker to update."
	
	if len(errorString) > 0:
		return "", errorString

	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()
			
			# Data Sample:
			# stockDataDict = {"ticker" : tickerSymbol, "exchange_name" : "", "company_name" : "", "last_price" : "", "change_amount" : "", "change_percent" : "", 
			# 								"share_volume" : "", "fiftytwo_week_high" : "", "fiftytwo_week_low" : ""}
			
			# Get the existing row for this ticker if it exists
			sqlString = "SELECT ticker, last_price, EXTRACT(epoch FROM price_updated AT TIME ZONE 'America/New_York'), history_updated "
			sqlString += "FROM stock_information WHERE ticker = E'" + stockDataDict["ticker"] + "';"
			cur.execute(sqlString)
			fetchedTickerRow = cur.fetchone()

			fieldsUpdated = 0

			if fetchedTickerRow is None:
				# We don't have a pre-existing row for this ticker, assume if a price time is passed that's the time we use for price data
				priceUpdateDT = ""
				if priceUpdateDTObj is not None:
					priceUpdateDT = priceUpdateDTObj.strftime('%Y-%m-%d %H:%M:%S')

				fieldsUpdated = 1
				# Build the SQL insert statement
				sqlString = "INSERT INTO stock_information (ticker"
				if "exchange_name" in stockDataDict.keys() and len(stockDataDict["exchange_name"]) > 0:
					sqlString += ", exchange_name"
				if "company_name" in stockDataDict.keys() and len(stockDataDict["company_name"]) > 0:
					sqlString += ", company_name"
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					sqlString += ", last_price"
				if "change_amount" in stockDataDict.keys() and len(stockDataDict["change_amount"]) > 0:
					sqlString += ", change_amount"
				if "change_percent" in stockDataDict.keys() and len(stockDataDict["change_percent"]) > 0:
					sqlString += ", change_percent"
				if "share_volume" in stockDataDict.keys() and len(stockDataDict["share_volume"]) > 0:
					sqlString += ", share_volume"
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					sqlString += ", price_updated"
				if len(tradeHistory) > 0 :
					sqlString += ", trade_history, history_updated"
				sqlString += ", created) "
				
				sqlString += "VALUES (E'" + stockDataDict["ticker"] + "'"
				if "exchange_name" in stockDataDict.keys() and len(stockDataDict["exchange_name"]) > 0:
					sqlString += ", E'" + stockDataDict["exchange_name"] + "'"
				if "company_name" in stockDataDict.keys() and len(stockDataDict["company_name"]) > 0:
					sqlString += ", E'" + stockDataDict["company_name"] + "'"
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					sqlString += ", '" + stockDataDict["last_price"] + "'"
				if "change_amount" in stockDataDict.keys() and len(stockDataDict["change_amount"]) > 0:
					sqlString += ", '" + stockDataDict["change_amount"] + "'"
				if "change_percent" in stockDataDict.keys() and len(stockDataDict["change_percent"]) > 0:
					sqlString += ", '" + stockDataDict["change_percent"] + "'"
				if "share_volume" in stockDataDict.keys() and len(stockDataDict["share_volume"]) > 0:
					sqlString += ", '" + stockDataDict["share_volume"] + "'"
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					if len(priceUpdateDT) > 0: # If a price update time has been provided (accounting for delay)
						sqlString += ", TIMESTAMP '" + priceUpdateDT + "'"
					else:
						sqlString += ", CURRENT_TIMESTAMP"
				if len(tradeHistory) > 0 :
					sqlString += ", E'" + tradeHistory + "', CURRENT_TIMESTAMP"
				sqlString += ", CURRENT_TIMESTAMP)"
			else:
				# We need to check that the price being updated data is not from an earlier quote time from when it was last updated
				priceUpdatedEpoch = int(fetchedTickerRow[2])
				lastUpdatedDTObj = datetime.fromtimestamp(priceUpdatedEpoch)

				# If a specified update data datetime is given, make sure its more recent than existing price data
				priceUpdateDT = ""
				ignorePriceUpdate = False
				if priceUpdateDTObj is not None:
					print(stockDataDict["ticker"] + " LAST UPDATED IN TABLE: " + lastUpdatedDTObj.strftime('%Y-%m-%d %H:%M:%S'))
					print(stockDataDict["ticker"] + " ATTEMPTING TO BE UPDATED WITH PRICE TIME: " + priceUpdateDTObj.strftime('%Y-%m-%d %H:%M:%S'))
					if priceUpdateDTObj > lastUpdatedDTObj:
						priceUpdateDT = priceUpdateDTObj.strftime('%Y-%m-%d %H:%M:%S')
					else:
						ignorePriceUpdate = True

				# Build the SQL update statement
				sqlString = "UPDATE stock_information SET "
				if "exchange_name" in stockDataDict.keys() and len(stockDataDict["exchange_name"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "exchange_name = E'" + stockDataDict["exchange_name"] + "'"
					fieldsUpdated += 1
				if "company_name" in stockDataDict.keys() and len(stockDataDict["company_name"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "company_name = E'" + stockDataDict["company_name"] + "'"
					fieldsUpdated += 1
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0 and ignorePriceUpdate is False:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "last_price = '" + stockDataDict["last_price"] + "'"
					fieldsUpdated += 1
				if "change_amount" in stockDataDict.keys() and len(stockDataDict["change_amount"]) > 0 and ignorePriceUpdate is False:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "change_amount = '" + stockDataDict["change_amount"] + "'"
					fieldsUpdated += 1
				if "change_percent" in stockDataDict.keys() and len(stockDataDict["change_percent"]) > 0 and ignorePriceUpdate is False:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "change_percent = '" + stockDataDict["change_percent"] + "'"
					fieldsUpdated += 1
				if "share_volume" in stockDataDict.keys() and len(stockDataDict["share_volume"]) > 0 and ignorePriceUpdate is False:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "share_volume = '" + stockDataDict["share_volume"] + "'"
					fieldsUpdated += 1
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0 and ignorePriceUpdate is False:
					if fieldsUpdated > 0:
						sqlString += ", "
					if len(priceUpdateDT) > 0: # If a price update time has been provided (accounting for delay)
						sqlString += "price_updated = TIMESTAMP '" + priceUpdateDT + "'"
					else:
						sqlString += "price_updated = CURRENT_TIMESTAMP"
					fieldsUpdated += 1
				if len(tradeHistory) > 0 :
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "trade_history = E'" + tradeHistory + "', history_updated = CURRENT_TIMESTAMP"
					fieldsUpdated += 1
				sqlString += " WHERE ticker = E'" + stockDataDict["ticker"] + "'"
				
			sqlString += ";"
				
			# If there is something to be updated 
			if fieldsUpdated > 0:
				cur.execute(sqlString)
	
			# commit the changes
			conn.commit()
			
			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return printString, errorString

def get_featured_stock_data(featureType):
	featuredStockData = None
	errorString = ''

	# Validate the featureType field
	if not (featureType == 1 or featureType == 2):
		errorString += "Please provide a valid featureType field."
	
	if len(errorString) > 0:
		return None, errorString

	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()

			sqlString = "SELECT si.ticker, si.exchange_name, si.company_name, si.last_price, si.change_amount, "
			sqlString += "si.change_percent, si.share_volume, si.price_updated, fs.order_value "
			sqlString += "FROM stock_information si "
			sqlString += "JOIN featured_stocks fs "
			sqlString += "ON si.ticker = fs.ticker_ref "
			sqlString += "WHERE fs.feature_type = " + str(featureType) + " "
			sqlString += "ORDER BY fs.order_value ASC;"

			cur.execute(sqlString)
			allFeaturedStocks = cur.fetchall()

			# Build the featured stock object array
			
			featuredStockData = { "featured_data" : None }
			stockDataInfoArray = []
			for featuredStock in allFeaturedStocks:
				featuredStockDict = {}
				featuredStockDict["ticker"] = featuredStock[0]
				featuredStockDict["exchange_name"] = featuredStock[1]
				featuredStockDict["company_name"] = featuredStock[2]
				featuredStockDict["last_price"] = str(featuredStock[3])
				featuredStockDict["change_amount"] = str(featuredStock[4])
				featuredStockDict["change_percent"] = str(featuredStock[5])
				featuredStockDict["share_volume"] = str(featuredStock[6])
				featuredStockDict["price_updated"] = featuredStock[7]
				featuredStockDict["order_value"] = str(featuredStock[8])
				stockDataInfoArray.append(featuredStockDict)

			featuredStockData["featured_data"] = stockDataInfoArray

			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return featuredStockData, errorString

def update_featured_stock_data(featuredStockDataDict):
	printString = ''
	errorString = ''
	conn = None
	
	if not "ticker_ref" in featuredStockDataDict.keys():
		errorString += "Please provide the featured ticker field to be updated."
	if len(featuredStockDataDict["ticker_ref"]) == 0:
		errorString += "Please provide a featured ticker to update."
	
	if len(errorString) > 0:
		return "", errorString

	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()
			
			# Data Sample:
			# featuredStockDataDict = {"ticker_ref" : "", "feature_type" : "", "order_value" : ""}
			
			# Get the existing row for this featured stock if it exists
			
			sqlString = "SELECT id, ticker_ref, feature_type, order_value, last_updated FROM featured_stocks "
			sqlString += "WHERE feature_type = '" + str(featuredStockDataDict["feature_type"]) + "' AND "
			sqlString += "order_value = '" + str(featuredStockDataDict["order_value"]) + "';"
			cur.execute(sqlString)
			fetchedTickerRow = cur.fetchone()
			
			if fetchedTickerRow is None:
				# Build the SQL insert statement
				sqlString = "INSERT INTO featured_stocks (ticker_ref, feature_type, order_value, last_updated) "
				sqlString += "VALUES (E'" + featuredStockDataDict["ticker_ref"] + "'"
				sqlString += ", '" + str(featuredStockDataDict["feature_type"]) + "'"
				sqlString += ", '" + str(featuredStockDataDict["order_value"]) + "'"
				sqlString += ", CURRENT_TIMESTAMP)"
			else:
				# Build the SQL update statement
				sqlString = "UPDATE featured_stocks SET "
				sqlString += "ticker_ref = E'" + featuredStockDataDict["ticker_ref"] + "' "
				sqlString += "WHERE feature_type = '" + str(featuredStockDataDict["feature_type"]) + "' AND "
				sqlString += "order_value = '" + str(featuredStockDataDict["order_value"]) + "'"
		
			sqlString += ";"
			
			cur.execute(sqlString)
	
			# commit the changes
			conn.commit()
			
			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return printString, errorString
	
def get_alphavantage_date_stats():
	fetchedApiObj = None
	errorString = ''

	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()
			
			currDate = datetime.now().strftime('%Y-%m-%d')

			sqlString = "SELECT id, log_date, date_total, last_call_minute, last_minute_count FROM alphavantage_api_log WHERE log_date = '" + currDate + "';"
			cur.execute(sqlString)
			fetchedApiLogRow = cur.fetchone()
			
			fetchedApiObj = {"id" : fetchedApiLogRow[0], "log_date" : fetchedApiLogRow[1].strftime('%Y-%m-%d'), "date_total" : fetchedApiLogRow[2], 
                        "last_call_minute" : fetchedApiLogRow[3].strftime('%Y-%m-%d %H:%M:00'), "last_minute_count" : fetchedApiLogRow[4] }
			
			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return fetchedApiObj, errorString
	
def update_alphavantage_date_stats(apiUpdateRow):
	printData = ''
	errorString = ''

	if not "id" in apiUpdateRow.keys():
		errorString += "Please provide a valid API row to update."
	
	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()
			
			# Data Sample:
			# apiUpdateRow = {"id" : 0, "log_date" : "", "date_total" : "", "last_call_minute" : "", "last_minute_count" : ""}
			
			# Get the existing row for this ticker if it exists
			if apiUpdateRow["id"] == 0:
				# Build the SQL insert statement
				sqlString = "INSERT INTO alphavantage_api_log (log_date, date_total, last_call_minute, last_minute_count) "
				sqlString += "VALUES ('" + apiUpdateRow["log_date"] + "', '" + str(apiUpdateRow["date_total"]) + "'"
				sqlString += ", TIMESTAMP '" + apiUpdateRow["last_call_minute"] + "', '" + str(apiUpdateRow["last_minute_count"]) + "')"
			else:
				# Build the SQL update statement
				
				sqlString = "UPDATE alphavantage_api_log SET date_total = '" + str(apiUpdateRow["date_total"]) + "', "
				sqlString += "last_call_minute = TIMESTAMP '" + apiUpdateRow["last_call_minute"] + "', last_minute_count = '" + str(apiUpdateRow["last_minute_count"]) + "' "
				sqlString += "WHERE id = '" + str(apiUpdateRow["id"]) + "'"
				
			sqlString += ";"
			
			cur.execute(sqlString)
			
			# commit the changes
			conn.commit()
			
			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return printData, errorString

def user_list():
	printString = ''
	errorString = ''
	conn = None
	
	try:
		conn, err = get_db_connection()
		
		if not err:
			# create a cursor
			cur = conn.cursor()
			
			# build SQL statement
			
			cur.execute('SELECT * FROM users')
			
			all_users = cur.fetchall()
			for usr in all_users:
				printString += str(usr) + '<br />'
			
			# close the communication
			cur.close()
		else:
			errorString += err
		
	except (Exception, psycopg2.DatabaseError) as error:
		errorString += str(error)
	finally:
		if conn is not None:
			conn.close()
	
	return printString, errorString