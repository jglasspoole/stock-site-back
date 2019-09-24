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
	
def update_ticker_data(stockDataDict, tradeHistory = ""):
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
			# 					"share_volume" : "", "fiftytwo_week_high" : "", "fiftytwo_week_low" : ""}
			
			# Get the existing row for this ticker if it exists
			
			sqlString = "SELECT ticker, last_price, price_updated, history_updated FROM stock_information WHERE ticker = E'" + stockDataDict["ticker"] + "';"
			cur.execute(sqlString)
			fetchedTickerRow = cur.fetchone()
			fieldsUpdated = 0
			
			if fetchedTickerRow is None:
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
					sqlString += ", CURRENT_TIMESTAMP"
				if len(tradeHistory) > 0 :
					sqlString += ", E'" + tradeHistory + "', CURRENT_TIMESTAMP"
				sqlString += ", CURRENT_TIMESTAMP)"
			else:
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
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "last_price = '" + stockDataDict["last_price"] + "'"
					fieldsUpdated += 1
				if "change_amount" in stockDataDict.keys() and len(stockDataDict["change_amount"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "change_amount = '" + stockDataDict["change_amount"] + "'"
					fieldsUpdated += 1
				if "change_percent" in stockDataDict.keys() and len(stockDataDict["change_percent"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "change_percent = '" + stockDataDict["change_percent"] + "'"
					fieldsUpdated += 1
				if "share_volume" in stockDataDict.keys() and len(stockDataDict["share_volume"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
					sqlString += "share_volume = '" + stockDataDict["share_volume"] + "'"
					fieldsUpdated += 1
				if "last_price" in stockDataDict.keys() and len(stockDataDict["last_price"]) > 0:
					if fieldsUpdated > 0:
						sqlString += ", "
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