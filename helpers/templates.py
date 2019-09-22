import psycopg2
import importlib

# import the quote helper
quote = importlib.import_module("helpers.quote")

# import the scrape helper
scrape = importlib.import_module("helpers.scrape")
	
def get_quote(tickerParam):
	printString = ''
	errorString = ''
	
	# Validate the ticker input
	if len(tickerParam) is 0:
		errorString = 'Please enter a ticker.'
	
	# Only want to work with '-' characters to split and rebuild in our known format
	tickerParam = tickerParam.replace('.', '-')
	
	tickerSplitParts = None
	if not errorString:
		tickerSplitParts = tickerParam.split('-')
		
		# Account for tickers like MSFT, RY-TO, ALEF-WT-TO, KLY-WT-B-V
		if len(tickerSplitParts) > 4:
			errorString = 'Please enter a valid ticker symbol.'
		
	# Required format example: ALEF-WT.TO
	tickerQuerySymbol = ''
	if not errorString:
		tickerSeparator = '-'
		
		# Account for non american tickers (canadian) that have exchange suffix
		# TODO watch for american tickers with a "-" separator
		if len(tickerSplitParts) > 1:
			# Build the ticker base before exchange symbol
			for tickerPartIndex in range(len(tickerSplitParts) - 1):
				if tickerPartIndex > 0:
					tickerQuerySymbol += tickerSeparator			
				tickerQuerySymbol += tickerSplitParts[tickerPartIndex]
			
			# Add the exchange symbol
			tickerQuerySymbol += '.' + tickerSplitParts[len(tickerSplitParts) - 1]
		else:
			tickerQuerySymbol = tickerParam 
		
		printString += tickerQuerySymbol + '<br />'

		requestData, errorString = quote.getTickerDailyData(tickerQuerySymbol)
		if not errorString:
			printString += str(requestData)
	
	return printString, errorString
	
def get_scrape():
	printString = ''
	errorString = ''
	
	printString, errorString = scrape.scrape_tradingview()
		
	return printString, errorString