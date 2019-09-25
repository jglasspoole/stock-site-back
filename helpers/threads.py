import threading
import importlib
import random
import time
import json
from datetime import datetime
import signal
import sys

tmxScraper = importlib.import_module("helpers.scrape_threads.tmx_scrape")
tradingViewScraper = importlib.import_module("helpers.scrape_threads.tradingview_scrape")

tmxScrapeThread = None
tradingViewScrapeThread = None

def run_scrape_thread():

	# Setup the thread kill method to stop the scraping threads
	def signal_handler(sig, frame):
		if tmxScrapeThread is not None:
			tmxScrapeThread.do_run = False
		if tradingViewScrapeThread is not None:
			tradingViewScrapeThread.do_run = False
		sys.exit(0)
		
	signal.signal(signal.SIGINT, signal_handler)

	# Start TMX scrape looping thread
	tmxScrapeThread = threading.Thread(target=thread_method_tmx_scrape)
	tmxScrapeThread.do_run = True
	tmxScrapeThread.start()

	# Append to scrape log
	with open("scrape_log.txt", 'a') as scrapeLogFile:
		currDT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		scrapeLogFile.write("Started TMX Scrape: " + currDT + "\n")
	
	tradingViewScrapeThread = threading.Thread(target=thread_method_tradingview_scrape)
	tradingViewScrapeThread.do_run = True
	tradingViewScrapeThread.start()

	# Append to scrape log
	with open("scrape_log.txt", 'a') as scrapeLogFile:
		currDT = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		scrapeLogFile.write("Started TradingView Scrape: " + currDT + "\n")

def thread_method_tmx_scrape():
	returnData = ""
	errorData = ""
	
	currThread = threading.currentThread()
	scrapeExchange = "TSX"
	
	# Looping thread, scrape TSX, then TSXV for volume movers
	while getattr(currThread, "do_run", True):
		dbData = ""
		errData = ""
		
		if scrapeExchange == "TSX":
			dbData, errData = tmxScraper.run_tmx_tsx_scrape()
			scrapeExchange = "TSXV" # switch to TSXV exchange for next loop
		elif scrapeExchange == "TSXV":
			dbData, errData = tmxScraper.run_tmx_tsxv_scrape()
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

def thread_method_tradingview_scrape():
	returnData = ""
	errorData = ""
	
	# TODO! Make part of the tmx scrape thread as may have conflicting access to same row of DB
	currThread = threading.currentThread()
	
	# Looping thread, scrape TSX, then TSXV for volume movers
	while getattr(currThread, "do_run", True):
		dbData = ""
		errData = ""
		
		dbData, errData = tradingViewScraper.run_tv_52wh_scrape()
			
		if len(dbData) > 0:
			returnData += dbData + "\n"
			
		if len(errData) > 0:
			errorData += errData + "\n"
			
		# Wait for a period of time until next loop iteration 

		# PRODUCTION
		secsToWait = 178 + random.randint(3,63)

		# TESTING 
		# secsToWait = 20 + random.randint(5, 15)

		secsWaited = 0
		while secsWaited < secsToWait and getattr(currThread, "do_run", True):
			time.sleep(1)
			secsWaited += 1
		
	return returnData, errorData
