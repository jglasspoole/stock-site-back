import importlib
from datetime import datetime

# import the helpers
server = importlib.import_module("helpers.server")
threads = importlib.import_module("helpers.threads")

if __name__ == "__main__":
	threads.run_scrape_thread()
	server.run_server()