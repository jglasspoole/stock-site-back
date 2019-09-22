from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import importlib
from datetime import datetime

app = Flask(__name__)
CORS(app)

# import the helpers
templates = importlib.import_module("helpers.templates")
threads = importlib.import_module("helpers.threads")

@app.route("/")
def main():
	renderString = "This is the home page"
	errorString = ""
	
	if not errorString:
		return renderString
	else:
		return errorString

# Fetch data for given ticker using alphavantage API
@app.route("/get_quote")
def get_quote():
	
	tickerParam = request.args['ticker']
	renderString, errorString = templates.get_quote(tickerParam)
	
	if not errorString:
		return renderString
	else:
		return errorString
		
# Sample scrape of trading view data
@app.route("/get_scrape")
def get_scrape():
	renderString, errorString = templates.get_scrape()
	
	if not errorString:
		return jsonify(renderString)
	else:
		return errorString
		
"""	
@app.route("/about")
def about():
	return render_template('about.html')
"""

if __name__ == "__main__":
	threads.run_scrape_thread()
	# app.run(debug=True, host="0.0.0.0", port=9090)
	app.run(debug=True, use_reloader=False, host="0.0.0.0", port=9090)
	