from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import importlib

app = Flask(__name__)
#app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
CORS(app)

# import server request handlers methods
request_handlers = importlib.import_module("helpers.request_handlers")

def run_server():
  # app.run(debug=True, host="0.0.0.0", port=9090)
	app.run(debug=True, use_reloader=False, host="0.0.0.0", port=9090)

@app.route("/")
def main():
	renderString = "This is the home page"
	errorString = ""
	
	if not errorString:
		return renderString
	else:
		return errorString

# Fetch data for given ticker using alphavantage API
# ticker - parameter passed
@app.route("/get_quote")
def get_quote():
	
	tickerParam = request.args['ticker']
	renderString, errorString = request_handlers.get_quote(tickerParam)
	
	if not errorString:
		return renderString
	else:
		return errorString

# Fetch featured volume movers from database and return as JSON
# exchange - paramater passed, valid: tsx, tsxv
@app.route("/get_volume_movers")
def get_volume_movers():
	
	exchangeParam = request.args['exchange']
	featureData, errorString = request_handlers.get_volume_movers(exchangeParam)
	
	if not errorString:
		return jsonify(**featureData)
	else:
		return errorString
		
"""	
Sample HTML file only template
@app.route("/about")
def about():
	return render_template('about.html')
"""

