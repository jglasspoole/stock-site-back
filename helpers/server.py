from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import importlib

app = Flask(__name__)
CORS(app)

templates = importlib.import_module("helpers.templates")

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
@app.route("/get_quote")
def get_quote():
	
	tickerParam = request.args['ticker']
	renderString, errorString = templates.get_quote(tickerParam)
	
	if not errorString:
		return renderString
	else:
		return errorString
		
"""	
@app.route("/about")
def about():
	return render_template('about.html')
"""

