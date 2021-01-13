from flask import Flask
from flask import make_response
from flask import request

from slackeventsapi import SlackEventAdapter
import os


from db import init_db, put_event, put_attendance, put_msg
from db2csv import db2csv

# This `app` represents your existing Flask app
app = Flask(__name__)

init_db()

@app.route("/")
def hello():
  return "Hello there!"

# get csv
@app.route("/csv")
def csv():
  query_date = request.args.get('d')
  sorting_key = request.args.get('s')
  output = make_response(db2csv(query_date, sorting_key))
  #output.headers["Content-Disposition"] = "attachment; filename=export.csv"
  #output.headers["Content-type"] = "text/csv"
  output.headers["Content-type"] = "text/plain; charset=utf-8"
  return output
  
# Bind the Events API route to your existing Flask app by passing the server
# instance as the last param, or with `server=app`.
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
slack_events_adapter = SlackEventAdapter(SLACK_SIGNING_SECRET, "/slack/events", app)

# Example responder to greetings
@slack_events_adapter.on("message")
def handle_message(event_data):
    message = event_data["event"]
    # print(message)
    # put_msg(message)

# Create an event listener for "reaction_added" events and print the emoji name
@slack_events_adapter.on("reaction_added")
def reaction_added(event_data):
  event = event_data['event']
  print(event)
  print(put_event(event))
  put_attendance(event)

# Start the server on port 3000
if __name__ == "__main__":
  app.run(port=3000)