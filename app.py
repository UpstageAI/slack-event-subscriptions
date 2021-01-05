from flask import Flask
from slackeventsapi import SlackEventAdapter
import os

from db import put_event, put_attendance

# This `app` represents your existing Flask app
app = Flask(__name__)

# An example of one of your Flask app's routes
@app.route("/")
def hello():
  return "Hello there!"

# Bind the Events API route to your existing Flask app by passing the server
# instance as the last param, or with `server=app`.
SLACK_SIGNING_SECRET = os.environ["SLACK_SIGNING_SECRET"]
slack_events_adapter = SlackEventAdapter(SLACK_SIGNING_SECRET, "/slack/events", app)

# Example responder to greetings
@slack_events_adapter.on("message")
def handle_message(event_data):
    message = event_data["event"]
    channel = message["channel"]
    user = message["user"]
    print(str(message) + " at " + channel + " by " + user)

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