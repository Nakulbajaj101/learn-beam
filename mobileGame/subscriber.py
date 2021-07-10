from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time

# TODO(developer)
project_id = "bridge-data-analytics-app"
subscription_id = "Subscribe2"
# Number of seconds the subscriber should listen for messages
timeout = 60

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

def callback(message):
    print("Received {}".format(message.data.decode('utf-8')))
    message.ack()

subsciption = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")


# Wrap subscriber in a 'with' block to automatically call close() when done.
try:
    subsciption.result()
except KeyboardInterrupt:
    subsciption.cancel()