from google.cloud import pubsub_v1
from time import sleep

# TODO(developer)
project_id = "bridge-data-analytics-app"
topic_id = "Topic1"


file_name = "store_sales.csv"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = f"projects/{project_id}/topics/{topic_id}"

with open(file=file_name, mode='rb') as salesfile:
    header = salesfile.readline()
    print("header for the data is filtered which is : {}".format(header.strip()))
    for line in salesfile:
        published = publisher.publish(topic_path,line)
        print(published.result())
        sleep(1)


