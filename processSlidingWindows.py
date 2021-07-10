import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window


project_id = 'bridge-data-analytics-app'
subscription_id = 'Subscribe1'
output_topic_id = 'Topic2'

input_subscription = f'projects/{project_id}/subscriptions/{subscription_id}'
output_topic = f'projects/{project_id}/topics/{output_topic_id}'


known_args = {
    'runner:': 'DirectRunner',
    'streaming': True,
}

options = PipelineOptions(flags=[], **known_args)

def clean_data(element):
    return (element.decode('utf-8').strip())

def calculate_profit(element):
    buy_rate = int(element[5])
    sell_price = int(element[6])
    sold = int(element[4])
    profit = (sell_price*sold) - (buy_rate*sold)
    element.append(profit)
    return element


p = beam.Pipeline(options=options)

pubsubData = (
    p
    | "Read pubsub data" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | "Clean and encode byte string" >> beam.Map(clean_data)
    | "Split data" >> beam.Map(lambda element: element.split(','))
    | "Filter Mumbai and Bangalore data" >> beam.Filter(lambda element: (element[1] == 'Bangalore' or element[1] == "Mumbai"))
    | "Calculate Profit" >> beam.Map(calculate_profit)
    | "Create key value pair" >> beam.Map(lambda element: (element[0],element[8]))
    | "Create window" >> beam.WindowInto(window.SlidingWindows(30,10)) #last 30 secs every 10 sec
    | "Sum Values" >> beam.CombinePerKey(sum)
)
printing = (
    pubsubData
    | "print" >> beam.Map(print)
    )
encodingAndSending = (
    pubsubData
    | "encode byte string" >> beam.Map(lambda element: str(element).encode('utf-8'))
    | "Write to another topic" >> beam.io.WriteToPubSub(topic=output_topic)
)
result = p.run()
result.wait_until_finish()
