import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

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

p = beam.Pipeline(options=options)

pubsubData = (
    p
    | "Read pubsub data" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | "Write to another topic" >> beam.io.WriteToPubSub(topic=output_topic)
    )
result = p.run()
result.wait_until_finish()