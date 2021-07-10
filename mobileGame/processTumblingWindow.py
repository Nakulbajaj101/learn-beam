import apache_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from apache_beam.transforms.trigger import AfterCount,AccumulationMode, Repeatedly, AfterAny, AfterProcessingTime


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

class AddTimestampDoFn(beam.DoFn):
      def process(self, element, timestamp = beam.DoFn.TimestampParam):
          element = list(element)
          element.append(timestamp.to_utc_datetime())
          element = tuple(element)
          return element
    

p = beam.Pipeline(options=options)

pubsubData = (
    p
    | "Read pubsub data" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    #| "Read some data" >> beam.io.ReadFromText("mobile_game.txt")
    | "Clean and encode byte string" >> beam.Map(clean_data)
    | "Split data" >> beam.Map(lambda element: element.split(','))
)

player_score = (
    pubsubData
    | "Create key value pair player and score" >> beam.Map(lambda element: ("Player ," + element[1] + "," + element[2],1))
    | "Create Global window player" >> beam.WindowInto(window.FixedWindows(100), trigger=Repeatedly(AfterAny(AfterCount(60),AfterProcessingTime(30))),accumulation_mode=AccumulationMode.ACCUMULATING)
    | "Sum Values for players Window" >> beam.CombinePerKey(sum)
    | "append timestamps to players" >> beam.Map(lambda element: beam.window.TimestampedValue(element, time.time()))
    | "add player timestamp" >> beam.ParDo(AddTimestampDoFn())
)

team_score = (
    pubsubData
    | "Create key value pair team and score" >> beam.Map(lambda element: ("Team ," + element[3] + "," + element[4],1))
    | "Create Global window team" >> beam.WindowInto(window.FixedWindows(100), trigger=Repeatedly(AfterAny(AfterCount(60),AfterProcessingTime(30))),accumulation_mode=AccumulationMode.ACCUMULATING)
    | "Sum Values for team Window" >> beam.CombinePerKey(sum)
    | "append timestamps to teams" >> beam.Map(lambda element: beam.window.TimestampedValue(element, time.time()))
    | "add team timestamp" >> beam.ParDo(AddTimestampDoFn())
)
printing = (
    (player_score, team_score)
    | "combine" >> beam.Flatten()
    | "print" >> beam.Map(print)
    )
encodingAndSending = (
    (player_score, team_score)
    | "combine before send" >> beam.Flatten()
    | "encode byte string" >> beam.Map(lambda element: str(element).encode('utf-8'))
    | "Write to another topic" >> beam.io.WriteToPubSub(topic=output_topic)
)
result = p.run()
result.wait_until_finish()
