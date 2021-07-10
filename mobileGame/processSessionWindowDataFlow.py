import apache_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions,SetupOptions
from apache_beam import window
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--subscription',
                    dest='subscriptionId',
                    required=True,
                    help='Input pubsub subscription to process')

parser.add_argument('--topic',
                    dest='topicId',
                    required=True,
                    help='Input pubsub topic to write to')






known_args, pipeline_args = parser.parse_known_args()
dataflowArgs = vars(known_args)

project_id = pipeline_args[1]
subscription_id = dataflowArgs['subscriptionId']
output_topic_id = dataflowArgs['topicId']

input_subscription = f'projects/{project_id}/subscriptions/{subscription_id}'
output_topic = f'projects/{project_id}/topics/{output_topic_id}'


pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(SetupOptions).save_main_session = True

def map_points(points, map_loc1, map_loc2, locPoints=3):
    if map_loc1 != map_loc2:
        return points + locPoints
    else:
        return points

def battle_time_points(points, battle_time):
    if (battle_time >= 10 and battle_time <= 20):
        return points + 4
    elif (battle_time >= 21 and battle_time <= 30):
        return points + 3
    elif (battle_time >= 31 and battle_time <= 40):
        return points + 2
    elif battle_time > 40:
        return points + 1

def weapon_comparison_points(points, win_player_weapon_rank, lost_player_weapon_rank):
    if win_player_weapon_rank - lost_player_weapon_rank > 6:
        return points + 3
    elif win_player_weapon_rank - lost_player_weapon_rank > 3 :
        return points + 2
    else:
        return points + 1



def calculate_points(element):
    total_points = 0
    win_player_ranking = int(element[6])
    lost_player_ranking = int(element[13])

    win_player_map = element[7]
    lost_player_map = element[14]

    battle_time = int(element[15])

    total_points = weapon_comparison_points(total_points,win_player_ranking, lost_player_ranking)
    total_points = battle_time_points(total_points, battle_time)
    total_points = map_points(total_points, win_player_map, lost_player_map)

    return element[0] + "," + element[1] + "," + element[2] + "," + element[3] + "," + element[4] + "," + element[5], total_points




p = beam.Pipeline(options=pipeline_options)

pubsubData = (
    p
    | "Read pubsub data" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | "clean data" >> beam.Map(lambda element: (element.decode('utf-8').rstrip().lstrip()))
    | "Split data" >> beam.Map(lambda element: element.split(','))
)

game_score = (
    pubsubData
    | "Create key value pair game, player and points" >> beam.Map(calculate_points)
    | "Create Global window player" >> beam.WindowInto(window.Sessions(30))
    | "Average values for players for inactivity" >> beam.combiners.Mean.PerKey()
)
printing = (
    game_score
    | "print" >> beam.Map(print)
    )
encodingAndSending = (
    game_score
    | "encode byte string" >> beam.Map(lambda element: str(element).encode('utf-8'))
    | "Write to another topic" >> beam.io.WriteToPubSub(topic=output_topic)
)
result = p.run()
result.wait_until_finish()
