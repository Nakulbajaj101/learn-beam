#!/bin/bash

python processSessionWindowDataFlow.py --subscription Subscribe1 --topic Topic2 --project bridge-data-analytics-app --runner DataflowRunner --streaming True --region us-central1 --temp_location=gs://beam-test-dev --job_name nakul-example-1 