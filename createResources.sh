#!/bin/bash

gcloud pubsub topics create Topic1
gcloud pubsub topics create Topic2
gcloud pubsub subscriptions create Subscribe1 --topic=Topic1
gcloud pubsub subscriptions create Subscribe2 --topic=Topic2