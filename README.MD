# Purpose

The purpose of this project is to start up with beam

The mobileGame folder has example of setting up and running beam on google cloud data flow

The other examples are using both batch and streaming data over different windows

# Using Dataflow

## Prerequisites

* Google project setup
* Gcloud sdk installed and configured


## Running

The developer should change the project name
and change the topic and subscriber name of stream

Also the python file that needs to be run 
can be manupulated in the runBeamDataFlow.sh file. Also topic name and subscriber name
also needs to be changed based on createResources.sh file setup

```bash 
pip install -r requirements.txt

bash createResources.sh

bash runBeamDataFlow.sh
```

## Cleaning dataflow and pubsub

```bash

bash deleteResources.sh

```

# Author
Name:  Nakul Bajaj   
Email: bajaj.nakul@gmail.com   
