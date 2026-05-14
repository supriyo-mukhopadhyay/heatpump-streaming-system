#!/bin/sh
. grant/ep011/venv/bin/activate 
nohup python3 grant/ep011/mqtt_kinesis_datastream.py --stream 'amzn-ep011-kinesis-datasource' & 
nohup python3 grant/ep011/mqtt_kinesis_consumer.py --source_stream 'amzn-ep011-kinesis-datasource' --dest_stream 'amzn-ep011-kinesis-destination' 