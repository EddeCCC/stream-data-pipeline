#!/bin/bash

python scripts/message_producer.py &
sleep 5

python scripts/message_consumer.py
