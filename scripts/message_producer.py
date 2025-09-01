import time
import os
import requests
import json
from sseclient import SSEClient
from confluent_kafka import Producer
from util.logger import logging as log

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC = "wikimedia.recentchange"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

def error_report(err, msg):
    """ Log error, if message could not be delivered to Kafka broker"""
    if err is not None:
        log.error(f"Delivery failed: {msg}")

def stream_client():
    """Open a streaming connection"""
    headers = {
        "User-Agent": "Demo-Message-Producer",
        "Accept": "text/event-stream"
    }
    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=30)
    return SSEClient(response)

def produce_messages(client):
    """
    Call stream API and produce messages for Kafka broker.
    Check out data/example-event.json for example data.
    """
    for event in client.events():
        if event.event == "message":
            try:
                data = json.loads(event.data)
                # Theoretically we could already filter out canary event here
                log.debug("Producing message...")
                producer.produce(
                    topic=TOPIC,
                    key=str(data.get("id")),
                    value=json.dumps(data).encode("utf-8"),
                    callback=error_report
                )
                producer.poll(0)
            except Exception as e:
                log.error(f"Error processing event: {e}")

if __name__ == "__main__":
    while True:
        log.info("Starting producer...")
        try:
            client = stream_client()
            produce_messages(client)
        except Exception as e:
            log.error(f"Producer interrupted: {e}")
            time.sleep(5)
