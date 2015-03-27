#!/usr/bin/env python
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

def main():
    client = KafkaClient("localhost:9092")
    producer = SimpleProducer(client)
    producer.send_messages('test', "test")
    time.sleep(1)

main()

