
from kafka import KafkaConsumer
from json import loads
import json
import numpy as np
import wavio


if __name__ == "__main__":

    consumer = KafkaConsumer('jakinda_audio_topic',bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092",
    "b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        na = message[6]['data']
        data = np.array(na)
        rate = message[6]['rate']
        sampwidth = message[6]['sampwidth']
        wavio.write("test.wav", data, rate,sampwidth=sampwidth)