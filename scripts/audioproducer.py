from kafka import KafkaProducer
from time import sleep
import json 
import numpy as np
from json import dumps
from datetime import datetime
import wavio


def audio (audio):
    wave = wavio.read(audio)
    rate = wave.rate 
    sampwidth = wave.sampwidth
    data = wave.data 
    data = data.tolist()
    dic = {'data' : data, 'rate': rate, 'sampwidth': sampwidth}
    return dic

aud = audio("../notebooks/SWH-05-20101106_16k-emission_swahili_05h30_-_06h00_tu_20101106_part2.wav")

if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

    producer.send("jakinda_audio_topic", key=b'audio', value=aud )