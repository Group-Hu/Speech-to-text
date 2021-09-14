from kafka import KafkaConsumer
from json import loads
import wavio
import numpy as np
import logging
from tqdm import tqdm as tq, trange 
import sys
import os
import boto3
import json
import base64
import io
from pydub import AudioSegment


# To consume latest messages and auto-commit offsets

def json_deserializer(data):
    return json.loads(data)

def consumer_audio():

    """
    This function creates a consumer object that listens to the audio topic of the kafka cluster
    
    """

    print("Consumer accessing kafka topic...")
    for i in tq(range(1),desc="Creating consumer object.."):

        # create a kafka consumer ad connect to the topic
        consumer = KafkaConsumer('groupHu_audio',                       
                            bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],
                            value_deserializer=json_deserializer)
        print("Done..")
    for message in consumer:
        
        #extracting the message and creating an audio file from it 
        
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                          message.value))
        key=message.key.decode('utf-8')
        data=base64.b64decode(message.value['data'])
        # data=message.value['data']
        bytes_wav=bytes()
        bytes_io_wav=io.BytesIO(data)
        # data=np.array(data)
        # sample_rate=message.value['sample_rate']
        # sample_width=message.value['sample width']
        s3=boto3.resource('s3')
        # audio = AudioSegment.from_raw(bytes_io_wav, sample_width=2, frame_rate=22050, channels=1)
        # s3.meta.client.upload_file('../audio/recaptured2.wav','/mnt/10ac-batch-4/all-data/groupHu/',+key+'.wav')
        print("Generating audio file ")
        for i in tq(range(100),desc="Generating audio file.."):
            audio = AudioSegment.from_raw(bytes_io_wav, sample_width=2, frame_rate=22050, channels=1).export('../audio/'+key+'.wav',format='wav')
            s3.meta.client.upload_file('../audio/'+key+'.wav','grouphu-audio-bucket',key+'.wav')
        #     wavio.write("../audio/recaptured2.wav",data,sampwidth=sample_width,rate=sample_rate)
        #     s3.meta.client.upload_file('../audio/recaptured2.wav','/mnt/10ac-batch-4/all-data/groupHu/',+key+'.wav')


    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # consume msgpack
    KafkaConsumer(value_deserializer=msgpack.unpackb)

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)


if __name__ == "__main__":
    #if ran as a script we want to access the following functions
    consumer_audio()
