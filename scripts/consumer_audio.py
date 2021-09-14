from kafka import KafkaConsumer
from json import loads
import wavio
import numpy as np
import logging
from tqdm import tqdm as tq, trange 
import sys
import os
import boto3


# To consume latest messages and auto-commit offsets
def consumer_audio():

    """
    This function creates a consumer object that listens to the audio topic of the kafka cluster
    
    """

    print("Consumer accessing kafka topic...")
    for i in tq(range(100),desc="Creating consumer object.."):

        # create a kafka consumer ad connect to the topic
        consumer = KafkaConsumer('groupHu_audio',                       
                            bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],
                            value_deserializer=lambda x: loads(x.decode('utf-8')))
        print("Done..")
    for message in consumer:
        
        #extracting the message and creating an audio file from it 
        
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value))
        data=message.value['data']
        data=np.array(data)
        sample_rate=message.value['sample_rate']
        sample_width=message.value['sample width']
        client=boto3.client('s3')
        print("Generating audio file ")
        for i in tq(range(100),desc="Generating audio file.."):
            wavio.write("../audio/recaptured2.wav",data,sampwidth=sample_width,rate=sample_rate)
            s3.meta.client.upload.file('../audio/recaptured2.wav','/mnt/10ac-batch-4/all-data/groupHu/',)


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
