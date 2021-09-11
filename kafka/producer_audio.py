from kafka import KafkaProducer
import json
from data import get_registered_user
import time
import pandas as pd
from fetch_data import gen_data
from audio import get_audio 
import logging
from tqdm import tqdm as tq, trange 
import sys
import os

logging.basicConfig(filename='../logs/producer_audio.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)

#serializer function to get data encode into json format and send it over the channel
def json_serializer(data):
    return json.dumps(data).encode("utf-8")
    

def producer_audio():
    """"
    This function creates a producer object that is able to generate and send data to a kafka topic cluster 
    
    """
    try:
        #creating a kafka producer cluster
        logging.info("Accessing Topic..")
        print("Accessing Audio kafkaTopic...")
        
        for i in tq(range(100),desc="Accessing Broker.."):
            producer=KafkaProducer(bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],value_serializer=json_serializer)
        print("Done ..... ")
    
    except Exception as e:
        
        logging.info("An error has occured")
        logging.error("The following error occured {} ".format(e.__class__))
        print("The following error occured {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)
    
    while 1==1:
        #generating audio and sending to kafka cluster
        logging.info("Producing audio..")
        print("Producing audio")
       
        data=get_audio()
        for audio in data.items():
            print("Publishing to Topic..\n")
            print (audio)
            time.sleep(10)
        
        
        producer.send("groupHu_audio",data)
        print("Done...\n")


if __name__ == "__main__":
    #if ran as a script we want to access the following functions
    producer_audio()

        
