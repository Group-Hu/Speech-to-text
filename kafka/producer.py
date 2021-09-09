from kafka import KafkaProducer
import json
from data import get_registered_user
import time
import pandas as pd
from fetch_data import gen_data

def json_serializer(data):
    return json.dumps(data).encode("utf-8")
    

producer=KafkaProducer(bootstrap_servers=["b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],value_serializer=json_serializer)


if __name__ == "__main__":
    while 1==1:
        data = get_registered_user()
        print(data)
        producer.send("groupHu_speech",data)
        time.sleep(4)
        
