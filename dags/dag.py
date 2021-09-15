from kafka import KafkaProducer,KafkaConsumer
import tqdm
import wavio
import pandas as pd
import numpy as np
from json import loads
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def produce_transcriptions():
    """
    This function creates a kakfa producer object and through it send data transcriptions to the kafka topic
    
    """
    try:
        #create a kafka producer object
        logging.info("Accessing Topic..")
        print("Accessing kafka topic")
        for i in tq(range(100),desc="Accessing Broker.."):
            producer=KafkaProducer(bootstrap_servers=["b-1.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092","b-2.demo-cluster-1.9q7lp7.c1.kafka.eu-west-1.amazonaws.com:9092"],value_serializer=json_serializer)
        
        print("Done")

    except Exception as e:
        


        logging.info("An error has occurred")
        logging.error("The following error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)
    while 1==1:
        #generating and sending transcitpions to topic 

        logging.info("Producing Transcripts..")
        print("Producing Transcriptions")
       
        data = gen_data()
        
        
        for text in data.items():
            print("Publishing to Topic..\n")
            
            producer.send("groupHu_speech",text)
            print("Done...\n")
            time.sleep(4)
def gen_data():
    """
    This function acquires and preprocessess text transcriptions
    """
    for i in tq(range(100),desc="Loading function..."):
        pass
    print ("Complete")
    logging.info("Accessing the gen_data function")
    try:
        print ("Reading data \n")
        logging.info("Reading data")
        for reading in trange((1),desc="Reading data"):
            data=pd.read_csv('..data/Clean_Amharic.txt',header=None)
        
        print(" Done \n")

    except FileNotFoundError as e:
        logging.info("An erro has occured")
        logging.error("The follocing error occured {} ".format(e.__class__))
        print("The following error occured {} ".format(e.__class__))
    
    tq.pandas()
    data.columns=['Text']
    for reading in trange((1),desc="Cleaning  data"):
            
        
        data['Text']=data['Text'].apply(lambda x: x.replace("(",""))
        data['Text']=data['Text'].apply(lambda x: x.replace(")",""))
        data['Text']=data['Text'].apply(lambda x :x.replace("[",""))


        data['text_id']=data['Text'].apply(lambda x: x.split(" ")[-1])
        data['Text']=data['Text'].apply(lambda x: x.replace("sentence",""))
        data['Text']=data['Text'].apply(lambda x:x[:-2])
    print ("Done... ")

    data.to_csv('amharic.csv',index=False)
    
    
    print("Creating dictonary for serialization ....")
    corpus={}
    with open('amharic.csv') as csvFile:
        csvReader=csv.DictReader(csvFile)
        for rows in csvReader:
            key=rows['text_id']
            
            corpus[key]=rows['Text']
            
    return corpus

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
        print("Generating audio file ")
        for i in tq(range(100),desc="Generating audio file.."):
            wavio.write("recaptured2.wav",data,sampwidth=sample_width,rate=sample_rate)

    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # consume msgpack
    KafkaConsumer(value_deserializer=msgpack.unpackb)

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)




dag = DAG('STT', description='speech to text',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

task1 = PythonOperator(task_id='produce_transcription',
                       python_callable=produce_transcriptions, 
                       dag=dag)
task2 = PythonOperator(task_id='consume_audio', 
                       python_callable=consumer_audio, 
                       dag=dag)

task1>>task2