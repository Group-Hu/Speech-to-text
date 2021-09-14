import pyspark
from pyspark.sql import SparkSession
import os
import boto3
import pandas as pd
from smart_open import smart_open
from tqdm import tqdm as tq, trange 
import sys
import os
import logging
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
logging.basicConfig(filename='../logs/boto_bucket.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)

client=boto3.client('s3')


def return_buckets():
    """
    This function access aws buckets and returns the list of created buckets
    
    """



    logging.info("Accessing s3 bucket ")
    print("Accessing s3 bucket ...")
    #connect and check to see if bucket exists
    try:
        #connect to the bucket 
        print("Connecting to s3 bucket ... ")

        for i in tq(range(1),desc="Fetching buckets ..."):
            logging.info("Connected to s3 ...")
            
            #connect to the boto3 client which will access our buckets stored in s3 bucket
            client=boto3.client('s3')
            
            #request boto3 list response
            bucket_response=client.list_buckets()
            buckets = bucket_response["Buckets"]
            logging.info("Fetched buckets")

        print ("Done... \n")

        print(buckets)

    

    except Exception as e:

        
        logging.info("An error has occurred")
        logging.error("The following error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)
    
    return buckets

def check_bucket_objects(bucket_name):
    """
    This function checks for the stored files in the specified bucket
    
    """

    
    print("Accessing s3 bucket ...")
    try:
        #create a boto client to access the aws s3 bucket
        client = boto3.resource('s3')

        #fetch bucket responses and access the specific bucket 
        print("Checking for response ...")
        buckets = bucket_response["Buckets"]
        response=client.list_objects(Bucket=bucket_name)
    
        print (response)
    except Exception as e:

        logging.info("An error has occurred")
        logging.error("The following error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)

    
    return response 



def fetch_s3_data(s3_file_path):
    """
    This function fetches specified files from a specific bucket
    
    """
    print("Communicating with s3 bucket..")
    try:
        client=boto3.client('s3')
        for i in tq(range(1),desc="Accessing S3 bucket server.."):
            # data=pd.read_csv("s3a://grouphu-text-bucket/Clean_Amharic.txt",header=None)
            data=pd.read_csv(s3_file_path,header=None)
            
    except Exception as e:

        logging.info("An error has occurred")
        logging.error("The follocing error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))


    return data

def stream_s3_data():
    client = boto3.resource('s3')
    bucket = client.Bucket("grouphu-text-bucket")
    for obj in bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read().decode('utf-8')
        
        with open("rep.csv","w") as file:
            file.write(body + "\n")

    return body







if __name__ == '__main__':



    # s3 = boto3.client('s3')
    # data = s3.get_object(Bucket='grouphu-text-bucket', Key='Clean_Amharic.txt')
    # contents = data['Body'].read().decode('utf-8')
    # datas=pd.DataFrame(list(contents))
    # # for cont in contents:
    # #     datas.append(cont)

    # # datar=pd.DataFrame(datas)    
    # print(datas)
    data=fetch_s3_data()
    print(data)
    # for line in smart_open('s3a://grouphu-text-bucket/Clean_Amharic.txt', 'rb'):
    #     print(line.decode('utf8'))
    # client = boto3.resource('s3')
    # bucket = client.Bucket("grouphu-text-bucket")
    # for obj in bucket.objects.all():
    #     key = obj.key
    #     body = obj.get()['Body'].read().decode('utf-8')
    #     with open("rep.csv","w") as file:
    #         file.write(body + "\n")
       
        # print(data)
 