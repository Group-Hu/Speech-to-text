import pandas as pd
import numpy as np
import json
import csv
import logging
from tqdm import tqdm as tq, trange 
import sys
import os
import pyspark
from pyspark.sql import SparkSession
import os
import boto3
from boto_s3 import fetch_s3_data

logging.basicConfig(filename='../logs/fetch_data.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)


def gen_data(s3_file_path):
    """
    This function acquires and preprocessess text transcriptions from s3 bucket
    """
    #nice status bar to show progress :)
    for i in tq(range(100),desc="Loading function..."):
        pass
    print ("Complete")
    logging.info("Accessing the gen_data function")
    try:
        #read data from the s3 bucket server
        print ("Reading data \n")
        logging.info("Reading data")
        for reading in tq(range(1),desc="Reading data"):
            data=fetch_s3_data(s3_file_path)
        
        print(" Done \n")

    except FileNotFoundError as e:
        logging.info("An error has occurred")
        logging.error("The follocing error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
    
    tq.pandas()

    #change a bit of the dataset columns
    data.columns=['Text']

    #do some bit of preprocessing of the dataframe
    for reading in trange((1),desc="Cleaning  data"):
            
        
        data['Text']=data['Text'].apply(lambda x: x.replace("(",""))
        data['Text']=data['Text'].apply(lambda x: x.replace(")",""))
        data['Text']=data['Text'].apply(lambda x :x.replace("[",""))


        data['text_id']=data['Text'].apply(lambda x: x.split(" ")[-1])
        data['Text']=data['Text'].apply(lambda x: x.replace("sentence",""))
        data['Text']=data['Text'].apply(lambda x:x[:-2])
    print ("Done... ")

    #save the dataframe to a csv file
    data.to_csv('amharic.csv',index=False)
    
    
    #create a dictionary that imitates a json object only then are we able to serialize our data and sent it 
    print("Creating dictionary for serialization ....")
    corpus={}
    with open('amharic.csv') as csvFile:
        csvReader=csv.DictReader(csvFile)
        for rows in csvReader:
            key=rows['text_id']
            
            corpus[key]=rows['Text']
            



    return corpus


if __name__ == "__main__":
    bucket_name=input("Enter Bucket path: ")
    data=gen_data(bucket_name)
    print(data)
