import pandas as pd
import numpy as np
import json
import csv
import logging
from tqdm import tqdm as tq, trange 
import sys
import os

logging.basicConfig(filename='../logs/fetch_data.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)


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
            data=pd.read_csv('../data/Clean_Amharic.txt',header=None)
        
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


if __name__ == "__main__":
    gen_data()

