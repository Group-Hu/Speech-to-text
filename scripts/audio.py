import wavio
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm as tq, trange 
import sys
import os
import boto3
logging.basicConfig(filename='../logs/producer_audio.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)


def get_audio():
    """
    This function reads a wav file and extracts sample rate data and ample width 
    
    """
    try:
        print("Fetching Audio File... \n")
        client=boto3.client('s3')
        sound=wavio.read('s3://grouphu-audio-bucket/test.wav')
        print("Done \n")
    except  FileNotFoundError as e:
        logging.info("File was not found")
        logging.error("The following error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)

    for i in tq(range(100),desc="Unpacking Audio.."):
        data=sound.data
        srate=sound.rate
        swidth=sound.sampwidth
        print("Done ... ")

    details={'data':data.tolist(),'sample_rate':srate,'sample width':swidth}
    # print(details)
    return details

if __name__ == "__main__":
    #if ran as a script we want to access the following functions
    get_audio()

