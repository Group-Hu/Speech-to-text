import os
import sys
import pandas as pd
import numpy as np
import logging
import boto3
from botocore.exceptions import ClientError
import glob
from scipy.io.wavfile import read


def convert_to_ndarray(DATASET_PATH):
    wavs = []
    for filename in glob.glob(DATASET_PATH + "wav/*.wav"):
        wavs.append(read(filename))
        return wavs

def upload_ndarray(file_name: str, bucket: str, object_name=None):
        
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        return False
    return True