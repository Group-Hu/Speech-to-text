import pyspark
from pyspark.sql import SparkSession
import os
import boto3
import pandas as pd
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

# client=boto3.client('s3')
df=pd.read_csv("s3a://grouphu-text-bucket/Clean_Amharic.txt")
print(df)





if __name__ == '__main__':
    # client=boto3.client('s3')
    df=pd.read_csv("s3a://grouphu-text-bucket/Clean_Amharic.txt")
    print(df)

