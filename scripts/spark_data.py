import pyspark
from pyspark.sql import SparkSession
import os
import boto3
import pandas as pd
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
import configparser



def get_from_s3():
    spark=SparkSession.builder.appName('spark-h').getOrCreate()


    data=spark.read.text("s3a://grouphu-text-bucket/Clean_Amharic.txt")

    return data

if __name__ == '__main__':
    # spark=SparkSession.builder.appName("spark-trial-test").getOrCreate()
    # df=spark.read.format("csv").option("header","true").option("inferSchema","true").csv("s3://grouphu-text-bucket/Clean_Amharic.txt")
    # print(df)
    # client=boto3.client('s3')
    # df=pd.read_csv("s3a://grouphu-text-bucket/Clean_Amharic.txt")
    # print(df)
    get_from_s3()