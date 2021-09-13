import pyspark
from pyspark.sql import SparkSession
import os
import boto3
import pandas as pd
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
import configparser

# config=configparser.ConfigParser()
# config.read(os.path.expanduser("~/.aws/credentials"))

# access_id = config.get('blaise_papa', "aws_access_key_id") 
# access_key = config.get('blaise_papa', "aws_secret_access_key")
# sc=spark.sparkContext
# hadoop_conf=sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
# hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

def get_from_s3():
    spark=SparkSession.builder.appName('spark-h').getOrCreate()


    data=spark.read.csv("s3a://grouphu-text-bucket/Clean_Amharic.txt")

    return data

if __name__ == '__main__':
    spark=SparkSession.builder.appName("spark-trial-test").getOrCreate()
    df=spark.read.format("csv").option("header","true").option("inferSchema","true").csv("s3://grouphu-text-bucket/Clean_Amharic.csv")
    print(df)
    # client=boto3.client('s3')
    # df=pd.read_csv("s3a://grouphu-text-bucket/Clean_Amharic.txt")
    # print(df)
