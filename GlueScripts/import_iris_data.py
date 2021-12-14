import sys
import boto3
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, expr, when, round
from pyspark.sql.types import LongType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


path="iris/iris.csv"
raw_bucket='34343-raw' #replace with your user id here
s3 = boto3.resource('s3')
bucket = s3.Bucket(raw_bucket)

objs = list(bucket.objects.filter(Prefix=path))
if len(objs) > 0 and objs[0].key == path:
 print("Object "+path+" already exists!")
else:
 print("Starting download")
 iris_url='https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data'
 iris_csv=requests.get(iris_url).text
 
 # Method 1: Object.put()
 object = s3.Object(raw_bucket, path)
 object.put(Body=iris_csv)
 print("Done")
