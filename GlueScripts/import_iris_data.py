import sys
from datetime import date
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import when
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, expr, when, round
from pyspark.sql.types import LongType
from awsglue.dynamicframe import DynamicFrame
import pandas as pd

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)
database = 'iris-database' #replace with your user id
today = date.today()
logger = glueContext.get_logger()
logger.info("info message")

#current_date=date.today()

processed_dir="s3://34343-processed/train/" #replace with your user id
#partition_predicate="(year=='"+str(today.year)+"' and month=='"+str(today.month)+"' and day=='"+str(today.day)+"')"


# Create a DynamicFrame using the 'iris' table
iris_DyF = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="34343_raw")


iris_df = iris_DyF.toDF() # convert to spark dataframe to remove some columns

# add column name
iris_df = iris_df.withColumnRenamed("col0", "sepal length")\
      .withColumnRenamed("col1", "sepal width")\
      .withColumnRenamed("col2", "petal length")\
      .withColumnRenamed("col3", "petal width")\
      .withColumnRenamed("col4", "label")

iris_df=iris_df[["sepal length","sepal width",  "petal length", "petal width", "label"]]

#iris_DyF= DynamicFrame.fromDF(iris_df, glueContext, "iris_DyF")

#iris_DyF.printSchema()

iris_df.show(10)


data_directory=processed_dir+"train.csv"
df_pandas = iris_df.toPandas()
df_pandas = df_pandas[:-1]
df_pandas.to_csv(data_directory, header=True, index=True, line_terminator="")

