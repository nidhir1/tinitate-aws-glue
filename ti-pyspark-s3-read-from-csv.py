import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Specify the S3 path to the CSV and gzip-compressed CSV files
s3_path = 's3://ti-student-feb-2025/nidhi/input/'
customer_table = glueContext.create_dynamic_frame_from_options(connection_type= 's3',
                                                               connection_options={"paths": [s3_path]},
                                                               format='csv', format_options = {"withHeader": True})
# Showing the Dataframe Contents                                                      
customer_table.printSchema()
customer_table.toDF().show()                                                        
