# Import necessary libraries for Spark and AWS Glue functionalities
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import sys

# Initialize Spark and Glue contexts
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the CSV file from S3
s3_path = "s3://ti-student-feb-2025/retail/purchase/purchase_transactions.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

# Select distinct product_supplier_id
distinct_suppliers_df = df.select("product_supplier_id").distinct()

# Show distinct values
print("Distinct product_supplier_id:")
distinct_suppliers_df.show()


# Log information
glueContext.get_logger().info("Distinct product_supplier_id values successfully displayed.")


