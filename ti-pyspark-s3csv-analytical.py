
# Importing necessary libraries from PySpark and AWS Glue
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, rank, dense_rank
from pyspark.sql.window import Window

# Initialize Spark context with log level
sc = SparkContext()

# Setting log level to INFO to see informative messages during execution
sc.setLogLevel("INFO")    

# Initialize Glue context and Spark session for AWS Glue operations
glueContext = GlueContext(sc) # Creating a Glue context based on the Spark context
spark = glueContext.spark_session # Getting the Spark session from the Glue context

# Set up logging
logger = glueContext.get_logger()
print("Logger initialized. Starting script execution.")
# Read the CSV file from S3

s3_path = "s3://ti-student-feb-2025/retail/purchase/purchase_transactions.csv"
analyzed_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)
# Applying analytical functions using window functions
window_spec = Window.partitionBy("product_supplier_id").orderBy("purchase_tnxdate")
analyzed_df = analyzed_df.withColumn("previous_invoice_price", lag("invoice_price").over(window_spec))
analyzed_df = analyzed_df.withColumn("next_invoice_price", lead("invoice_price").over(window_spec))
analyzed_df = analyzed_df.withColumn("invoice_price_rank", rank().over(Window.partitionBy("product_supplier_id").orderBy(col("invoice_price").desc())))
analyzed_df = analyzed_df.withColumn("invoice_price_dense_rank", dense_rank().over(Window.partitionBy("product_supplier_id").orderBy(col("invoice_price").desc())))

# Define the column names for the DataFrame
column_names = [
    "purchase_tnx_id", "product_supplier_id", "purchase_tnxdate", "quantity", "invoice_price", 
    "previous_invoice_price", "next_invoice_price", "invoice_price_rank", "invoice_price_dense_rank"
]

# Rename DataFrame columns
analyzed_df = analyzed_df.toDF(*column_names)

# Display the DataFrame with analytical results
print("Displaying analyzed DataFrame results:")
analyzed_df.show(truncate=False)

# Log information after displaying results
logger.info("Analyzed DataFrame results displayed in the console successfully.")
