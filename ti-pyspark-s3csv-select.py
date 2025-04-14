
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")  # Set log level to INFO to monitor execution
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the CSV file from S3
s3_path = "s3://ti-student-feb-2025/retail/product/products.csv"
products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

# Select two specific columns with aliasing: 'product_name' as 'ProductName' and 'price' as 'UnitPrice'
selected_columns_df = products_df.select(
    col("productname").alias("Product Name"), 
    col("unit_price").alias("Unit Price")
)

# Show the output with the new column names
print("Selected Columns with Aliases from Products Table:")
selected_columns_df.show()
