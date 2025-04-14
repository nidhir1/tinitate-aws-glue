from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark and Glue contexts
sc = SparkContext()
sc.setLogLevel("INFO")
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read the CSV file from S3
s3_path = "s3://ti-student-feb-2025/retail/product/products.csv"
products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

s3_path = "s3://ti-student-feb-2025/retail/category/categories.csv"
categories_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

s3_path = "s3://ti-student-feb-2025/retail/dispatch/dispatch_transactions.csv"
dispatch_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)


# Perform joins
# Join products with categories
product_category_df = products_df.join(categories_df, products_df.categoryid == categories_df.categoryid, "inner")

# Join the result with dispatch
product_category_df = products_df.join(categories_df, products_df.categoryid == categories_df.categoryid, "inner")
final_df = product_category_df.join(dispatch_df, product_category_df.productid == dispatch_df.product_id, "inner")

# Display results in console for CloudWatch logging
print("Final DataFrame:")
final_df.show()

# Log the operation completion in CloudWatch logs
glueContext.get_logger().info("Join operation completed successfully. Results displayed in console.")
