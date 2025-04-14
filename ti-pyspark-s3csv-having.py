# Import necessary libraries for Spark and AWS Glue functionalities
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession 
from pyspark.sql.functions import count


# Initialize Spark context and set the log level to INFO to capture informational messages during execution
sc = SparkContext.getOrCreate()
sc.setLogLevel("INFO")

# Create a GlueContext and retrieve the associated Spark session
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Setup logging
logger = glueContext.get_logger()
logger.info("Using Logger: Starting script execution")



# Read the CSV file from S3

s3_path = "s3://ti-student-feb-2025/ti-athena-training/electric-vehicles/electric_vechiles/Electric_Vehicle_Population_Data.csv"
electric_vehicles_df= spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(s3_path)

# Group by 'make' and 'model' columns, and count the occurrences
result_df = electric_vehicles_df.groupBy("make", "model").agg(count("*").alias("count"))
# Apply filter similar to HAVING clause
result_df_filtered = result_df.filter(result_df["count"] > 1000)

# Display the filtered DataFrame in the console
print("Filtered Results:")
result_df_filtered.show()

# Log information after displaying in the console
glueContext.get_logger().info("Filtered data successfully displayed in the console.")
sc.stop()
