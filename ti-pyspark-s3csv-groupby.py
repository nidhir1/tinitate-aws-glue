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

# Display the aggregated results in the console
print("Aggregated Results:")
result_df.show()

# Log that results are displayed
logger.info("Aggregated data displayed in console successfully.")

# Final log to indicate successful completion of the script
logger.info("Script execution completed successfully.")
sc.stop()
