from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import expr, array, lit



# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Load CSV data into a DataFrame
df = spark.read.option("header", "true").csv("s3://ti-student-feb-2025/hr-data/employee_dept/")

# Pivot the data with aggregation
pivot_df = df.groupBy("employee_id", "employee_name", "department").agg(
    sum("january_salary").alias("january_salary"),
    sum("february_salary").alias("february_salary"),
    sum("march_salary").alias("march_salary")
)

# Show the pivoted DataFrame
pivot_df.show()

# Unpivot the pivoted DataFrame with aggregation
unpivot_df = pivot_df.selectExpr(
    "employee_id", "employee_name", "department",
    "stack(3, 'january_salary', january_salary, 'february_salary', february_salary, 'march_salary', march_salary) as (month, salary)"
)

# Show the unpivoted DataFrame
unpivot_df.show()
