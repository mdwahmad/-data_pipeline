from pyspark.sql import SparkSession
from datetime import date
import datetime


# Initialize SparkSession with necessary GCS connector and settings
from pyspark.sql import SparkSession

# Initialize SparkSession with necessary GCS connector and settings
spark = SparkSession.builder \
    .appName("Flights Data Analysis") \
    .config("spark.jars", "file:///C:/Users/md.w.ahmad/gcs-connector-hadoop2-latest/gcs-connector-hadoop2-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "D:/GitHub/bigquery-sparksql-batch-etl/feisty-flow-437715-t9-de4d8b790b41.json") \
    .getOrCreate()


print("Debugging Configuration:")
print("FS.GS Impl:", spark._jsc.hadoopConfiguration().get("fs.gs.impl"))
print("Abstract FS.GS Impl:", spark._jsc.hadoopConfiguration().get("fs.AbstractFileSystem.gs.impl"))
print("Service Account Enabled:", spark._jsc.hadoopConfiguration().get("google.cloud.auth.service.account.enable"))
print("JSON Key Path:", spark._jsc.hadoopConfiguration().get("google.cloud.auth.service.account.json.keyfile"))


# Correct path format for the service account JSON key
local_json_path = "D:\\GitHub\\bigquery-sparksql-batch-etl\\feisty-flow-437715-t9-de4d8b790b41.json"
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", local_json_path)



# Test to read data from your GCS bucket
#file_date = "2019-05-04"  
bucket_name = "gs://feisty-flow-bucket-437715"


try:
    #flights_data = spark.read.json(f"{bucket_name}/data-files/{file_date}.json")
    # Read all JSON files from the 'data-files' directory in the bucket
    flights_data = spark.read.json(f"{bucket_name}/data-files/*.json")

    flights_data.show()
except Exception as e:
    print("Failed to read data from GCS:", e)


# Check if the DataFrame has been loaded
print(flights_data.count())


# Register the DataFrame as a SQL temporary view
flights_data.createOrReplaceTempView("flights_data")

# Execute SQL query
spark.sql("SELECT MAX(distance) FROM flights_data LIMIT 10").show()


qry = """
        SELECT 
            flight_date, 
            ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
            ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
            flight_num 
        FROM 
            flights_data 
        GROUP BY 
            flight_num, 
            flight_date 
      """

avg_delays_by_flight_nums = spark.sql(qry)

query = """
        SELECT 
            *,
            CASE 
                WHEN distance BETWEEN 0 AND 500 THEN 1 
                WHEN distance BETWEEN 501 AND 1000 THEN 2
                WHEN distance BETWEEN 1001 AND 2000 THEN 3
                WHEN distance BETWEEN 2001 AND 3000 THEN 4 
                WHEN distance BETWEEN 3001 AND 4000 THEN 5 
                WHEN distance BETWEEN 4001 AND 5000 THEN 6 
            END AS distance_category 
        FROM 
            flights_data 
        """

flights_data = spark.sql(query)
flights_data.createOrReplaceTempView("flights_data")

qry = """
        SELECT 
            flight_date, 
            ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
            ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
            distance_category 
        FROM 
            flights_data 
        GROUP BY 
            distance_category, 
            flight_date 
      """

avg_delays_by_distance_category = spark.sql(qry)



#file_name = file_date

#output_flight_nums = f"{bucket_name}/flights_data_output/{file_name}_flight_nums"
#output_distance_category = f"{bucket_name}/flights_data_output/{file_name}_distance_category"

# Save the data
#avg_delays_by_flight_nums.coalesce(1).write.format("json").save(output_flight_nums)
#avg_delays_by_distance_category.coalesce(1).write.format("json").save(output_distance_category)

# Save the data with overwrite mode
#avg_delays_by_flight_nums.coalesce(1).write.format("json").mode("overwrite").save(output_flight_nums)
#avg_delays_by_distance_category.coalesce(1).write.format("json").mode("overwrite").save(output_distance_category)

# Generate a timestamp or a suitable name for the output directory
current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_flight_nums = f"{bucket_name}/flights_data_output/flight_nums_{current_time}"
output_distance_category = f"{bucket_name}/flights_data_output/distance_category_{current_time}"


try:
    avg_delays_by_flight_nums.coalesce(1).write.format("json").mode("overwrite").save(output_flight_nums)
    avg_delays_by_distance_category.coalesce(1).write.format("json").mode("overwrite").save(output_distance_category)
    print("Data saved successfully.")
except Exception as e:
    print(f"Failed to save data: {e}")


avg_delays_by_distance_category.show()
print(avg_delays_by_distance_category.count())