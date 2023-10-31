from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, when, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

staging_bucket = "movie-review-bucket-staging"
processed_log_reviews = f'gs://{staging_bucket}/log_review/'
processed_movie_reviews = f'gs://{staging_bucket}/movie_review/'
GCP_PROJECT_ID = 'user-behaviour-project'
DATASET_ID = None


log_review_schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("log_date", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("os", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("phone_number", StringType(), True)
])

movie_review_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("positive_review", IntegerType(), True),
    StructField("review_id", IntegerType(), True)
])

spark = SparkSession.builder.appName("MovieReviewAnalytics").getOrCreate()
spark.conf.set("temporaryGcsBucket", f"{staging_bucket}")

log_review_df = spark.read.csv(processed_log_reviews, schema=log_review_schema)
movie_review_df = spark.read.csv(processed_movie_reviews, schema=movie_review_schema)

log_review_df.show()
movie_review_df.show()
data = moviedf \
    .join(logdf, moviedf.review_id == logdf.log_id) \
    .select(
        col('user_id'),
        to_date(col("log_date"), 'MM-dd-yyyy').alias('log_date').cast('date'),
        col("location"),
        col("device"),
        col("os"),
        col("positive_review"),
        col("review_id"),
    )

os_browser_mapping = {
    "Google Android": "Google Chrome", 
    "Apple iOS": "Safari", 
    "Linux": "Firefox", 
    "Apple MacOS": "Safari", 
    "Microsoft Windows": "Edge"
}

data = data.withColumn(
    "browser",
     when(col("os") == 'Google Android', "Google Chrome")
    .when(col('os') == "Apple iOS", "Safari")
    .when(col('os') == "Linux", "Firefox")
    .when(col('os') == "Apple MacOS", "Safari")
    .when(col('os') == "Microsoft Windows", "Edge")   
)
data = data.withColumn("browser", data["browser"].cast("string"))

data.show()

dim_date = data.selectExpr(
    "log_date AS date", 
    "day(log_date) AS day", 
    "month(log_date) AS month", 
    "year(log_date) AS year", 
    "CASE \
        WHEN month(log_date) IN (12, 1, 2) THEN 'Winter' \
        WHEN month(log_date) IN (3, 4, 5) THEN 'Spring' \
        WHEN month(log_date) IN (6, 7, 8) THEN 'Summer' \
        ELSE 'Fall' END AS season"
    ).distinct().withColumn("id_date", monotonically_increasing_id())

dim_date = dim_date.select(
    'id_date',
    'date',
    'day',
    'month',
    'year',
    'season'
)

dim_date.show()

dim_device = data.select("device").distinct().withColumn("id_device", monotonically_increasing_id())
dim_device = dim_device.select('id_device', 'device')
dim_device.show()

dim_location = data.select("location").distinct().withColumn("id_location", monotonically_increasing_id())
dim_location = dim_location.select('id_location', 'location')
dim_location.show()

dim_browser = data.select("browser").distinct().withColumn("id_browser", monotonically_increasing_id())
dim_browser = dim_browser.select('id_browser', 'browser')
dim_browser.show()

dim_os = data.select("os").distinct().withColumn("id_os", monotonically_increasing_id())
dim_os = dim_os.select('id_os', 'os')
dim_os.show()

fact_movie_analytics = data.join(dim_device, data.device == dim_device.device, "inner") \
    .join(dim_location, data.location == dim_location.location, "inner") \
    .join(dim_os, data.os == dim_os.os, "inner") \
    .join(dim_browser, data.browser == dim_browser.browser, "inner") \
    .join(dim_date, data.log_date == dim_date.date, "inner") \
    .select(
        "id_device",
        "id_location",
        "id_os",
        "id_browser",
        col("id_date").alias("id_log_date"),
        col("positive_review").alias("review_score"),
    ).withColumn("id_movie_analytics", monotonically_increasing_id())

fact_movie_analytics = fact_movie_analytics.select(
    'id_movie_analytics',
    'id_log_date',
    'id_location',
    'id_os',
    'id_browser',
    'review_score'
)

fact_movie_analytics.show()

fact_movie_analytics.printSchema()
fact_movie_analytics.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option('table', f"{DATASET_ID}.fact_movie_analytics") \
    .save()


dim_date.printSchema()
dim_date.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("writeMethod", "direct") \
    .save(f"{DATASET_ID}.dim_date")


dim_device.printSchema()
dim_device.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", f"{DATASET_ID}.dim_device") \
    .save()


dim_location.printSchema()
dim_location.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", f"{DATASET_ID}.dim_location") \
    .save()


dim_os.printSchema()
dim_os.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", f"{DATASET_ID}.dim_os") \
    .save()

dim_browser.printSchema()
dim_browser.write \
    .format("bigquery") \
    .mode("overwrite") \
    .option("table", f"{DATASET_ID}.dim_browser") \
    .save()

# Stop the SparkSession
spark.stop()
