from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProcessLogReview").getOrCreate()

BUCKET_NAME = 'movie-review-bucket'
LOG_REVIEW_OBJECT_KEY = 'log_review.csv'

try:
    print("Running spark file...")
    df = spark.read.csv(f'gs://{BUCKET_NAME}/{LOG_REVIEW_OBJECT_KEY}', inferSchema=True, header=True, sep=',')


    logdf = df.selectExpr(
        "id_review AS log_id",
        "xpath_string(log, 'reviewlog/log/logDate') AS log_date",
        "xpath_string(log, 'reviewlog/log/device') AS device",
        "xpath_string(log, 'reviewlog/log/location') AS location",
        "xpath_string(log, 'reviewlog/log/os') AS os",
        "xpath_string(log, 'reviewlog/log/phoneNumber') AS phone_number",
    )

    logdf.printSchema()
    logdf.coalesce(1).write.csv(f'gs://{BUCKET_NAME}-staging/log_review/', mode='append')
    spark.stop()

except Exception as e:
    print("An Error occured...")
    print(e)
    raise e


