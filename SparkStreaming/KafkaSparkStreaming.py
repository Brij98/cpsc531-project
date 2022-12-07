import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T


KAFKA_TOPIC_NAME = "New_topic_4"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

my_udf = F.udf(lambda x: x.decode('unicode-escape'),T.StringType())

if __name__ == "__main__":

    # creating spark session object
    spark = (
        SparkSession.builder.master("local[*]").
        appName("KafkaSparkStreaming").
        getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # read data stream from kafka topic
    dataFrame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    dataFrame.printSchema()

    #applying the data schema
    inputDataSchema = StructType([
        StructField("age", StringType(), False),
        StructField("sex", StringType(), False),
        StructField("cp", StringType(), False),
        StructField("trestbps", StringType(), False),
        StructField("chol", StringType(), False),
        StructField("fbs", StringType(), False),
        StructField("restecg", StringType(), False),
        StructField("thalach", StringType(), False),
        StructField("exang", StringType(), False),
        StructField("oldpeak", StringType(), False),
        StructField("slope", StringType(), False),
        StructField("ca", StringType(), False),
        StructField("thal", StringType(), False),
    ])

    # clean_DataFrame = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .withColumn(my_udf("value"), inputDataSchema) \
    #     .select("key", col('value.*'))

    clean_DataFrame = dataFrame \
        .select(from_json(my_udf(col("value")), inputDataSchema))

    clean_DataFrame.printSchema()

    posts_stream = clean_DataFrame.writeStream.trigger(processingTime='5 seconds') \
        .outputMode('update') \
        .option("truncate", "false") \
        .format("console") \
        .start()

    posts_stream.awaitTermination()
