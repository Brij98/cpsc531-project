from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import *

KAFKA_TOPIC_NAME = "sampleTopic"
KAFKA_BOOTSTRAP_SERVER = "127.0.0.1:9000"

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

    inputDataSchema = StructType([
        StructField("age", IntegerType(), True),
        StructField("sex", IntegerType(), True),
        StructField("cp", IntegerType(), True),
        StructField("trestbps", IntegerType(), True),
        StructField("chol", IntegerType(), True),
        StructField("fbs", IntegerType(), True),
        StructField("restecg", IntegerType(), True),
        StructField("thalach", IntegerType(), True),
        StructField("exang", IntegerType(), True),
        StructField("oldpeak", FloatType(), True),
        StructField("slope", IntegerType(), True),
        StructField("ca", IntegerType(), True),
        StructField("thal", IntegerType(), True),
    ])

    clean_DataFrame = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .withColumn("value", to_json("value", inputDataSchema))\
        .select("key", col('value.*'))

    clean_DataFrame.printSchema()
