from time import sleep

import pyspark
import openai
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import httpx
# from config.config import config

config = {
    "openai": {
        "api_key": "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"
    },
    "kafka": {
        "sasl.username": "HZSMFHKPVP2JG5AY",
        "sasl.password": "AMYzHj3eQcOyRQ3LUkDebUbwYZjarm6d/w44jF6Pvn8fxwSC6JaLODb+Hfo774QC",
        "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'session.timeout.ms': 50000
    },
    "schema_registry": {
        "url": "https://psrc-wrp99.us-central1.gcp.confluent.cloud",
        "basic.auth.user.info": "AOGI6W3CXFI5OFOE:FnFkOe1HEtvHcngRknpUEv0YZdnRKTwpekvtSHtPQicuJFK5Wz2CMzBu58TR91fa"

    }
}
def sentiment_analysis(comment) -> str:
    url = "http://host.docker.internal/predict"  # Điều chỉnh URL nếu cần thiết
    data = {"text": comment}

    try:
        response = httpx.post(url, data=data)
        response.raise_for_status()  # Nếu có lỗi, raise exception
        result = response.json()
        return result['result'][0]['label']
    except Exception as e:
        return {"success": False, "error": str(e)}
# def sentiment_analysis(comment) -> str:
#     if comment:
#         openai.api_key = config['openai']['api_key']
#         completion = openai.ChatCompletion.create(
#             model='gpt-3.5-turbo',F
#             messages = [
#                 {
#                     "role": "system",
#                     "content": """
#                         You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
#                         You are to respond with one word from the option specified above, do not add anything else.
#                         Here is the comment:
                        
#                         {comment}
#                     """.format(comment=comment)
#                 }
#             ]
#         )
#         return completion.choices[0].message['content']
#     return "Empty"

def start_streaming(spark):
    topic = 'customers_review'
    while True:
        try:
            stream_df = (spark.readStream.format("socket")
                         .option("host", "localhost")
                         .option("port", 9999)
                         .load()
                         )

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

            sentiment_analysis_udf = udf(sentiment_analysis, StringType())

            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             )

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            query = (kafka_df.writeStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                   .option("kafka.security.protocol", config['kafka']['security.protocol'])
                   .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                   .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                           'password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('topic', topic)
                   .start()
                   .awaitTermination()
                )

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)

def test_stream(spark):
    # topic = 'customers_review'
    # while True:
    #     try:
            stream_df= (spark.readStream.format('socket')
                        .option("host",'localhost')
                        .option('port','9999')
                        .load())
            # schema = StructType([
            #         StructField("review_id", StringType()),
            #         StructField("user_id", StringType()),
            #         StructField("business_id", StringType()),
            #         StructField("stars", FloatType()),
            #         StructField("date", StringType()),
            #         StructField("text", StringType())
            #     ])
            # stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))
            # kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            # query = (kafka_df.writeStream
            #         .format("kafka")
            #         .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
            #         .option("kafka.security.protocol", config['kafka']['security.protocol'])
            #         .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
            #         .option('kafka.sasl.jaas.config',
            #                 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
            #                 'password="{password}";'.format(
            #                     username=config['kafka']['sasl.username'],
            #                     password=config['kafka']['sasl.password']
            #                 ))
            #         .option('checkpointLocation', '/tmp/checkpoint')
            #         .option('topic', topic)
            #         .start()
            #         .awaitTermination()
            #         )

            query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start()
            query.awaitTermination()
        # except Exception as e:
        #     print(e)
        #     sleep(10)
if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    test_stream(spark_conn)
