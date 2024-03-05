from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField
import new
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import col,expr
from pyspark.sql.functions import lit

suspense_df = new.reddit_scrape('shortstories', 'suspense')

new.send_data_to_kafka(suspense_df, 'redditData')

sparksession = SparkSession.builder \
    .appName("Reddit Data Processing") \
    .master("local") \
    .getOrCreate()
posts_df = new.read_data_from_kafka('redditData')
def sentimentAnalysis_usingSparkNLP(df):
    spark_df = spark.createDataFrame(posts_df)
    
    
            
    
    prepared_df = spark_df.withColumnRenamed("selftext", "text")
    
    # Analyze sentiment with both pipelines
    pipeline = PretrainedPipeline('analyze_sentimentdl_glove_imdb', 'en')
    pipeline2 = PretrainedPipeline("analyze_sentimentdl_use_imdb", lang = "en")
    result_df1 = pipeline.transform(prepared_df)
    result_df2 = pipeline2.transform(prepared_df)
    
            # Extract sentiment and sentiment score from the first pipeline's results
    test1 = result_df1.withColumn("sentiment1", expr("sentiment.result[0]"))
            
    
            # Extract sentiment and sentiment score from the second pipeline's results
    test2 = result_df2.withColumn("sentiment2", expr("sentiment.result[0]"))
            
            # Join the original DataFrame with the first pipeline's results
    combined_df = spark_df.join(test1.select("title", "sentiment1"), "title")
    
            # Join the combined DataFrame with the second pipeline's results
    final_df = combined_df.join(test2.select("title", "sentiment2"), "title")
    return final_df
    
        

def read_from_kafka_and_write_to_hive(topic_name, hive_table_name, bootstrap_servers='localhost:9092', sparksession : SparkSession):
    post_df = sparksesison \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()
    
    
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("sentiment1", StringType(), True),
        StructField("sentiment2", StringType(), True),
        
    ])
    
    df = df.selectExpr("CAST(value AS STRING)") \
           .select(from_json(col("value"), schema).alias("data")) \
           .select("data.*")
    
    # Writing to Hive in Parquet format
    df.writeStream \
      .format("parquet") \
      .option("path", "path/to/hive/table/location") \
      .option("checkpointLocation", "path/to/checkpoint/dir") \
      .start(hive_table_name) \
      .awaitTermination()
