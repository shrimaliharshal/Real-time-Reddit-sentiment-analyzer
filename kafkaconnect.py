import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import praw
import pandas as pd
import numpy as np
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import lit
from functools import reduce
from pyspark.sql import DataFrame
from kafka import KafkaProducer
import json
import creds

reddit = praw.Reddit(client_id=creds.CLIENT_ID,
                     client_secret= creds.CLIENT_SECRET,
                     user_agent=creds.USER_AGENT)


sparksession = SparkSession.builder \
    .appName("Reddit Data Processing") \
    .master("local") \
    .getOrCreate()

spark = sparknlp.start()

pipeline = PretrainedPipeline('analyze_sentimentdl_glove_imdb', 'en')
pipeline2 = PretrainedPipeline("analyze_sentimentdl_use_imdb", lang = "en")

def analyze_keyword(keywordforscraping):
    print(f"Starting processing for keyword: {keywordforscraping}")
    try:

        def scrape_posts_by_keyword(subreddit_name, keyword, limit=50):
            subreddit = reddit.subreddit(subreddit_name)
            posts_data = []

            for post in subreddit.search(keyword, limit=limit):
                post_data = {
                    'title': post.title,
                    'author': str(post.author),
                    'score': post.score,
                    'id': post.id,
                    'url': post.url,
                    'created_utc': post.created_utc,
                    'selftext': post.selftext,
                    'num_comments':post.num_comments
                }
                posts_data.append(post_data)

            return pd.DataFrame(posts_data)
        # posts_df = scrape_posts_by_keyword('shortstories', keyword)
        posts_df = scrape_posts_by_keyword('shortstories', keywordforscraping)
        if posts_df.empty:
            print(f"No posts found for keyword: {keywordforscraping}")
            return None  # Return None or an empty DataFrame
        posts_df['created_utc'] = pd.to_datetime(posts_df['created_utc'], unit='s')

        def clean_text(text):# Remove URLs

          text = re.sub(r'http\S+', '', text)
          # Remove anything that is not a letter, number, punctuation, or whitespace
          text = re.sub(r'[^\w\s\.,!?]', '', text)
          return text

        posts_df['selftext'] = posts_df['selftext'].apply(clean_text)

        # Convert to Spark DataFrame, preprocess, analyze sentiment, etc.
        spark_df = spark.createDataFrame(posts_df)
        print(f"Converted to Spark DataFrame for keyword: {keywordforscraping}, Number of posts: {spark_df.count()}")

        
        # Rename column for sentiment analysis
        prepared_df = spark_df.withColumnRenamed("selftext", "text")

        # Analyze sentiment with both pipelines
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
        final_df = final_df.withColumn("keyword", lit(keywordforscraping))
        
        print(f"Processing keyword: {keywordforscraping}")
       
        print(f"Number of posts for {keywordforscraping}: {final_df.count()}")

        final_df.show(5)
        print(f"Completed processing for keyword: {keywordforscraping}")
        return final_df 

    except Exception as e:
        print(f"Error processing keyword {keywordforscraping}: {e}")
        return None  # Return None or an empty DataFrame on error
def send_data_to_kafka(df, topic_name, bootstrap_servers='localhost:9092'):
    """
    Sends data from a Pandas DataFrame to a Kafka topic.
    
    Parameters:
    - df: The Pandas DataFrame containing the data to be sent.
    - topic_name: The name of the Kafka topic to send the data to.
    - bootstrap_servers: The Kafka bootstrap servers. Default is 'localhost:9092'.
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for index, row in df.iterrows():
        # Convert the row to a dictionary and send it to Kafka
        producer.send(topic_name, row.to_dict())
    
    producer.flush()
    producer.close()
def scrape_keyword(subreddit_name, keyword, limit=50):
            subreddit = reddit.subreddit(subreddit_name)
            posts_data = []

            for post in subreddit.search(keyword, limit=limit):
                post_data = {
                    'title': post.title,
                    'author': str(post.author),
                    'score': post.score,
                    'id': post.id,
                    'url': post.url,
                    'created_utc': post.created_utc,
                    'selftext': post.selftext,
                    'num_comments':post.num_comments
                }
                posts_data.append(post_data)

            return pd.DataFrame(posts_data)
thriller_df = scrape_keyword('shortstories', 'Thriller')
thriller_df.head()
