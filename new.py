from kafka import KafkaConsumer
import json
import pandas as pd
import time
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

def read_data_from_kafka(topic_name, bootstrap_servers='localhost:9092', group_id='my-group', timeout=600, max_messages=100):
    """
    Reads data from a Kafka topic and formats it into a Pandas DataFrame.
    
    Parameters:
    - topic_name: The name of the Kafka topic to read from.
    - bootstrap_servers: The Kafka bootstrap servers. Default is 'localhost:9092'.
    - group_id: The consumer group ID. Default is 'my-group'.
    - timeout: The maximum time in seconds to wait for messages. Default is 60 seconds.
    - max_messages: The maximum number of messages to process. Default is 100.
    """
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=group_id
    )
    
    posts_data = []
    start_time = time.time()
    while time.time() - start_time < timeout:
        messages = consumer.poll(timeout_ms=1000, max_records=max_messages)
        if not messages:
            break
        for tp, msgs in messages.items():
            for msg in msgs:
                post_data = msg.value
                posts_data.append(post_data)
                if len(posts_data) >= max_messages:
                    break
        if len(posts_data) >= max_messages:
            break
    
    # Convert the list of dictionaries to a Pandas DataFrame
    posts_df = pd.DataFrame(posts_data)
    
    # Close the consumer
    consumer.close()
    
    return posts_df

def read_data_from_kafka(topic_name, bootstrap_servers='localhost:9092', group_id='my-group', timeout=60, max_messages=100):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=group_id
    )
    
    posts_data = []
    start_time = time.time()
    while time.time() - start_time < timeout:
        messages = consumer.poll(timeout_ms=1000, max_records=max_messages)
        if not messages:
            print("No messages received.")
            break
        for tp, msgs in messages.items():
            for msg in msgs:
                post_data = msg.value
                print(f"Received message: {post_data}")
                posts_data.append(post_data)
                if len(posts_data) >= max_messages:
                    break
        if len(posts_data) >= max_messages:
            break
    
    # Print the list of dictionaries to ensure it's being populated correctly
    print("Posts data:", posts_data)
    
    # Convert the list of dictionaries to a Pandas DataFrame
    posts_df = pd.DataFrame(posts_data)
    consumer.close()
    return posts_df


reddit = praw.Reddit(client_id='R2hyuU2V_wnLGBZyFEVLtw',
                     client_secret='ayv3CTLaTdIot3qpXqqoGCrQq8KM3A',
                     user_agent='web bot')



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
        
        producer.send(topic_name, row.to_dict())
    
    producer.flush()
    producer.close()

def reddit_scrape(subreddit_name, keyword, limit=50):
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
                        # 'created_utc': post.created_utc,
                        'selftext': post.selftext,
                        'num_comments':post.num_comments
                    }
                    posts_data.append(post_data)

                return pd.DataFrame(posts_data)
    posts_df = scrape_keyword('shortstories', keyword)
    if posts_df.empty:
            print(f"No posts found for keyword: {keyword}")
            return None  # Return None or an empty DataFrame
    # posts_df['created_utc'] = pd.to_datetime(posts_df['created_utc'], unit='s')

    def clean_text(text):# Remove URLs

        text = re.sub(r'http\S+', '', text)
        # Remove anything that is not a letter, number, punctuation, or whitespace
        text = re.sub(r'[^\w\s\.,!?]', '', text)
        return text

    posts_df['selftext'] = posts_df['selftext'].apply(clean_text)
    return posts_df

suspense_df = reddit_scrape('shortstories', 'suspense')

send_data_to_kafka(suspense_df, 'redditData')


kafka_df = read_data_from_kafka('redditData')
print(kafka_df.head(5))
