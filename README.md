# Real-time-Reddit-sentiment-analyzer
The repository focusing on sentiment analysis of Reddit posts using SparkNLP, Kafka, and PRAW contains several functions and system design components. Here are some of them:

1. **Function for Reddit Data Scraping**: There is a function called `reddit_scrape(subreddit_name, keyword, limit=50)` that scrapes Reddit posts from a specified subreddit based on a keyword. It uses PRAW to access the Reddit API and retrieve post data.

2. **Function for Sending Data to Kafka**: The function `send_data_to_kafka(df, topic_name)` sends the scraped data to a Kafka topic for further processing.

3. **Function for Reading Data from Kafka**: There is a function `read_data_from_kafka(topic_name, bootstrap_servers='localhost:9092', group_id='my-group', timeout=600, max_messages=100)` that reads data from a Kafka topic and formats it into a Pandas DataFrame.

4. **Function for Sentiment Analysis using SparkNLP**: The function `sentimentAnalysis_usingSparkNLP(df)` performs sentiment analysis on the Reddit posts using SparkNLP. It uses two pre-trained pipelines for sentiment analysis and joins the results to the original DataFrame.

5. **Function for Reading from Kafka and Writing to Hive**: The function `read_from_kafka_and_write_to_hive(posts_df, topic_name, hive_table_name)` reads data from Kafka, transforms it into a structured format, and writes it to a Hive table in Parquet format.

6. **Plotting Function**: There is a function `plot3(pandas_test)` that prepares a histogram plot showing the agreement between sentiment analyses performed by the two SparkNLP pipelines.

These functions and system design components work together to scrape Reddit posts, perform sentiment analysis using SparkNLP, store and process data in Kafka and Hive, and visualize the sentiment analysis results.
