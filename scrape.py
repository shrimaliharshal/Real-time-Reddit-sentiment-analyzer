import creds
import praw
import pandas as pd

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

reddit = praw.Reddit(client_id=creds.CLIENT_ID,
                     client_secret= creds.CLIENT_SECRET,
                     user_agent=creds.USER_AGENT)
comedy_df = scrape_keyword('shortstories',' comedy')
print(comedy_df.head())