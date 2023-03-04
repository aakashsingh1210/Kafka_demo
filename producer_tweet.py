import json
import tweepy
import time
import os
from dotenv import load_dotenv
from loguru import logger
from kafka import KafkaProducer
import requests

load_dotenv()

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=json_serializer
)

def stream_it(data):
    producer.send('tweets',data)
    time.sleep(4)
    
# key provided by tweeter developer account
api_key=os.environ['api_key']          
api_secret=os.environ['api_key_secret']
access_token=os.environ['access_token']
access_token_secret=os.environ['access_token_secret']
bearer_token=os.environ['bearer_token']

#creating authentication object 

auth=tweepy.OAuthHandler(api_key,api_secret)           
auth.set_access_token(access_token,access_token_secret)

api=tweepy.API(auth) # creating api object by passing auth information 

client = tweepy.Client( bearer_token=bearer_token, 
                        consumer_key=api_key, 
                        consumer_secret=api_secret, 
                        access_token=access_token, 
                        access_token_secret=access_token_secret, 
                        return_type = requests.Response,
                        wait_on_rate_limit=True)

query = 'Virat Kohli'

# get max. 100 tweets
tweets = client.search_recent_tweets(query=query, 
                                    tweet_fields=['author_id', 'created_at'],
                                     max_results=100)          
tweets=tweets.json()   

# for tweet in tweets['data']:
#     print(tweet)

for tweet in tweets['data']:
    text=tweet['text']
    created_at=tweet['created_at']
    data={'text':text,'created_at':created_at}
    print(data)
    stream_it(data)
    logger.info("-----------------------------------------")