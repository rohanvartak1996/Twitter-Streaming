from multiprocessing import Process
import os
from pymongo import MongoClient
import pandas as pd
import tweepy
import yaml
import tweepy
import http
from DbWriter import DbWriter

queries = pd.read_csv('cryptoinfluencers.csv') # Read the profile names and used ids of the profiles who's tweets are to be collected
queries = queries.astype(str)                   # Convert everything to string

if os.path.isfile("config.yml"):                 # Load the config file
    with open("config.yml", 'r') as ymlfile:
        config_file = yaml.load(ymlfile)
else:
    config_file = {}

config = {}
for conf_item in ['CONSUMER_KEY', 'CONSUMER_SECRET', 'ACCESS_TOKEN', 'ACCESS_TOKEN_SECRET']:    #Load the keys for twiiter
    config[conf_item] = os.getenv(conf_item, config_file.get(conf_item))


auth = tweepy.OAuthHandler(config['CONSUMER_KEY'], config['CONSUMER_SECRET'])
auth.set_access_token(config['ACCESS_TOKEN'], config['ACCESS_TOKEN_SECRET'])

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)   # Authenticate twitter using the keys

def stream_tweets(tracks):                                           # Function for collecting tweets and storing to Mongo Db
    print('Streaming tweets')
    custom_listener = DbWriter('mongodb://localhost/twitterdb')     # Specify the Mongo Db connection
    stream = tweepy.Stream(auth = api.auth,                         # Create a streamer
                           listener=custom_listener,
                           tweet_mode='extended')
    try:
        stream.filter(follow=tracks)                               # start the streamer
    except http.client.IncompleteRead:
        print("Incomplete read, restarting")
        stream.disconnect()
        stream_tweets(tracks)
    except KeyboardInterrupt:
        stream.disconnect()

tracks = list(queries['userid'])                                   # make a list of user ids
stream_tweets(tracks)                                              # start the streaming of the tweets

while True:
    pass
