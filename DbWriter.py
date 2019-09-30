import tweepy
import os
import pandas as pd
from pymongo import MongoClient
import json
import datetime

class DbWriter(tweepy.StreamListener):                     # This class collects the tweets and stores them in Mongo Db
    
    def __init__(self, client):
        self.client = MongoClient(client)                 # Intialize the Mongo Db
        self.db = self.client.twitterdb                   # connect to database
        self.collection = self.db.twitter                 # set a variable for your collection
        super().__init__()

    def on_status(self, status):                         # This method gets called when a tweet is made
        try:
            if not status.retweeted:                      # Collect the tweet is it is not a retweet
                base_dict = status._json
                base_dict['twitter_entities'] = base_dict.pop('entities')
                tweet_dict = {'description': status.user.description,             # Store the field required in a dictionary
                            'text': getattr(status, 'extended_tweet', {}).get('full_text') or status.text,
                            'user_name': status.user.screen_name,
                            'followers': status.user.followers_count,
                            'retweets': status.retweet_count,
                            'id': status.id,
                            'collected_at': datetime.datetime.now()}
                update_dict = {**base_dict, **tweet_dict}
                print(tweet_dict['collected_at'], tweet_dict['text'])             # Print the text of the tweet
                self.collection.update_one({'_id':status.id}, {'$set': update_dict}, upsert=True)  # Store the tweet in Mongo DB
        except Exception as e:
            print(f'Hit exception {e} while streaming {self.tracks}')
