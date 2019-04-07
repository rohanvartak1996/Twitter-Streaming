import tweepy
import os
import pandas as pd
from pymongo import MongoClient
import json
import datetime

class DbWriter(tweepy.StreamListener):
    
    def __init__(self, client):
        self.client = MongoClient(client)
        self.db = self.client.twitterdb
        self.collection = self.db.twitter
        #self.tracks = tracks
        #self.use_redis = use_redis
        #self.count = 0
        #self.metric_client = metric_client
        #self.currency_dict=currency_dict
        #self.redis_client = redisSlidingWindow.SlidingWindow(debugOn = True, host = redis_conn, port=6379, queueSize = 200)
        #self.redis_coin_client_dict = {currency: redisSlidingWindow.SlidingWindow(debugOn = True, host = redis_conn, port=6379, queueSize = 200, newsQueueName = currency) for currency in currency_dict.values()}
        super().__init__()

    #def send_redis(self, redis_dict):
        #redis_dict['source'] = redis_dict['source']['name']
        #currency = redis_dict['query_params']['currency']
        #redis_dict = {k:str(redis_dict[k]) for k in redis_dict}
        #self.redis_client.insertObject(redis_dict['id'], redis_dict)
        #self.redis_coin_client.insertObject(f"twitter_{currency}_{redis_dict['id']}", redis_dict)

    def on_status(self, status):
        try:
            if not status.retweeted:
                #for currency in self.get_entities(status.text):
                base_dict = status._json
                base_dict['twitter_entities'] = base_dict.pop('entities')
                tweet_dict = {'description': status.user.description,
                            'text': getattr(status, 'extended_tweet', {}).get('full_text') or status.text,
                            'user_name': status.user.screen_name,
                            'followers': status.user.followers_count,
                            'retweets': status.retweet_count,
                            'id': status.id,
                            'collected_at': datetime.datetime.now()}
                update_dict = {**base_dict, **tweet_dict}
                print(tweet_dict['collected_at'], tweet_dict['text'])
                self.collection.update_one({'_id':status.id}, {'$set': update_dict}, upsert=True)
                    #if self.use_redis:
                        #self.send_redis(tweet_dict)
                    #self.count = self.count + 1
                    #self.metric_client.incr("mongo_record_insert")
                    #if self.count % 100 == 0:
                        #print(f'Streamed {self.count} records for tracks {self.tracks}')
        except Exception as e:
            print(f'Hit exception {e} while streaming {self.tracks}')

    #def on_error(self, status_code):
        #self.metric_client.incr("twitter_stream_exception")
        # TODO probably log this into Mongo
        #print(f'Failed with status code {status_code}')
        #return True

    #def on_timeout(self, status_code):
        #self.metric_client.incr("twitter_stream_timeout")
        # TODO probably log this into Mongo
        #print(f'Timed out, restarting')
        #return True

    #def on_limit(self, track):
        #print(f"Hit limit for {track}")

    #def get_entities(self, text_data):
        #entities = []
        #for key in self.currency_dict:
            #if key in text_data:
                #entities.append(self.currency_dict[key])
        #return entities
