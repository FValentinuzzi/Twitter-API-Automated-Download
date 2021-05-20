# Databricks notebook source
# DBTITLE 1,Cyber Event Response - Automated Tweets Download 
#### Version 1 features

# Download Tweets in EN from:
import tweepy
from tweepy import OAuthHandler
import pandas as pd

# Change the next four lines based on your own consumer_key, consume_secret, access_token, and access_secret. 
consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

import datetime

today = datetime.date.today().strftime('%Y-%m-%d')
#Add the user Id you want to get tweets
user_id="Account1"
#Add the number of tweets you want to get
number_of_tweets = 200
tweets = []
time = []

for i in tweepy.Cursor(api.user_timeline, id = "Account1", tweet_mode= 'extended').items(number_of_tweets):
  tweets.append(i.full_text)
  time.append(i.created_at)

Account1 = pd.DataFrame({'text':tweets, 'time':time})


##############################################################################################################################################################

#Add the user Id you want to get tweets
user_id="Account2"
#Add the number of tweets you want to get
number_of_tweets = 200
tweets = []
time = []

for i in tweepy.Cursor(api.user_timeline, id = "Account2", tweet_mode= 'extended').items(number_of_tweets):
  tweets.append(i.full_text)
  time.append(i.created_at)

Account2 = pd.DataFrame({'text':tweets, 'time':time})

##############################################################################################################################################################

user_id="Account3"
#Add the user Id you want to get tweets
for i in tweepy.Cursor(api.user_timeline, id = "Account3", tweet_mode= 'extended').items(number_of_tweets):
  tweets.append(i.full_text)
  time.append(i.created_at)

Account3 = pd.DataFrame({'text':tweets, 'time':time})


daily_Tweets = Account1.append([Account2, Account3])
daily_Tweets = spark.createDataFrame(daily_Tweets)
daily_Tweets = daily_Tweets.dropDuplicates()

