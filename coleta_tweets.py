import json
from kafka import KafkaProducer, KafkaConsumer
import tweepy
from datetime import datetime
import pandas as pd
import hashlib


class Producers:
    def __init__(self):
        self.broker = 'localhost:9092'
        self.topico = 'twitters'
        self.resp = ""
        self.prod = KafkaProducer(bootstrap_servers=[self.broker],
                                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_msg(self):
        data_hora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dados = {"tweet": self.resp, "horario": data_hora}

        self.prod.send(self.topico, value=dados)


def create_user_locs(tweets):
    user_locs = []
    for tweet in tweets:
        username = str(tweet.user.screen_name.lower().encode('ascii', errors='ignore'))
        description = str(tweet.user.description.lower().encode('ascii', errors='ignore'))
        location = str(tweet.user.location.lower().encode('ascii', errors='ignore'))
        following = str(str(tweet.user.friends_count).lower().encode('ascii', errors='ignore'))
        followers = str(str(tweet.user.followers_count).lower().encode('ascii', errors='ignore'))
        totaltweets = str(str(tweet.user.statuses_count).lower().encode('ascii', errors='ignore'))
        retweetcount = str(str(tweet.retweet_count).lower().encode('ascii', errors='ignore'))
        text = str(tweet.text.lower().encode('ascii', errors='ignore'))
        th_tweet = [username, description, location, following, followers, totaltweets, retweetcount, text]
        user_locs.append(th_tweet)

    return user_locs


def hash_username(user_list):
    length = range(len(user_list))
    for i in length:
        user_list[i][0] = hashlib.md5(user_list[i][0].encode()).hexdigest()
    return user_list


def create_csv(data, file_name):
    df = pd.DataFrame(data=data, columns=['username', 'description', 'location', 'following',
                                          'followers', 'totaltweets', 'retweetcount', 'text'])

    df.to_csv(file_name)


if __name__ == "__main__":

    contadorTweets = 0
    consumer_key = "sEZiPCVGrw08LCL3MVd0OfR4F"
    consumer_secret = "aw2dQ8MFwDsfiYrb3zQlcXZowXVVt5LYmOl90rpVQOHad1JMw2"
    access_token = "3547470323-iOkfc7BEoHxx05nscDFWbYiJo1UkU5b6scmdTGZ"
    access_token_secret = "fTGad6pNSCJowEsnQoklgxJq9NhLmHKgfiCDjodbSmPAB"

    hashtag = "#Covid" + " -filter:retweets"
    numbertweets = 10
    file_name = "twitter_arq.txt"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    tweets = tweepy.Cursor(api.search, q=hashtag, lang="pt").items(numbertweets)

    # Simulando um producer para um processo streaming
    prod = Producers()

    for tweet in tweets:
        print(tweet)
        prod.resp = tweet._json
        prod.send_msg()
        contadorTweets += 1

    data = create_user_locs(tweets)
    data_hash = hash_username(data)
    create_csv(data_hash, file_name)

    print("Foram coletados " + str(contadorTweets) + " tweets.")
