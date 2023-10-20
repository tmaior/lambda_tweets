import json
import boto3
import tweepy
import os
# Configurar o cliente do AWS Lambda
lambda_client = boto3.client('lambda')

# Twitter API Credentials
API_KEY = os.environ.get("API_KEY")
API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
CONSUMER_KEY = os.environ.get("CONSUMER_KEY")

# Nome ou ARN da função Lambda de destino
function_name = 'aiidea-llc-dev-lambda-process-data'

# Initialize Tweepy Client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

def fetch_tweets(query, count=100):
    try:
        # Fetch tweets using the Tweepy API.
        tweets = client.search_recent_tweets(query=query, tweet_fields=['author_id', 'lang', 'created_at'])
        return tweets.data
    except:
        return error

def handler(event, context):
    try:

        if 'queryStringParameters' in event:
            query_parameters = event['queryStringParameters']
            if 'query' in query_parameters:
                query_value = query_parameters['query']
      
                tweets = fetch_tweets(query_value)
                
                data_list = []
                for tweet in tweets:
                    data = {
                        "ID": tweet.id,
                        "Raw_Content": tweet.text
                    }
                    data_list.append(data)
                
         
                response = lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(data_list)
                )
                

                return {
                    "statusCode": 201,
                    "body": json.dumps("Data uploaded")
                }
        else:
            return {
                "statusCode": 400,
                "body": "'Query' param not found in the url."
            }
    except Exception as error:
        print(error)
        return {
            "statusCode": 400,
            "body": "error"
        }


