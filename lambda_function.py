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
function_name = 'aiidea-llc-dev-lambda-proccess_data'

# Initialize Tweepy Client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

def fetch_tweets(query, count=100):
    try:
        # Fetch tweets using the Tweepy API.
        tweets = client.search_recent_tweets(query=query, tweet_fields=['author_id', 'lang', 'created_at'])
        return tweets.data
    except:
        print(error)

def handler(context, event):
    try:
        
        tweets = fetch_tweets(context['query'])
        data_list = []
        for tweet in tweets:
            data = {
                "ID": tweet.id,
                "Raw_Content": tweet.text
            }
            data_list.append(data)
        
        # Chamar a função Lambda
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',  # Use 'Event' para chamada assíncrona
            Payload=json.dumps(data_list) # Converte os dados em JSON
        )
        
        # Recuperar a resposta da função Lambda
        response_payload = response['Payload'].read()
        response_data = json.loads(response_payload)
        
   
        # Processar a resposta
        return {
            "code": 200,
            "body": json.dumps(response_data)
        }
  
    except Exception as error:
        print(error)
        return {
            "code": 400,
            "body": "error"
        }


