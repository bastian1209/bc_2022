import argparse
import tweepy
import pandas as pd
from tqdm import tqdm


def parse_data(tweets_generator, limit=1000):
    ids = []
    created_ats = []
    retweet_counts = []
    reply_counts = []
    like_counts = []
    tags = []
    texts = []
    
    for tweet in tqdm(tweets_generator.flatten(limit=limit)):
        tweet = tweet.data
        ids.append(tweet['id'])
        created_ats.append(tweet['created_at'])
        retweet_counts.append(tweet['public_metrics']['retweet_count'])
        reply_counts.append(tweet['public_metrics']['reply_count'])
        like_counts.append(tweet['public_metrics']['like_count'])
        texts.append(tweet['text'])
        
        try:
            hashtags = " ".join(list(map(lambda x : x['tag'], tweet['entities']['hashtags'])))
            
        except:
            hashtags = None

        tags.append(hashtags)
        
    df = pd.DataFrame({'id' : ids, 'created_at' : created_ats, 'retweet' : retweet_counts, 'reply' : reply_counts, 'like' : like_counts, 'hashtag' : hashtags, 'text' : texts})
    df['count'] = 1

    return df



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query-tag', type=str, default='bayc')
    parser.add_argument('--num-post-limit', type=int, default=10000)
    args = parser.parse_args()

    consumer_key = "9K5PrCQxxwI0OGMKE9G0Stb4j"
    consumer_key_secret = "tQUTp5SKJCWuR7FgGKzjk0YovD54R6k4MrvYQGv8xcuFMQQD4Z"
    access_token = "1310440475371814912-XtMcLHTHWoILvL8VLpx2pZtkvefeyS"
    access_token_secret = "KedbLZ5pmxrLKxwN2stjnJsT5MsiXtlHXrclcbfLpl9He"
    b_token = "AAAAAAAAAAAAAAAAAAAAALvJbgEAAAAA5owyA7OauLtVgo6HhdKQc8%2BQyZk%3DebnzCgAUDh0eTCNQS1XX4ImulAfhdhob89xHMTujziBLdX8yiB"

    client = tweepy.Client(bearer_token=b_token, 
                        consumer_key=consumer_key, 
                        consumer_secret=consumer_key_secret, 
                        access_token=access_token, 
                        access_token_secret=access_token_secret)

    query = '#{} "nft" -is:retweet'.format(args.query_tag)

    tweets_generator = tweepy.Paginator(client.search_recent_tweets, query=query,
                                tweet_fields=['text', 'public_metrics', 'context_annotations', 'created_at', 'entities'], max_results=100)

    data = parse_data(tweets_generator, limit=args.num_post_limit)
    data = data.sort_values('created_at', ascending=False)
    data.set_index('id').to_csv('crawled_{}.csv'.format(query.split()[0].replace('#', '')), encoding='utf-8')