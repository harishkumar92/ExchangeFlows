import twitter

def create_api(consumer_key, consumer_secret, access_token_key, access_token_secret):
	api = twitter.Api(consumer_key, consumer_secret, access_token_key, access_token_secret, tweet_mode='extended')
	return api

def pull_all_tweets(api):
	tweets = pull_tweets(api, max_id=None)
	ret_val = tweets

	while (len(tweets) > 0):
		print (tweets[-1])
		max_id = ret_val[-1].id
		tweets = pull_tweets(api, max_id)[1:]
		ret_val += tweets

	return ret_val

def pull_tweets(api, max_id=None):
	ret_val = []
	statuses = api.GetUserTimeline(screen_name='thetokenanalyst', count=200, max_id=max_id)
	for status in statuses:
		if 'BTC exchange on-chain flows' in status.full_text:
			ret_val.append(status)

	return ret_val


def parse_tweets(tweets):
	tweets



if __name__=='__main__':
	consumer_key = 'BgIHvQOg6GRdQxKWToPjyVwcd'
	consumer_secret = 'cuV4VoLdkidtia3V0M0JfL1CLpJ21RAZsq2QZiQKEdY72jUnax'
	access_token_key = '21477398-W6L3ElGiPzfSzm1DwBlGlXxuvV07UQ0PAMsgdd33U'
	access_token_secret = 'YI0u5zOsMhi3zdMBfSRTLhRwhWQUueghJF6pZj8e3JYGL'


	api = create_api(consumer_key, consumer_secret, access_token_key, access_token_secret)
	tweets = pull_all_tweets(api)



