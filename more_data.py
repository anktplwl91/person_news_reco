"""
This file traverses through all the Medium users(also on Twitter with same user_id), traverses through all of their friends on Twitter, stores those who are also on Medium, and collects the Medium RSS feeds from all these users. All feeds are cleaned, tokenized and stemmed and stored in ES. This corpus will finally be used to train LDA model 
(1) server_addr : ES server address
(2) user_key : user's key for Twitter API connection
(3) user_secret : user's secret key for Twitter API connection
(4) token1 and token2 : tokens app provides during oauth authentication
(5) medium_users.txt : twitter ids of all medium users stored in pickle format -- 17,000 users
We can only send 15 requests in a 15-min window using Twitter API
"""

import oauth2
import pprint
import json
import feedparser
from elasticsearch import Elasticsearch, helpers
from nltk import RegexpTokenizer
from nltk.corpus import stopwords
import time
import pickle
import re

tokenizer = RegexpTokenizer(r'[a-zA-Z_]+')
es = Elasticsearch(server_addr)

def oauth_req(url, key, secret, http_method="GET", post_body="", http_headers=None):

	consumer = oauth2.Consumer(key=user_key, secret=user_secret)
	token = oauth2.Token(key=key, secret=secret)
	client = oauth2.Client(consumer, token)
	resp, content = client.request(url, method=http_method, body=post_body, headers=http_headers)
	return content

def clean_text(text_list):

	new_text_list = []
	for i in range(len(text_list)):
		text = re.sub(r"<.*?>", "", text_list[i])
		tokens = tokenizer.tokenize(text)
		new_tokens = [tok.lower() for tok in tokens if tok not in stopwords.words('english') and len(tok) > 2]
		new_text_list.append(new_tokens)

	return new_text_list

def save_to_es(user_id, doc_first_list, doc_second_list):

	actions = []
	for d in range(len(doc_first_list)):
		action = {
			'_op_type' : 'create',
			'_index' : 'data_dump',
			'_type' : 'medium',
			'_source' : {
				'user_id' : user_id,
				'medium_title' : doc_first_list[d],
				'medium_summary' : doc_second_list[d]
			}
		}
		actions.append(action)

	es_status = helpers.bulk(es, actions)
	return es_status

twitter_set = pickle.load(open("medium_users.txt", "rb"))
twitter = list(twitter_set)

for user in twitter:
	print 'user %s activated' % user
	cursor_list = [-1]
	users_list = []
	medium_title = []
	medium_summary = []

	url_string = "https://api.twitter.com/1.1/users/show.json?user_id="+str(user)
	u = oauth_req(url_string, token1, token2)
	u_dict = json.loads(u)
	users_list.append(u_dict["screen_name"])

	for i in range(1, 15):
		try:
			url_string = 'https://api.twitter.com/1.1/friends/list.json?cursor='+str(cursor_list[-1])+'&user_id='+str(user)+'&count=200&skip_status=true&include_user_entities=false'
			friends = oauth_req(url_string, token1, token2)
			friends_dict = json.loads(friends)
			cursor_list.append(friends_dict['next_cursor'])
			for frnd in friends_dict['users']: users_list.append(frnd['screen_name'])
		
		except Exception, e:
			print e
			continue

	print 'friend list built...'

	curr_time = time.time()

	for us in users_list:
		try:
			d = feedparser.parse('https://medium.com/feed/@'+us)
			if len(d['entries']) > 0:
				for entry in d['entries']:
					medium_title.append(entry['title'])
					medium_summary.append(entry['summary'])

		except Exception, e:
			print e
			continue

	medium_title_clean = clean_text(medium_title)
	medium_summary_clean = clean_text(medium_summary)

	print 'saving to elasticsearch'
	print save_to_es(user, medium_title_clean, medium_summary_clean)
	
	tm_ep = time.time() - curr_time
	if tm_ep > 900: continue
	else:
		print 'sleeping for %s seconds...' % (900-tm_ep)
		time.sleep(900-tm_ep)
