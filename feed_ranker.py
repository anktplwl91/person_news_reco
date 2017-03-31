"""
This file is the main thread of whole module - it collects data from elasticsearch, uses "user_server" for user simulation and also for calculating fraction of important news in top-10 streamed.
(1) server_addr : ES server addr
(2) dict_name : name of dictionary created using Gensim
(3) model_name : name of lda model trained
(4) user_frac_accuracy : for each user stores fraction of important news in top-10 in each iteration
(5) news_feeds_dict : stores the text in each news feed and its top-3 contributing topics
(6) user_interests : list of topic-ids that are of interest for each user
(7) user_topic_weights : stores the importance weight for each topic for all users
(8) user_buckets : stores for each user news-feeds that he has attached as important to him
"""

from gensim.models import LdaModel
from gensim.corpora import Dictionary
from elasticsearch import Elasticsearch, helpers
import numpy as np
from user_server import Server
import random
import time
import math
from scipy.spatial.distance import cosine


es = Elasticsearch(server_addr, timeout=300)
us_server = Server()

user_frac_accuracy = {"user_1": [], "user_2": [], "user_3": [], "user_4": [], "user_5": [], "user_6": [], "user_7": [], "user_8": [], "user_9": [], "user_10": []}

dictionary = Dictionary.load(dict_name)
lda_model = LdaModel.load(model_name)

query = {
	"query": {
		"match_all": {}
	}
}


print "collecting news-feeds..."
news_feeds_dict = {}
result = helpers.scan(es, query, scroll=u"5m", index="news_topics", doc_type="feeds")
for res in result:
	news_feeds_dict[(res["_source"]["news_id"])] = (res["_source"]["news_text"], res["_source"]["news_topics"])

topic_ids = [i for i in range(20)]

user_interests = {"user_1": [0, 1, 3, 6, 9, 12, 13, 17]}

user_topic_weights = {"user_1": {key: random.random() for key in topic_ids}, "user_2": {key: random.random() for key in topic_ids}, "user_3": {key: random.random() for key in topic_ids}, "user_4": {key: random.random() for key in topic_ids}, "user_5": {key: random.random() for key in topic_ids}, "user_6": {key: random.random() for key in topic_ids}, "user_7": {key: random.random() for key in topic_ids}, "user_8": {key: random.random() for key in topic_ids}, "user_9": {key: random.random() for key in topic_ids}, "user_10": {key: random.random() for key in topic_ids}}

'''
topic_penalizer starts by separating user selected news and unselected ones and calculates top-k contributing topics from each news feed in both lists. Then, for each topic in user_selected_news it calculates topic's weight by summing the products of topic prob in that news and current weight of that topic for that user. The same is done for unselected_news but here weights are calculated in negative, so that resultant topic_weights can increase/decrease accordingly. Finally, it updates user_topic_weights 
'''

def topic_penalizer(user_buckets, feed_topic_dict):

	for user in user_buckets.keys():
		user_topic = user_topic_weights[user]
		
		user_news_list = user_buckets[user]
		unselect_list = [feed for feed in feed_topic_dict.keys() if feed not in user_news_list]
	
		user_topic_list = [sorted(feed_topic_dict[news], key=lambda t: t[1], reverse=True)[:5] for news in user_news_list]
		unsel_topic_list = [sorted(feed_topic_dict[news], key=lambda t: t[1], reverse=True) for news in unselect_list]
		unselect_topic_list = [[topic for topic in topic_list if topic[0] not in user_interests[user]][:5] for topic_list in unsel_topic_list]

		new_user_topic_list = [sorted(topic_list, key=lambda t: t[0]) for topic_list in user_topic_list]
		new_unselect_topic_list = [sorted(topic_list, key=lambda t: t[0]) for topic_list in unselect_topic_list]

		pos_topic_weight_dict = {}
		for row, topic_list in enumerate(new_user_topic_list):
			for topic_outer in topic_list:
				t_id = topic_outer[0]
				if t_id in pos_topic_weight_dict.keys(): continue
				else:
					pos_topic_weight_dict[t_id] = topic_outer[1]
					for i in range(row+1, len(new_user_topic_list)):
						idx = 0
						while idx < len(new_user_topic_list[i][idx])-1 and new_user_topic_list[i][idx][0] < t_id: idx += 1
						if t_id == new_user_topic_list[i][idx][0]: pos_topic_weight_dict[t_id] += new_user_topic_list[i][idx][1]

		neg_topic_weight_dict = {}
		for row, topic_list in enumerate(new_unselect_topic_list):
			for topic_outer in topic_list:
				t_id = topic_outer[0]
				if t_id in neg_topic_weight_dict.keys(): continue
				else:
					neg_topic_weight_dict[t_id] = topic_outer[1]
					for i in range(row+1, len(new_unselect_topic_list)):
						idx = 0
						while idx < len(new_unselect_topic_list[i][idx])-1 and new_unselect_topic_list[i][idx][0] < t_id: idx += 1
						if t_id == new_unselect_topic_list[i][idx][0]: neg_topic_weight_dict[t_id] += new_unselect_topic_list[i][idx][1]


		res_topic_weight_dict = {}
		for topic in pos_topic_weight_dict.keys():
			if (topic in neg_topic_weight_dict.keys()) and (topic not in user_interests[user]): res_topic_weight_dict[topic] = 0.3*(pos_topic_weight_dict[topic] - neg_topic_weight_dict[topic])
			else: res_topic_weight_dict[topic] = 0.3*pos_topic_weight_dict[topic]
		for topic in neg_topic_weight_dict.keys():
			if topic in pos_topic_weight_dict.keys(): continue
			else: res_topic_weight_dict[topic] = -(0.3*neg_topic_weight_dict[topic])

		print "resultant - "+str(res_topic_weight_dict)
		for topic in res_topic_weight_dict.keys():
			user_topic[topic] = user_topic[topic] + res_topic_weight_dict[topic]
			
		user_topic_weights[user] = user_topic

'''
ranker calculates for every user feed score for every feed using cosine similarity
'''

def ranker(feed_topic_dict):

	user_feed_scores = {}
	for user in user_topic_weights.keys():
		user_feed_scores[user] = []
		user_topic_list = sorted(user_topic_weights[user].keys())
		topic_weights = [user_topic_weights[user][topic] for topic in user_topic_list]
		min_wt = min(topic_weights)
		max_wt = max(topic_weights)
		if min_wt < max_wt: norm_topic_weights = [float((wt - min_wt)/(max_wt - min_wt)) for wt in topic_weights]		
		else: norm_topic_weights = topic_weights		

		for feed in feed_topic_dict.keys():
			feed_prob_list = sorted([feed_topic_dict[feed][idx] for idx in range(len(feed_topic_dict[feed]))], key=lambda t: t[0])
			prob_list = [tup[1] for tup in feed_prob_list]
			feed_score = cosine(norm_topic_weights, prob_list)
			if math.isnan(feed_score): user_feed_scores[user].append((feed, 0.0, news_feeds_dict[feed][1]))
			else: user_feed_scores[user].append((feed, 1 - feed_score, news_feeds_dict[feed][1]))

	return user_feed_scores

'''
main() starts here, taking 50 feeds at a time(randomly), calling ranker to rank all feeds, sending feeds to user_server for user_attachments to be returned, and then using topic_penalizer to narrow down user interests, finally deleting the 50 news-feeds and continuing. Also calculates the fraction of user selected news in the top-10 ranked 50 news items to judge user-satisfaction.
'''

idx = 0
while len(news_feeds_dict.keys()) > 0:

	print "iteration #" + str(idx)

	topic_frac = [0 for _ in range(20)]
	for news in news_feeds_dict.keys():
        	for topic in news_feeds_dict[news][1]:
		        topic_frac[topic] += 1

	for topic in range(20):
        	count = topic_frac[topic]
        	topic_frac[topic] = float(count / float(3*len(news_feeds_dict.keys())))

	topic_choices = np.random.choice(topic_ids, size=50, p=topic_frac).tolist()
	topic_counts = {key:0 for key in topic_ids}
	for topic in topic_choices:
		topic_counts[topic] += 1

	key_choices = []
	for topic in topic_counts.keys():
		if topic_counts[topic] > 0: 
			news_feeds = [k for k in news_feeds_dict.keys() if news_feeds_dict[k][1][0] == topic]
			if len(news_feeds) >= topic_counts[topic]: key_choices.extend(random.sample(news_feeds, topic_counts[topic]))
			else: key_choices.extend(random.sample(news_feeds, len(news_feeds)))


	feed_topic_dict = {}
	for k in key_choices:
		feed_topic_dict[k] = lda_model.get_document_topics(dictionary.doc2bow(news_feeds_dict[k][0]), minimum_probability=0.00001)

	user_feeds_scores = ranker(feed_topic_dict)
	feed_score_list = sorted(user_feeds_scores["user_1"], key=lambda k: k[1], reverse=True)[:10]
	
	print "user_feeds_scores - " + str(feed_score_list)
	fo.write("user_feeds_scores - " + str({"user_1": feed_score_list}) + "\n")

	action_list = []
	for user in user_feeds_scores.keys():
		for j in range(len(user_feeds_scores[user])):
			action = {
				"_op_type" : "create",
				"_index" : "news_feeds",
				"_type" : "feeds",
				"_source" : {
					"iteration" : idx,
					"user_id" : user,
					"news_topics" : user_feeds_scores[user][j][2],
					"news_id" : user_feeds_scores[user][j][0],
					"news_content" : news_feeds_dict[user_feeds_scores[user][j][0]][0],
					"news_score" : user_feeds_scores[user][j][1]
				}
			}
			action_list.append(action)
	print helpers.bulk(es, action_list)
	
	es_status = us_server.select_feeds(es, user_feeds_scores, news_feeds_dict, idx)
	print es_status

	frac_accuracy = us_server.sel_fraction(user_feeds_scores, user_interests)
	print "accuracy - "+str(frac_accuracy)
	for user in frac_accuracy.keys():
		user_frac_accuracy[user].append(frac_accuracy[user])

	time.sleep(60)

	user_buckets = {"user_1": [], "user_2": [], "user_3": [], "user_4": [], "user_5": [], "user_6": [], "user_7": [], "user_8": [], "user_9": [], "user_10": []}
	result_new = helpers.scan(es, query, scroll=u"5m", index="user_attachments", doc_type="feeds")
	for res in result_new:
		if res["_source"]["iteration"] == idx: user_buckets[res["_source"]["user_id"]].append(res["_source"]["news_id"])

	if len(user_buckets["user_1"]) > 0:
		topic_penalizer(user_buckets, feed_topic_dict)
		print "user_topic_weights - "+str(user_topic_weights)

	for k in key_choices: del news_feeds_dict[k]
	idx += 1

for user in user_frac_accuracy.keys():
	print "%s" % user
	print user_frac_accuracy[user]

