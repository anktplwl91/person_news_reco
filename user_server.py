"""
This file is used as a simulation of a live user - selects feeds from the streamed news and also calculates the fraction of attached news that was in top-10 streamed news
(1) self.user_interests : mentions topic-ids that are of interest to the user
(2) self.user_attach : stores, for each user, news-feeds that he finds interesting
(3) frac_accuracy : stores, for each user, fraction of news-feeds in top-10 streamed to the user which user has attached
"""

from elasticsearch import Elasticsearch, helpers
import random

class Server(object):

	def __init__(self):

		self.user_interests = {"user_1": [0, 1, 3, 6, 9, 12, 13, 17], "user_2": [3, 12, 17], "user_3": [2, 8, 10], "user_4": [11, 13, 7], "user_5": [1, 13, 19], "user_6": [12, 17, 19], "user_7": [11, 4, 14], "user_8": [15, 16, 17], "user_9": [9, 7, 4], "user_10": [0, 11, 19]}

		self.user_attach = {}

"""
The user will select feeds which are most important to him i.e., all three topics are of his interest, then any two topics are of his interest and then move onto any one topic of his interest. In this way selects 10 feeds as attachments
"""
	def select_feeds(self, es, user_feeds_scores, news_feed_dict, iteration):

		for user in user_feeds_scores.keys():
			interest_list = self.user_interests[user]
			three_list = [feed[0] for feed in user_feeds_scores[user] if len([topic for topic in feed[2] if topic in interest_list])==3]
			two_list = [feed[0] for feed in user_feeds_scores[user] if len([topic for topic in feed[2] if topic in interest_list])==2]
			one_list = [feed[0] for feed in user_feeds_scores[user] if len([topic for topic in feed[2] if topic in interest_list])==1]

			self.user_attach[user] = []
			if len(three_list) <= 10:
				for feed in three_list: self.user_attach[user].append(feed)
			else:
				for idx in range(10): self.user_attach[user].append(three_list[idx]) 

			if len(self.user_attach[user]) < 10:
				if len(two_list) >= (10-len(self.user_attach[user])):
					for idx in range(10-len(self.user_attach[user])): self.user_attach[user].append(two_list[idx])
				else:
					for feed in two_list: self.user_attach[user].append(feed)

			if len(self.user_attach[user]) < 10:
				if len(one_list) >= (10-len(self.user_attach[user])):
					for idx in range(10-len(self.user_attach[user])): self.user_attach[user].append(one_list[idx])
				else:
					for feed in one_list: self.user_attach[user].append(feed)

			print "user_attach - "+str(self.user_attach[user])

		actions = []
		for user in self.user_attach.keys():
			for idx in range(len(self.user_attach[user])):
				action = {
					"_op_type" : "create",
					"_index" : "user_attachments",
					"_type" : "feeds",
					"_source" : {
						"iteration" : iteration,
						"user_id" : user,
						"news_topics" : user_feeds_scores[user][idx][2],
						"news_id" : self.user_attach[user][idx]
					}
				}
				actions.append(action)

		return helpers.bulk(es, actions)

	def sel_fraction(self, user_feeds_scores, interest_dict):

		frac_accuracy = {}
		for user in self.user_attach.keys():
			cnt = 0
			interest_list = interest_dict[user]
			user_feeds = sorted(user_feeds_scores[user], key=lambda t: t[1], reverse=True)[:10]
			user_feeds_ids = [feed[0] for feed in user_feeds]
			attach_list = [feed for feed in self.user_attach[user]]
			for news in attach_list:
				if news in user_feeds_ids: cnt += 1

			frac_accuracy[user] = float(cnt / float(len(attach_list)))

		return frac_accuracy
