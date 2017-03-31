"""
This file is used to find and store the topic-probabilities of all collected news-feeds based on the trained LDA model.
(1) server_addr : ES server address
(2) dict_name : name of dictionary built using Gensim
(3) model_name : name of trained LDA model
"""

from elasticsearch import Elasticsearch, helpers
from gensim.corpora import Dictionary
from gensim.models import LdaModel

es = Elasticsearch(server_addr, timeout=300)
dictionary = Dictionary.load(dict_name)
lda_model = LdaModel.load(model_name)

query = {"query": {"match_all": {}}}

print "getting es data..."
news_feeds = {}
result = helpers.scan(es, query, scroll=u"5m", index="news_collection", doc_type="feeds")
for res in result:
	news_feeds[(res["_source"]["news_dom"], res["_source"]["news_id"])] = res["_source"]["news_text"]

print len(news_feeds.keys())

news_topic = {}
for k in news_feeds.keys():
	topic_list = lda_model.get_document_topics(dictionary.doc2bow(news_feeds[k]), minimum_probability=0.00001)
	sort_topic = sorted([topic for topic in topic_list], key=lambda t: t[1], reverse=True)[:3]
	news_topic[k] = [t[0] for t in sort_topic]

print len(news_topic.keys())

idx = 0
action_list = []
for k in news_feeds.keys():
	action = {
		"_op_type" : "create",
		"_index" : "news_topics",
		"_type" : "feeds",
		"_source" : {
			"news_id" : idx,
			"news_text" : news_feeds[k],
			"news_topics" : news_topic[k]
		}
	}
	action_list.append(action)
	idx += 1

print helpers.bulk(es, action_list)
