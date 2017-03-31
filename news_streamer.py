"""
This file collects news_feeds from various sites of different domains - Science, Technology, World, Sports, Arts, Health, Lifestyle and Economics. After collecting these feeds, each feed will be cleaned(removal of unnecessary text), tokenized and stemmed and stored in ES
(1) self.news_sources : all the URLs of different news websites
(2) server_addr : ES server address
All feeds are collected as RSS
"""


import feedparser as fp
from nltk import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
import re
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(server_addr, timeout=300)

class Streamer(object):

	def __init__(self):	
	
		self.tokenizer = RegexpTokenizer(r'[a-zA-Z_]+')
		self.p_stem = PorterStemmer()
		self.news_sources = {"Technology": ["http://feeds.feedburner.com/TechCrunch/", "https://thenextweb.com/feed/", "http://feeds.feedburner.com/digit/latest-news", "http://feeds.reuters.com/reuters/technologyNews", "http://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "http://rss.cnn.com/rss/edition_technology.rss", "http://feeds.bbci.co.uk/news/technology/rss.xml", "http://timesofindia.indiatimes.com/rssfeeds/5880659.cms", "https://www.cnet.com/rss/news/", "https://www.wired.com/category/gear/feed/", "http://www.techrepublic.com/rssfeeds/topic/tech-and-work/"], "Science": ["http://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "http://news.mit.edu/rss/research", "https://rss.sciencedaily.com/all.xml", "http://rss.nytimes.com/services/xml/rss/nyt/Science.xml", "http://feeds.nature.com/NatureLatestResearch", "http://rss.cnn.com/rss/edition_space.rss", "http://feeds.reuters.com/reuters/scienceNews", "http://news.nationalgeographic.com/rss/index.rss", "http://www.economist.com/sections/science-technology/rss.xml"], "World": ["http://rss.nytimes.com/services/xml/rss/nyt/World.xml", "http://rss.cnn.com/rss/edition_world.rss", "http://feeds.reuters.com/Reuters/worldNews", "http://feeds.bbci.co.uk/news/world/rss.xml", "http://feeds.bbci.co.uk/news/politics/rss.xml", "http://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml", "http://www.business-standard.com/rss/home_page_top_stories.rss", "http://timesofindia.indiatimes.com/rssfeeds/296589292.cms", "http://www.economist.com/sections/international/rss.xml", "http://feeds.foxnews.com/foxnews/world", "http://feeds.foxnews.com/foxnews/politics"], "Economics": ["http://www.economist.com/sections/business-finance/rss.xml", "http://economictimes.indiatimes.com/rssfeedsdefault.cms", "http://rss.cnn.com/rss/money_news_international.rss", "http://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "http://feeds.reuters.com/reuters/businessNews", "http://feeds.reuters.com/news/wealth", "http://www.hindustantimes.com/rss/business/rssfeed.xml", "http://www.hindustantimes.com/rss/realestate/rssfeed.xml", "http://timesofindia.indiatimes.com/rssfeeds/1898055.cms", "http://www.business-standard.com/rss/markets-106.rss", "http://www.business-standard.com/rss/economy-policy-102.rss", "http://www.business-standard.com/rss/finance-103.rss"], "Sports": ["http://api.foxsports.com/v1/rss?partnerKey=zBaFxRyGKCfxBagJG9b8pqLyndmvo7UU", "http://feeds.sport24.co.za/articles/Sport/Featured/TopStories/rss", "http://feeds.bbci.co.uk/sport/rss.xml?edition=uk", "https://sports.yahoo.com/top/rss.xml", "http://www.hindustantimes.com/rss/sports/rssfeed.xml", "http://rss.cnn.com/rss/edition_sport.rss", "http://rss.nytimes.com/services/xml/rss/nyt/Sports.xml", "http://feeds.reuters.com/reuters/sportsNews", "http://www.espn.com/espn/rss/news", "http://www.thestar.com.my/rss/editors-choice/sport/", "http://rss.cbssports.com/rss/headlines", "http://rssfeeds.usatoday.com/UsatodaycomSports-TopStories", "http://timesofindia.indiatimes.com/rssfeeds/4719148.cms"], "Health": ["http://rss.medicalnewstoday.com/featurednews.xml", "http://www.medicinenet.com/rss/weeklyhealth.xml", "http://www.nytimes.com/svc/collections/v1/publish/www.nytimes.com/section/well/rss.xml", "http://www.health.com/fitness/feed", "http://www.health.com/mind-body/feed", "http://feeds.reuters.com/reuters/healthNews", "http://feeds.bbci.co.uk/news/health/rss.xml"], "Arts": ["http://timesofindia.indiatimes.com/rssfeeds/1081479906.cms", "https://www.wired.com/category/design/feed/", "http://feeds.washingtonpost.com/rss/rss_arts-post", "http://feeds.feedburner.com/thr/news", "http://rss.nytimes.com/services/xml/rss/nyt/Arts.xml", "http://www.digitalartsonline.co.uk/rss/feeds/digitalarts-news.xml", "http://www.americansforthearts.org/feeds/national-news", "http://feeds.feedburner.com/creativebloq/news", "http://feeds.reuters.com/news/artsculture", "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml"]}

	def nlp_clean(self, document):

		new_doc = re.sub(r"<.*?>", "", document)
		tokens = self.tokenizer.tokenize(new_doc)
		new_tokens = [self.p_stem.stem(tok.lower()) for tok in tokens if tok not in stopwords.words('english')]
	
		return new_tokens

	def collect_news(self):

		news_articles = {}
		for domain in self.news_sources.keys():
			news_id = 0
			for url in self.news_sources[domain]:
				d = fp.parse(url)
				for post in d.entries:
					title = self.nlp_clean(post["title"])
					summary = self.nlp_clean(post["summary"])
					news_articles[(domain, news_id)] = title + summary
					news_id += 1

		actions = []
		for news in news_articles.keys():
			action = {
				"_op_type" : "create",
				"_index" : "news_collection",
				"_type" : "feeds",
				"_source" : {
					"news_dom" : news[0],
					"news_id" : news[1],
					"news_text" : news_articles[news]
				}
			}
			actions.append(action)

		print helpers.bulk(es, actions)
