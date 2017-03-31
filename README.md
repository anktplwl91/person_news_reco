# person_news_reco
Recommendation system for news-feeds personalized for users

Tools - Python-2.7, ElasticSearch, python-nltk, feedparser, Gensim, oauth2

Begin with collecting data - use "more_data.py" to collect data from Medium feeds.
Next, train a LDA model using "user_lda.py" on above collected corpus.
Now, collect some news-feeds using "news_streamer.py" and store them,
"news_topic_match.py" is used to collect the topics of all collected news-feeds.
Finally, "feed_ranker.py" can be run to check the recommendations
