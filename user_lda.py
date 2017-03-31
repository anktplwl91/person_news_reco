"""
This file is used to create a dictionary from the Medium corpus stored in ES, and then use that dictionary to train a LDA model over this corpus
(1) server_addr : ES server address
(2) dict_name : name of created dictionary
(3) model_name : name of created LDA model
(4) min_docs : min number of documents the word should appear in the corpus
(5) max_docs : max number of docs the word should appear in the corpus (in fraction)
(6) n_topics : number of topics the model is to be trained on
(7) n_passes : number of passes the model is to be trained for
(8) n_iters : number of iterations per pass
"""

from elasticsearch import Elasticsearch, helpers
from gensim.models import LdaModel
from gensim.corpora import Dictionary
import logging
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

es = Elasticsearch(server_addr, timeout=300)
p_stem = PorterStemmer()

query = {
	"query": {
		"match_all": {}
	}
}

docs = []
texts = []

stop_list = stopwords.words('english')
stop_list.extend(['continue', 'reading', 'medium'])

print "data extraction from es..."
'''
result1 = helpers.scan(es, query, scroll=u'5m', index='new_data_dump', doc_type='medium')
for res in result1:
	docs.append(res["_source"]["medium_title"] + res["_source"]["medium_summary"])
'''

result2 = helpers.scan(es, query, scroll=u'5m', index='data_dump', doc_type='medium')
for res in result2:
	docs.append(res["_source"]["medium_title"] + res["_source"]["medium_summary"])

"""
doc_len = []
for d in docs:
	doc_len.append(len(d))
print "average document length is: %s" % (sum(doc_len) / len(doc_len))


print "combining documents in sets of 7..."
for i in range(0, len(docs), 7):
	temp_list = []
	for j in range(i, i+7):
		temp_list.extend(docs[j])
	texts.append(temp_list)
"""

print "removing stopwords and stemming..."
new_texts = [[p_stem.stem(tok) for tok in doc if tok not in stop_list] for doc in docs]

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

dictionary = Dictionary(new_texts)
dictionary.filter_extremes(no_below=min_docs, no_above=max_docs, keep_n=None)
dictionary.compactify()
dictionary.save(dict_name)

corpus = [dictionary.doc2bow(text) for text in new_texts]

lda = LdaModel(corpus=corpus, id2word=dictionary, num_topics=n_topics, passes=n_passes, iterations=n_iters)
lda.save(model_name)

print lda.print_topics(10)
