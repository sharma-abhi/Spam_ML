__author__ = 'Abhijeet'

from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from os import listdir
import cPickle as cp
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
import gc
import sys

class ComputeMatrix(object):
    """
    Returns the query scores
    """
    def __init__(self):
        self.spam_search_size = 75420
        self.crawl_search_size = 1000
        self.es = Elasticsearch(timeout=180)

        file_corpus_path = 'trec07p/data'
        self.file_names = listdir(file_corpus_path)

        stop_file_name = 'stoplist.txt'
        with open(stop_file_name) as f:
            self.stop_file_data = [x.replace("\n", '') for x in f]

    def compute_spam_ngrams(self):
        """

        :return:
        """
        spam_list = ['anxiety', 'free', 'as seen on', 'buy', 'buy direct', 'order status', 'shopper',
                     'meet singles', 'babes', 'additional income', 'earn', 'per week', 'extra income', 'work at home',
                     'work from home', 'money making', 'f r e e', 'discount', 'cheap', 'lowest price',
                     'no fees', 'million', 'mortgage rates', 'profit', 'dollars', 'credit card', 'get paid', 'wife',
                     'xxx', 'ad', 'valium', 'viagra', 'weight loss', 'vicodin', 'wrinkles', 'aging', 'lose weight',
                     'winner', 'sign up free', 'deal', 'limited time', 'for you', 'casino', 'rolex', 'bonus']
        #return spam_list
        return spam_list

    def create_feature_matrix(self, args):
        """

        :return:
        """
        spam_ngrams = self.compute_spam_ngrams()

        if args[1] == '-f':
            fmatrix = {}
            print "Starting elastic search computation ..."
            for spam in spam_ngrams:
                print "Starting fetch for gram: ", spam
                result = self.es.search(index="spam_dataset", doc_type="document", size=self.spam_search_size,
                                        analyzer="my_english", body={"query": {"match": {"text": spam}}})
                hits = result['hits']['hits']
                print "Result Size: ", len(hits)
                for i in range(len(hits)):
                    if fmatrix.get(spam) is None:
                        fmatrix[spam] = {hits[i]['_id']: hits[i]['_score']}
                    else:
                        fmatrix[spam][hits[i]['_id']] = hits[i]['_score']

            with open('fmatrix.txt', 'w') as f:
                f.write(str(fmatrix))
            print "elastic search computation complete"

            df = pd.DataFrame()
            for spam in spam_ngrams:
                df[spam] = pd.Series(fmatrix[spam], index=self.file_names)
            df = df.fillna(0)
            with open("spam_labels.txt") as f:
                spam_dict = cp.load(f)
            df['label'] = pd.Series(spam_dict, index=df.index)

            cols = df.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            df = df[cols]
            print df.head(5)
            print "Train Data Frame created. Printing..."
            df.to_csv('foo.csv')

        elif args[1] == '-g':
            crawl_doc_set = set()
            tmatrix = {}
            for spam in spam_ngrams:
                print "Starting fetch for gram: ", spam
                result = self.es.search(index="vs_dataset", doc_type="document", size=self.crawl_search_size,
                                        analyzer="my_english", body={"query": {"match": {"text": spam}}})

                hits = result['hits']['hits']
                print "Result Size: ", len(hits)
                for i in range(len(hits)):
                    crawl_doc_set.add(hits[i]['_id'])
                    if tmatrix.get(spam) is None:
                        tmatrix[spam] = {hits[i]['_id']: hits[i]['_score']}
                    else:
                        tmatrix[spam][hits[i]['_id']] = hits[i]['_score']

            with open('tmatrix.txt', 'w') as f:
                f.write(str(tmatrix))
            print "elastic search computation complete"

            df2 = pd.DataFrame()
            for spam in spam_ngrams:
                if tmatrix.get(spam) is None:
                    df2[spam] = 0
                else:
                    df2[spam] = pd.Series(tmatrix[spam], index=crawl_doc_set)
            df2 = df2.fillna(0)
            print "Test Data Frame created. Printing..."
            df2.to_csv('goo.csv')
            print df2.head(5)

if __name__ == "__main__":
    args = sys.argv
    cm = ComputeMatrix()
    cm.create_feature_matrix(args)