__author__ = 'Abhijeet'

from elasticsearch import Elasticsearch
from os import listdir
import string
from bs4 import BeautifulSoup
import cPickle as cp


class CreateIndex(object):
    """
    Creates an Index in Elastic Search
    """
    def __init__(self):

        self.es = Elasticsearch()
        self.file_corpus_path = 'trec07p/data'
        self.label_path = 'trec07p/full'
        self.label_file_name = 'index'

        stop_file_name = 'stoplist.txt'
        with open(stop_file_name) as f:
            self.stop_file_data = f.readlines()
        # little cleaning up the stop list data
        self.stop_file_data = [x.replace("\n", '') for x in self.stop_file_data]

    def compute_labels(self):
        """
        1 for spam, 0 for ham
        :return:
        """
        label_dict = {}
        with open(self.label_path+"/"+self.label_file_name) as f:
            for line in f:
                label_list = line.split()
                label_dict[label_list[1].split('/')[2]] = '1' if label_list[0] == 'spam' else '0'
        return label_dict

    def compute_index(self):
        """
        Creates the index in ES
        :return: Void
        """
        #spam_dict = dict()
        label_dict = self.compute_labels()
        print "label computation complete\n"

        file_names = listdir(self.file_corpus_path)

        for data_file in file_names:
            #print "indexing file: ", data_file
            with open(self.file_corpus_path+"/"+data_file) as f:
                extract_text = False
                text_string = f.read()
                original_string = text_string

                text_string = text_string.replace("\n", " ")
                text_string = text_string.replace("\t", "")

                # Removing stop words
                tlist = text_string.split()
                slist = []
                for i in range(len(tlist)):
                    if tlist[i] in self.stop_file_data:
                        slist.append('')
                    else:
                        slist.append(tlist[i])
                text_string = ' '.join(slist)

                # Converting to lower text
                text_string = text_string.lower()
                # Removing punctuations from query
                for p in string.punctuation:
                    if p != '_' and p != '-' and p != '\'':
                        text_string = text_string.replace(p, " ")
                text_string = text_string.replace("  ", " ")
                #text_string = text_string.replace("p", "")
                #text_string = text_string.replace("div", "")

                doc_length = len(text_string.split())
                text_string = BeautifulSoup(text_string).text

                spam_val = label_dict[data_file]

                doc = {'docno': data_file, 'text': text_string, "spam": spam_val, 'doclength': doc_length,
                       'original': original_string.decode('ascii', errors='ignore')}

                res = self.es.index(index="spam_dataset", doc_type='document', id=data_file, body=doc)

        with open("spam_labels.txt", "w") as f:
            cp.dump(label_dict, f)

if __name__ == "__main__":
    ci = CreateIndex()
    ci.compute_index()