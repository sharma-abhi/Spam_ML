__author__ = 'Abhijeet'

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from elasticsearch import Elasticsearch

es = Elasticsearch(timeout=180)
df = pd.read_csv('foo.csv', index_col=0)
train_data = df.values
print "Reading Feature matrix complete"

df2 = pd.read_csv('goo.csv', index_col=0)
test_data = df2.values
print "Reading Test matrix complete"

print "Fitting Data for Random Forest Regressor..."
forest = RandomForestRegressor(n_estimators=100)
forest = forest.fit(train_data[0::, 1::], train_data[0::, 0])
print "Training complete. Predicting on test data now ..."
output_forest = forest.predict(test_data)
print "Prediction complete"

#print output_forest
forest_dict = {}
for item in zip(df2.index, output_forest):
    docno, score = item
    forest_dict[docno] = score

with open("results/random_forest_reg_test_output.txt", "w") as f:
    rank = 1
    sorted_keys = sorted(forest_dict, key=forest_dict.get, reverse=True)
    for docno in sorted_keys:
        f.write('spam' + " Q0 " + str(docno) + " " + str(rank) + " " + str(forest_dict[docno]) + " Exp\n")
        if rank <= 10:
            result = es.get(index="vs_dataset", doc_type="document", id=docno, fields='raw_html')
            with open("results/rforest/rank" + str(rank) + ".txt","w") as fw:
                fw.write(result['fields']['raw_html'][0])
        rank += 1
print "Random Forest Regression on test data complete"


print "Fitting Data for Linear Regressor..."
linear_reg = LinearRegression(normalize=True)
linear_reg = linear_reg.fit(train_data[0::, 1::], train_data[0::, 0])
print "Training complete. Predicting on test data now ..."
output_linear_reg = linear_reg.predict(test_data)
print "Prediction complete"

linear_reg_dict = {}
for item in zip(df2.index, output_linear_reg):
    docno, score = item
    linear_reg_dict[docno] = score

with open("results/linear_reg_test_output.txt", "w") as f:
    rank = 1
    sorted_keys = sorted(linear_reg_dict, key=linear_reg_dict.get, reverse=True)
    for docno in sorted_keys:
        f.write('spam' + " Q0 " + str(docno) + " " + str(rank) + " " + str(linear_reg_dict[docno]) + " Exp\n")
        if rank <= 10:
            result = es.get(index="vs_dataset", doc_type="document", id=docno, fields='raw_html')
            with open("results/lreg/rank" + str(rank) + ".txt","w") as fw:
                fw.write(result['fields']['raw_html'][0])
        rank += 1
print "Linear Regression on test data complete"