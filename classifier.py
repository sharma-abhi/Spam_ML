__author__ = 'Abhijeet'

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression

df = pd.read_csv('foo.csv')
train_data = df.values

df2 = pd.read_csv('goo.csv')
test_data = df.values

forest = RandomForestRegressor(n_estimators=100)
forest = forest.fit(train_data[0::, 1::], train_data[0::, 0])
output_forest = forest.predict(test_data)

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
        rank += 1
print "Random Forest Regresssor on test data complete"