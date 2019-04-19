import xgboost as xgb
import operator
import pandas as pd
import numpy as np
import json

import logging

from sklearn.datasets import load_svmlight_file
from sklearn.metrics import classification_report
from sklearn import metrics

logfile="log/train.log"
logging.basicConfig(filename=logfile, level=logging.DEBUG)

#import matplotlib.pyplot as plt

# Read the LibSVM labels/features
train_file  = "data/gbdt_train_final_ctr_vec_10w.txt"
test_file   = "data/gbdt_test_final_vec_interval_01.txt"
feature_file= "data/featmap.txt"

#output model
model_file      = "model/xgb-model.json"
importance_file = "model/xgb-imp.csv"
metrics_file    = "model/xgb-metrics.txt"

try:
   X_test, y_test = load_svmlight_file(test_file)
except ValueError:
   logging.warning((X_test, y_test))
dtest = xgb.DMatrix(test_file)

# training
logging.info("training process start")
dtrain = xgb.DMatrix(train_file)
param = {'max_depth':7, 'eta':0.2, 'silent':0, 'objective':'binary:logistic'}
num_round = 10
bst = xgb.train(param, dtrain, num_round)
logging.info("training process success")

y_pred = bst.predict(dtest)

y_pred_result = []
for x in y_pred:
  if x > 0.35:
    y_pred_result.append(1)
  else:
    y_pred_result.append(0)

target_names = ['class 0', 'class 1']

print(classification_report(y_test, y_pred_result, target_names=target_names))


fpr_xgboost, tpr_xgboost, thresholds = metrics.roc_curve(y_test, y_pred)
print('auc=', metrics.auc(fpr_xgboost, tpr_xgboost))


with open(metrics_file, "w") as fpr:
    np.savetxt(fpr, fpr_xgboost)
    np.savetxt(fpr, tpr_xgboost)
    np.savetxt(fpr, thresholds)

#print(fpr_xgboost)
#print(tpr_xgboost)
#print(thresholds)

importance = bst.get_fscore(fmap=feature_file)
importance = sorted(importance.items(),key=operator.itemgetter(1))

df = pd.DataFrame(importance, columns=['feature', 'fscore'])
df['fscore'] = df['fscore'] / df['fscore'].sum()
df.to_csv(importance_file, index=False)

model = bst.get_dump(fmap=feature_file, dump_format='json')

with open(model_file, 'w') as output:
    output.write('[' + ','.join(list(model)) + ']')
