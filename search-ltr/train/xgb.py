import xgboost as xgb
from sklearn.datasets import load_svmlight_file
from sklearn.metrics import classification_report
from sklearn import metrics
#import matplotlib.pyplot as plt
import operator
import pandas as pd
import numpy as np

# Read the LibSVM labels/features
dtrain = xgb.DMatrix('gbdt_train_final_ctr_vec_01.txt')
param = {'max_depth':6, 'eta':0.2, 'silent':0, 'objective':'binary:logistic'}
num_round = 70

bst = xgb.train(param, dtrain, num_round)

try:
   X_test, y_test = load_svmlight_file('gbdt_test_final_vec_interval_01.txt')
except ValueError:
   print(X_test, y_test)
dtest = xgb.DMatrix('gbdt_test_final_vec_interval_01.txt')
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

fpr = open("result.npy", "wb")
np.save(fpr, fpr_xgboost)
np.save(fpr, tpr_xgboost)
np.save(fpr, thresholds)
fpr.close()



importance = bst.get_fscore(fmap='featmap.txt')
importance = sorted(importance.items(),key=operator.itemgetter(1))

df = pd.DataFrame(importance, columns=['feature', 'fscore'])
df['fscore'] = df['fscore'] / df['fscore'].sum()
df.to_csv("./feat_importance.csv", index=False)

model = bst.get_dump(fmap='featmap.txt', dump_format='json')

with open('xgb-model.json', 'w') as output:
    output.write('[' + ','.join(list(model)) + ']')
    output.close()
