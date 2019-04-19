import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import auc

def plot():

    f = open("/Users/zyf/development/search-ltr/model/result.npy", "rb")
    fpr = np.load(f)
    tpr = np.load(f)
    thresholds = np.load(f)
    auc_area = auc(fpr, tpr)

    plt.plot(fpr, tpr, 'darkorange', lw=2, label='AUC = %.4f' % auc_area)
    plt.legend(loc='lower right')
    plt.axis([0,1,0,1])
    plt.plot([0, 1], [0, 1], color='navy', linestyle='--')
    plt.title('ROC-AUC curve for xgboost model')
    plt.ylabel('True Positive Rate')
    plt.xlabel('False Positive Rate')
    plt.show()


if __name__ == '__main__':
    plot()