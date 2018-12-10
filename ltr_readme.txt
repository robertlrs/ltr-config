参考文档：https://elasticsearch-learning-to-rank.readthedocs.io/en/latest/index.html

1 创建索引： 
   	PUT _ltr
2 创建 _featureset：
   	PUT /_ltr/_featureset/lr-feature
   	{
   	 	... 说明：这里的json是xgboost_future.txt 文本内容
   	}
3 创建模型：
   POST /_ltr/_featureset/lr-feature/_createmodel
   {
   		... 说明：这里的json是ltr_create_model.txt 文本内容
   }
   
   GET _ltr/_model/search_xgboost
   
4 创建索引mapping：
    参见： ltr_item_mapping.txt
5 测试数据：
   往索引：ltr_item 添加测试数据
	PUT test_item/test_item/231
	{
		"iid": 231,
  		"addCartCount": 9,
  		"price": 1580,
  		"replyCnt": 0,
  		"sellcnt": 4,
  		"collectcnt": 51,
  		"categoryId": 525,
  		"itemCtr": 0.05206,
  		"userCtr": 0.03,
  		"cateCtr": 0.03,
  		"sellerTop": 16,
  		"categoryTop": 0.03,
  		"evlcnt": 0,
  		"province": 0,
  		"categoryGrade": 4,
  		"reItemCvr": 0,
  		"reCateCtr": 0.00888,
  		"expoCategory": "chaye",
  		"reItemCtr": 0,
  		"parentCategoryId": 523,
  		"hashUid": 3905,
  		"refundRate": 0.13,
  		"vector": "0.4, 0.5, 0.8"
	}

6 查询语句：
  参见：ltr查询query语句.txt
  
说明：
    1 模型： xgboost、lr两种
      xgboost 由n颗树组成，每颗树是一个完全二叉树，分数计算 每颗树权重*每颗树的值， 每棵树的值计算逻辑：由FeatureVector 计算得来
      FeatureVector[position:value], 每个位置由一个特征计算得到的值。
      lr：w1*v1 + w2*v2 + ... + wn*vn
      
    2 oneHot:
       对于离散值，比如类目，值0 到 850，每个值是一个特征，category=value表示，即：category=0， category=1， ..., category=850
       对于单个文档，只有一个特征有值。
       
    3 交叉特征：（为什么使用交叉特征原因不明确，跟线性分类有关）
       比如：city + phone 作为一个交叉特征， 
    