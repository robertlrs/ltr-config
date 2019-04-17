
训练、测试样本提取步骤

1. 先执行/data/ftp/zyf/es2csv.py，导出商品索引到本地（这里我们缺失实时商品特征快照，在实际提取样本时，用的是提取时间时刻的商品快照）
2. 统计top1000个频率最高的搜索词--top-keyword.sh
3. 商品每天的销量、收藏、加购、评价统计---item-stats-interval.sh
4. 曝光、点击样本提取，关联搜索词、每个商品统计信息(这一步会进行去噪，去掉公司内部id的曝光，点击日志。正负样本采用采取skip-above方式）---search_sample_middle.sh 
5. 关联商品反馈点击率，匠人点击率、退货率、类目点击率信息--gbdt-train-sample-full.sh 
6. 根据收藏、加购、评价对样本进行加权---gbdt-train-sample-upsample.sh
7. 关联搜索词、商品标题、类目隐语义向量----gbdt-train-vec-sample-full.sh
