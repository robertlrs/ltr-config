1. checkdata.py 校验训练数据，测试数据的格式是否正确
2. 用output.sh生成featmap.txt用于xgboost模型生成
3. xgb.py--训练脚本，最新版本在bigdatadev的/opt/task/zhouyf
4. paint.py脚本来绘制roc曲线
5. 因为xgboost生成的模型格式跟ltr要求有些不同，这里需要执行adjust.py将xgboost模型格式化成ltr插件可以读的格式
