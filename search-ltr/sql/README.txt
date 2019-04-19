#脚本执行顺序以及对应sql的功能
1. 首先执行item-stats.sh
  item-stats.sql： 统计商品的销量，评价，购物车，收藏，都是按天统计（累计到统计日）.
2. 执行gbdt-train-compliment.sh 以及gbdt-train-compliment-android.sh
   gbdt-train-compliment.sql 以及gbdt-train-compliment-android.sql分别对应ios和android端（因为打点不同）
   生成一张宽表，对应label，商品信息
3. 执行gbdt-train-ctr.sh 生成商品反馈点击率，匠人点击率，类目点击率信息
4. 执行gbdt-train-ctrcom.sh 以及gbdt-train-ctrcom-android.sh
   按照libsvm格式产生宽表
5. 执行gbdt-train-final.sh以及gbdt-test-final.sh 导出libsvm格式的训练以及测试样本，这里注意修改对应的时间（训练样本跟测试样本的比例大概为7：3）
    
