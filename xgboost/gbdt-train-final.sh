#recommend for you on the start page
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
impala-shell -i hadoop-data1:21000 -r -q "select label, price, sellcnt, evlcnt, collectcnt, addCartCnt, ctr,uid_ctr,refund_rate,cate_ctr,cate_weight,titleSim, cateSim,categoryid,query_id from zhouyf.gbdt_feature_train_vec_interval_weight_sample where ds between '2018-05-21' and '2018-09-30'"  -B --output_delimiter=" " -o gbdt_train_final_ctr_vec_01.txt

