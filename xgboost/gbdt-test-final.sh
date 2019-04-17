#recommend for you on the start page
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
impala-shell -i hadoop-data1:21000 -r -q "select label, price, sellcnt, evlcnt, collectcnt, addCartCnt, ctr,uid_ctr,refund_rate,cate_ctr,cate_weight, titleSim, cateSim, categoryid,query_id from zhouyf.gbdt_feature_test_vec_interval_weight_sample  where ds >= '2018-10-01'"  -B --output_delimiter=" " -o gbdt_test_final_vec_interval_01.txt

