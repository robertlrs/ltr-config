#recommend for you on the start page
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
impala-shell -i hadoop-data1:21000 -r -q "select pvid, item_id, queryVec, titleVec,cateVec, simVec(queryVec,titleVec) as titleSim, simVec(queryVec,cateVec) as cateSim from zhouyf.gbdt_feature_train_vec_sample where ds = '2018-09-14'"  -B --output_delimiter=" " -o gbdt_train_final_ctr_vec_test.txt

