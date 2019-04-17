#recommend for you on the start page
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
impala-shell -i hadoop-data1:21000 -r -q "select * from zhouyf.test_vec"  -B --output_delimiter=" " -o gbdt_train_final_ctr_vec_01.txt

