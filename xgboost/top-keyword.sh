#recommen dfor you on the start page
JAVA_HOME=/opt/jdk1.8.0_121/
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
/data/pyspark/program/auto_report/tool/cc_hive -p "bizdate=2018-04-01" -f top-keyword.sql
