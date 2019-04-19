cd /data/ftp/zyf
export JAVA_HOME=/opt/jdk1.8.0_121/
source venv/bin/activate
python es2csv.py
if [ $? -ne 0 ]; then
   echo "load items from es failed"
   exit 1
fi
hdfs dfs -put esitem_result1 /user/hdfs/zyf/
if [ $? -ne 0 ]; then
   echo "put file to hdfs failed"
   exit 1
fi
impala-shell -i hadoop-data1:21000 -f load-es.sql
