#recommend for you on the start page
cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
for index in {1..164}
do
   count=165
   count=$((${count}-${index}))
   echo $count
   ds=`date -d "${count} days ago" +%Y-%m-%d`
   /data/pyspark/program/auto_report/tool/cc_hive -p "bizdate=$ds" -f gbdt-train-sample-interval1.sql
   echo $？
done
