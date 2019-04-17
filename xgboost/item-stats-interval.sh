cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
for index in {1..1}
do
   count=2
   count=$((${count}-${index}))
   echo $count
   ds=`date -d "${count} days ago" +%Y-%m-%d`
   count=$((${count}+59))
   startday=`date -d "${count} days ago" +%Y-%m-%d`
   /data/pyspark/program/auto_report/tool/cc_hive -p "statsday=$ds,startday=$startday" -f item-stats-interval.sql
   #impala-shell -i hadoop-data1:21000 -f item-stats-interval.sql --var=statsday=$ds --var=startday=$startday --var=interval=30
   echo $？
done
