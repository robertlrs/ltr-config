cd /opt/task/zhouyf/xgboost
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
for index in {1..60}
do
   count=70
   count=$((${count}-${index}))
   echo $count
   impala-shell -i hadoop-data1:21000 -f gbdt-train-ctrcom-hot.sql --var=statsday=`date --date="-${count} day" +%Y-%m-%d`
   echo $？
done
