#recommend for you on the start page
cd /opt/task/zhouyf/xgboost
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
for index in {55..59}
do
   count=60
   count=$((${count}-${index}))
   echo $count
   impala-shell -i hadoop-data1:21000 -f gbdt-train-compliment-android.sql --var=statsday=`date --date="-${count} day" +%Y-%m-%d`
   echo $？
done
