#recommend for you on the start page
cd /opt/task/zhouyf
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
for index in {1..1}
do
   count=2
   count=$((${count}-${index}))
   echo $count
   impala-shell -i hadoop-data1:21000 -f item-stats.sql --var=statsday=`date --date="-${count} day" +%Y-%m-%d`
   echo $？
done
