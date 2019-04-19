#recommen dfor you on the start page
rt JAVA_HOME=/opt/jdk1.8.0_121/
cd /opt/task/zhouyf/newmodel
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
impala-shell -i hadoop-data1:21000 -f top-keyword.sql
