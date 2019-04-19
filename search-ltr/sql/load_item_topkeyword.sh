cd /opt/task/zhouyf

export JAVA_HOME=/opt/jdk1.8.0_121/

count=60
impala-shell -i hadoop-data1:21000 -f load_item_topkeyword.sql --var=statsday=`date --date="-${count} day" +%Y-%m-%d`

