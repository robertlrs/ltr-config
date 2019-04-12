source ~/.bashrc
export JAVA_HOME=/opt/jdk1.8.0_121
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=$PATH:${JAVA_HOME}/bin:{JRE_HOME}/bin:$PATH
expATH=$PATH:/opt/mysql/bin
export PATH=/opt/anaconda3/bin:$PATH
#cc_pys

cd /data/pyspark/program/songwt/lr 
#spark2-submit --master yarn  --num-executors 2 --executor-cores 1  ./avro_train.py '2018-07-01'
for day in {101..188}
do
    bizdata=`date -d "$day days ago" +%Y-%m-%d`
    day1=$(($day+1)) 
    ds=`date -d "$day1 days ago" +%Y-%m-%d`
    echo $bizdata, $ds
    spark2-submit --master yarn  --num-executors 2 --executor-cores 1  /data/pyspark/program/songwt/lr/avro_train2.py $bizdata $ds 
done
