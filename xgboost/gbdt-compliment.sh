cd /opt/task/zhouyf/xgboost-V2
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "the compliment process for features extraction"
  sh -x search_sample_middle.sh
  sh -x gbdt-train-sample-full.sh
  sh -x gbdt-train-vec-sample-full.sh
  sh -x gbdt-train-sample-vec-interval.sh
  sh -x gbdt-train-sample-vec-interval1.sh
  echo $？
done
