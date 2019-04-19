cd /opt/task/zhouyf/xgboost
export JAVA_HOME=/opt/jdk1.8.0_121/
sh -x gbdt-train-compliment.sh
sh -x gbdt-train-compliment-android.sh
sh -x gbdt-train-ctr.sh
sh -x gbdt-train-ctrcom.sh
sh -x gbdt-train-ctrcom-android.sh
