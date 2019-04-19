cd /opt/task/zhouyf/xgboost
#echo "begin-------------------------------${db}:${table}------------------------------"
    #sqoop import --connect jdbc:mysql://10.117.60.85:3306/${db}?tinyInt1isBit=false --username don    gjia --password dongjia --table ${table}   --compression-codec=snappy     --as-parquetfile  -m 1 --    hive-import --hive-overwrite     --hive-database ${db}
 sqoop export --connect jdbc:mysql://172.16.8.4:3306/warehouse --username datateam --password  RJ4f84S4RjfRxRMm  --table dj_search_item_ctr --export-dir hdfs://hadoop-manager:8020/user/hive/warehouse/zhouyf.db/search_item_ctr --input-fields-terminated-by '\001' --input-null-non-string '\\N' --input-null-string '\\N' --update-mode allowinsert --update-key item_id
