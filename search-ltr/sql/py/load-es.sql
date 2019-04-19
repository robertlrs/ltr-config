invalidate metadata;

drop table if exists zhouyf.item_index_new;
create table  zhouyf.item_index_new
(attrs string, preCategoryList string, iid string, uid string, price string, createtm string, sellcnt string, contentType string, evlcnt string,collectcnt string,replyCnt string,add_cart_count string, title_smart string,categoryId string,city string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';

load data inpath 'hdfs://hadoop-manager:8020/user/hdfs/zyf/esitem_result1' into table zhouyf.item_index_new;
