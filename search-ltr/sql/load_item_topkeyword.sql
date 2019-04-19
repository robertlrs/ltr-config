drop table if exists zhouyf.item_index_new;
create table zhouyf.item_index_new
(attrs string, preCategoryList string, iid string, uid string,  price string, createtm string, createtm string, sellcnt string, contentType string, evlcnt string,collectcnt string,replyCnt string,
add_cart_count string, title_smart string,categoryId string, city string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';

load data inpath 'hdfs://hadoop-manager:8020/user/hdfs/zyf/esitem_result'into table zhouyf.item_index_new;

drop table if exists zhouyf.top_keyword
create table zhouyf.top_keyword
as
select row_number() over (order by count desc) as row_num,* from (
select  split_part(pid,"_", 3) as keyword,count(*) as count 
from logs.app_log  
where pid like "search_i_%" and eid = "request" and ds > '${var:statsday}'
group by pid
order by count desc
limit 1000
) a;
