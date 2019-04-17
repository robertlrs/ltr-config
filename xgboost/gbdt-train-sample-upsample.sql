--1. 提取每个用户点击商品--
drop table if exists zhouyf.cid_item_click;

create table  zhouyf.cid_item_click as
select distinct cid, item_id from zhouyf.gbdt_feature_train_stats where ds = '${statsday}' and (cid != -1 or cid != 0) and label = 1;

--2. 根据用户购买、加购、收藏设置权重--
create  table if not exists zhouyf.cid_item_weight(cid bigint, item_id bigint, weight int) partitioned by (ds string)
 ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
   stored as parquet;

insert overwrite table zhouyf.cid_item_weight partition(ds='${statsday}')
select cid, item_id, sum(weight) as weight from
(select a.cid, a.item_id,
   case when b.uid is null then 0 else 5 end as weight
from
zhouyf.cid_item_click a
left join
(select uid, targetid from kaipao.kp_collect where from_unixtime(createtm, 'yyyy-MM-dd') = '${statsday}' and type = 3) b
on a.cid = b.uid and a.item_id = b.targetid

UNION ALL 

select a.cid, a.item_id,
   case when c.cid is null then 0 else 10 end as weight
from
zhouyf.cid_item_click  a
left join
(select user_id as cid, item_id from kaipao.dj_cart where from_unixtime(cast(create_time/1000 as bigint), 'yyyy-MM-dd') = '${statsday}') c
on a.cid = c.cid and a.item_id = c.item_id

UNION ALL

select a.cid, a.item_id, 
   case when d.cid is null then 0 else 15 end as weight
from 
zhouyf.cid_item_click  a
left join
(select user_id as cid, item_id from dw.dwd_order_item_dd where 
ds = '${yesterday}' and from_unixtime(unix_timestamp(order_create_time, 'yyyy-MM-dd'), 'yyyy-MM-dd') = '${statsday}') d
on a.cid = d.cid and a.item_id = d.item_id) temp
group by cid, item_id;

--3.添加样本的weight信息--
create table if not exists zhouyf.gbdt_feature_train_weight(cid bigint, pvid string,label tinyint, weight int, item_id bigint,  keyword string,seg_word string,attrs string, preCategoryList string, price double, difftm double, sellcnt int, sellcnt_interval int, evlcnt int, evlcnt_interval int, collectcnt int,
    collectcnt_interval int, addCartCnt int, addCartCnt_interval int, titleSmart string, ctr double,
    interval_ctr double, uid_ctr double, uid_interval_ctr double,
    refund_rate double, refund_interval_rate double, cate_ctr double,
    cate_interval_ctr double, cate_weight double, query_id int, categoryId int) partitioned by (ds string)
   ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
   stored as parquet;

insert overwrite table zhouyf.gbdt_feature_train_weight partition(ds='${statsday}')
select a.cid, pvid, label,
   case when b.weight is null then 1
        when b.weight = 0 then 1
        else b.weight end as weight,
   a.item_id, keyword, seg_word, attrs, preCategoryList, price, difftm,sellcnt, sellcnt_interval, evlcnt,
   evlcnt_interval, collectcnt, collectcnt_interval, addCartCnt, addCartCnt_interval, titleSmart, ctr,
   interval_ctr, uid_ctr, uid_interval_ctr, refund_rate, refund_interval_rate, cate_ctr, cate_interval_ctr, cate_weight,
   query_id, categoryId from
(select * from zhouyf.gbdt_feature_train_stats where ds = '${statsday}') a
left join
(select * from zhouyf.cid_item_weight where ds = '${statsday}') b
on a.cid = b.cid and a.item_id = b.item_id;

