--1.关联商品信息--

create table if not exists zhouyf.item_pv_click_seg_word_stats(cid bigint, label tinyint, keyword string, item_id bigint, searchtime bigint, pvid string, seg_word string, query_id bigint, attrs string,  precategorylist string, price double, createtm bigint, updatetm bigint, title string, titleSmart string, categoryId int, evlcnt int, evlcnt_interval int, sellcnt int, sellcnt_interval int, collectcnt int, collectcnt_interval int,addCartCnt int, addCartCnt_interval int, uid bigint, city string)  partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_pv_click_seg_word_stats partition(ds='${bizdate}')
select b.cid, b.label, b.keyword, b.item_id, unix_timestamp(b.searchtime,'yyyyMMddHHmmss') as searchtime, b.pvid, b.seg_word, b.query_id, a.attrs, a.precategorylist, cast(a.price as double) as price, cast(a.createtm as bigint) as createtm, cast(a.updatetm as bigint) as updatetm, a.title, a.titleSmart, cast(a.categoryId as int) categoryId,
   case when c.evlcnt = -1 then 0 else c.evlcnt end as evlcnt,
   case when c.evlcnt_interval = -1 then 0 else c.evlcnt_interval end as evlcnt_interval,
   case when c.sellcnt = -1 then 0 else c.sellcnt end as sellcnt,
   case when c.sellcnt_interval = -1 then 0 else c.sellcnt_interval end as sellcnt_interval,
   case when c.collectcnt = -1 then 0 else c.collectcnt end as collectcnt, 
   case when c.collectcnt_interval = -1 then 0 else c.collectcnt_interval end as collectcnt_interval,
   case when c.add_cart_cnt = -1 then 0 else c.add_cart_cnt end as addCartCnt,
   case when c.add_cart_cnt_interval = -1 then 0 else c.add_cart_cnt_interval end as addCartCnt_interval,
   cast(a.uid as bigint), a.city from
     (select * from zhouyf.search_pv_join_word_id_seg_word where ds='${bizdate}') b
left join
    zhouyf.item_index_new a on cast(a.iid as bigint) = b.item_id
left join (select * from zhouyf.item_stats_info where ds='${bizdate}') c on b.item_id = c.item_id;

--2.关联商品点击率、匠人点击率，退货率、类目点击率等信息--
create table if not exists zhouyf.gbdt_feature_train_stats(cid bigint, pvid string,label tinyint, item_id bigint, 
    keyword string,seg_word string,attrs string, preCategoryList string, price double, difftm double, sellcnt int, 
    sellcnt_interval int, evlcnt int, evlcnt_interval int, collectcnt int, 
    collectcnt_interval int, addCartCnt int, addCartCnt_interval int, titleSmart string, ctr double, 
    interval_ctr double, uid_ctr double, uid_interval_ctr double, 
    refund_rate double, refund_interval_rate double, cate_ctr double, 
    cate_interval_ctr double, cate_weight double, query_id int, categoryId int) partitioned by (ds string)
   ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
   stored as parquet;


insert overwrite table zhouyf.gbdt_feature_train_stats partition(ds='${bizdate}')
select cid, pvid, label, item_id, keyword, seg_word, attrs, preCategoryList,nvl(price,0),nvl(difftm/86400,0), nvl(sellcnt,0),
   nvl(sellcnt_interval,0),nvl(evlcnt, 0), nvl(evlcnt_interval,0),nvl(collectcnt,0), nvl(collectcnt_interval,0),
  nvl(addCartCnt,0), nvl(addCartCnt_interval,0),titleSmart, ctr, interval_ctr, uid_ctr,
   uid_interval_ctr, refund_rate, refund_interval_rate,cate_ctr, cate_interval_ctr, nvl(cate_weight,0), nvl(query_id,0), nvl(categoryId,0)
from 
(
select a.*, case when (searchtime - createtm/1000) < 0 then 0 else (searchtime -createtm/1000) end as difftm from
(
select m.*, n.ctr, n.interval_ctr, p.ctr as uid_ctr, p.interval_ctr as uid_interval_ctr, q.ctr as cate_ctr, q.interval_ctr 
as cate_interval_ctr, r.refund_rate, r.interval_rate as refund_interval_rate, s.boost as cate_weight from 
  (select * from zhouyf.item_pv_click_seg_word_stats where ds = '${bizdate}') m 
left join
  (select * from zhouyf.search_item_ctr where ds = '${bizdate}') n 
on m.item_id = n.item_id
left join 
  (select * from zhouyf.search_uid_ctr where ds = '${bizdate}') p 
on m.uid = p.uid
left join 
  (select * from zhouyf.search_cate_ctr where ds = '${bizdate}') q 
on m.categoryid = q.categoryid 
left join
  (select * from zhouyf.crafts_refund where ds = '${bizdate}') r
on m.uid = r.uid
left join
  (select * from dim.dim_hive_query_pre where ds ='${bizdate}') s 
on m.keyword = s.search_words and m.categoryid = s.category_id) a) temp;





