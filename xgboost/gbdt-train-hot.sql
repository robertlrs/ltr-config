invalidate metadata;


create table if not exists zhouyf.gbdt_feature_train_sample(pvid string,label tinyint,hitattrs string, hitpreCategoryList string, price string, difftm string, sellcnt string, contenttype string, evlcnt string, collectcnt string, add_cart_cnt string, hittitle string, query_id string, ctr string,uid_ctr string, category_ctr string, categoryId string) partitioned by (ds string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as parquet;

insert overwrite zhouyf.gbdt_feature_train_sample partition(ds='${var:statsday}')
select pvid, label,
case when tokenInSet(seg_word,attrs) = true then '1:1' else '1:0' end as hitattrs,
case when tokenInSet(seg_word,preCategoryList) = true then '2:1' else '2:0' end as hitpreCategoryList,
concat("3:",cast(ln(isnull(price,0)+1) as string)) as price,
concat("4:",cast(isnull(difftm/86400,0) as string)) as difftm,
concat("5:",cast(ln(isnull(sellcnt,0)+1) as string)) as sellcnt,
concat("6:",cast(ln(isnull(evlcnt,0)+1) as string)) as evlcnt,
concat("7:",cast(ln(isnull(collectcnt,0)+1) as string)) as collectcnt,
concat("8:",cast(ln(isnull(add_cart_cnt,0)+1) as string)) as add_cart_cnt,
case when tokenInSet(seg_word,title_smart) = true then '9:1' else '9:0' end as hittitle,
concat("10:",cast(isnull(query_id,0) as string)) as query_id,
concat("11:",cast(isnull(ctr,0) as string)) as ctr,
concat("12:",cast(isnull(uid_ctr,0) as string)) as uid_ctr,
concat("13:",cast(isnull(category_ctr,0) as string)) as category_ctr,
idsToFeatures(cast(isnull(categoryId,0) as string), 14, 868) as categoryId from
(
select a.*, case when (searchtime - createtm) < 0 then 0 else (searchtime -createtm) end as difftm,  b.ranks from
(
select m.*, n.ctr, n.interval_ctr, p.ctr as uid_ctr, p.interval_ctr, q.ctr as category_ctr, q.interval_ctr from 
(select * from zhouyf.item_pv_click_seg_word_train_compliment where ds = '${bizdate}') m left join
(select * from zhouyf.search_item_ctr where ds = '${bizdate}') n on m.item_id = n.item_id
left join (select * from zhouyf.search_uid_ctr where ds = '${bizdate}') p on m.uid = p.uid
left join (select * from zhouyf.search_cate_ctr where ds = '${bizdate}') q on m.categoryid = q.categoryid) a, 
zhouyf.craftsman b where a.uid = b.uid) temp;





