create table if not exists zhouyf.gbdt_feature_train_hot_sample(pvid string,label tinyint,
    attrs string, hittitle string, price string, difftm string, sellcnt string,
    evlcnt string, collectcnt string, addCartCnt string,  ctr string,
     uid_ctr string, refund_rate string,cate_ctr string, cate_weight string,  categoryId string, query_id string) partitioned by (ds string)
   ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
   stored as parquet;


insert overwrite table zhouyf.gbdt_feature_train_hot_sample partition(ds='${bizdate}')
select pvid, label,
case when tokenInSet(seg_word,attrs) = true then '1:1' else '1:0' end as hitattrs,
case when tokenInSet(seg_word,titleSmart) = true then '2:1' else '2:0' end as hittitle,
concat("3:",cast(ln(nvl(price,0)+1) as string)) as price,
concat("4:",cast(ln(nvl(difftm,0)+1) as string)) as difftm,
concat("5:",cast(ln(nvl(sellcnt,0)+1) as string)) as sellcnt,
concat("6:",cast(ln(nvl(evlcnt,0)+1) as string)) as evlcnt,
concat("7:",cast(ln(nvl(collectcnt,0)+1) as string)) as collectcnt,
concat("8:",cast(ln(nvl(addCartCnt,0)+1) as string)) as addCartCnt,
concat("9:",cast(nvl(ctr,0) as string)) as ctr,
concat("10:",cast(nvl(uid_ctr,0) as string)) as uid_ctr,
concat("11:",cast(nvl(refund_rate,0) as string)) as refund_rate,
concat("12:",cast(nvl(cate_ctr,0) as string)) as cate_interval_ctr,
concat("13:",cast(nvl(cate_weight,0) as string)) as cate_weight,  
idsToFeatures(cast(nvl(categoryId,0) as string), 14, 1078) as categoryId,
idsToFeatures(cast(nvl(query_id,0) as string), 1093, 1000) as query_id 
from
(
select * from zhouyf.gbdt_feature_train_sample where ds ='${bizdate}') a;





