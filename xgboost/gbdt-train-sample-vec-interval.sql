create table if not exists zhouyf.gbdt_feature_train_vec_weight_svm(pvid string,item_id bigint, label string, price string, sellcnt string, evlcnt string, collectcnt string, addCartCnt string, ctr string,uid_ctr string, refund_rate string, cate_ctr string, cate_weight string, titleSim string, cateSim string, categoryId string, query_id string) partitioned by (ds string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' stored as parquet;

insert overwrite table zhouyf.gbdt_feature_train_vec_weight_svm partition(ds='${bizdate}')select pvid, item_id, concat(cast(label as string), ":", cast(nvl(weight,1) as string)) as label,
concat("1:",cast(ln(nvl(price,0)+1) as string)) as price,
concat("2:",cast(ln(nvl(sellcnt,0) + 1)  as string)) as sellcnt,
concat("3:",cast(ln(nvl(evlcnt,0) +1)  as string)) as evlcnt,
concat("4:",cast(ln(nvl(collectcnt,0) + 1) as string)) as collectcnt,
concat("5:",cast(ln(nvl(addCartCnt,0) + 1) as string)) as addCartCnt,
concat("6:",cast(nvl(interval_ctr,0) as string)) as ctr,
concat("7:",cast(nvl(uid_interval_ctr,0) as string)) as uid_ctr,
concat("8:",cast(nvl(refund_interval_rate,0) as string)) as refund_rate,
concat("9:",cast(nvl(cate_interval_ctr,0) as string)) as cate_interval_ctr,
concat("10:",cast(nvl(cate_weight,0) as string)) as cate_weight,
concat("11:", cast(vecSim(queryVec, titleVec) as string)) as titleSim,
concat("12:", cast(vecSim(queryVec,cateVec) as string)) as cateSim,
idsToFeatures(cast(nvl(categoryId,0) as string), 13, 1080) as categoryId,
idsToFeatures(cast(nvl(query_id,0) as string), 1094, 1000) as query_id
from
(
select * from zhouyf.gbdt_feature_train_vec_weight_sample where ds ='${bizdate}') a;
