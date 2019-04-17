create table if not exists zhouyf.gbdt_feature_train_vec_weight_sample(cid bigint,pvid string,label tinyint, weight int, 
    item_id bigint, keyword string,seg_word string,attrs string, preCategoryList string, 
   price double, difftm double, sellcnt int, sellcnt_interval int, evlcnt int, evlcnt_interval int,collectcnt int, collectcnt_interval int, addCartCnt int, addCartCnt_interval int, 
   titleSmart string, ctr double, 
    interval_ctr double, uid_ctr double, uid_interval_ctr double, 
    refund_rate double, refund_interval_rate double, cate_ctr double, 
    cate_interval_ctr double, cate_weight double, query_id int, categoryId int, queryVec string, titleVec string, cateVec string) partitioned by (ds string)
   ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
   stored as parquet;


insert overwrite table zhouyf.gbdt_feature_train_vec_weight_sample partition(ds='${bizdate}')
select c.cid, c.pvid, c.label,c.weight, c.item_id, c.keyword, c.seg_word, c.attrs, c.preCategoryList, c.price, c.difftm, c.sellcnt, c.sellcnt_interval, c.evlcnt, c.evlcnt_interval,c.collectcnt, c.collectcnt_interval,
 c.addCartCnt, c.addCartCnt_interval, c.titleSmart, c.ctr, c.interval_ctr, c.uid_ctr, c.uid_interval_ctr,
c.refund_rate, c.refund_interval_rate, c.cate_ctr, c.cate_interval_ctr,
c.cate_weight, c.query_id, c.categoryId, d.vec as queryVec, e.title_vec as titleVec, e.cats_vec as cateVec from
(select a.*, normalized_word from 
  (select * from zhouyf.gbdt_feature_train_weight where ds = '${bizdate}') a
left join 
   zhouyf.word_mapping b
on concat_ws('', split(a.keyword, ' ')) = b.keyword) c  
left join
   (select norm_search_query, vec from zhl.dim_segmented_history_queries) d
  on c.normalized_word = d.norm_search_query
left join
    (select iid, title_vec, cats_vec from zhl.dim_segmented_items) e
on c.item_id = e.iid;


