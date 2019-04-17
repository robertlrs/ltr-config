--1.搜索pv、cpv--

--5.关联商品信息--

 create table if not exists zhouyf.item_pv_click_seg_word(label tinyint, keyword string, item_id bigint, searchtime bigint, pvid string, seg_word string, query_id bigint, attrs string,  precategorylist string, price double, createtm bigint, updatetm bigint, title string, titleSmart string, categoryId int, evlcnt int, sellcnt int, collectcnt bigint, addCartCnt bigint, uid bigint, city string)  partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_pv_click_seg_word partition(ds='${bizdate}')
 select b.label, b.keyword, b.item_id, unix_timestamp(b.searchtime,'yyyyMMddHHmmss') as searchtime, b.pvid, b.seg_word, b.query_id, a.attrs, a.precategorylist, cast(a.price as double) as price, cast(a.createtm as bigint) as createtm, cast(a.updatetm as bigint) as updatetm, a.title, a.titleSmart, cast(a.categoryId as int) categoryId, 
   case when c.evlcnt = -1 then 0 else c.evlcnt end as evlcnt, 
   case when c.sellcnt = -1 then 0 else c.sellcnt end as sellcnt, 
   case when c.collectcnt = -1 then 0 else c.collectcnt end as collectcnt, c.add_cart_cnt, cast(a.uid as bigint), a.city from 
     (select * from zhouyf.search_pv_join_word_id_seg_word where ds='${bizdate}') b
left join
    zhouyf.item_index_new a on cast(a.iid as bigint) = b.item_id
left join (select * from zhouyf.item_stats_info where ds='${bizdate}') c on b.item_id = c.item_id;
