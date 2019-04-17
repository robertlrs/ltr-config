--1.搜索pv、cpv--
create table if not exists zhouyf.search_pv_click_cate(label tinyint, keyword string, item_id bigint,
rank_index bigint, requesttime string, pvid string) partitioned by (ds string) stored as parquet;


insert overwrite table zhouyf.search_pv_click_cate partition(ds='${bizdate}')
  select case when a.item_id = b.item_id then 1 else 0 end as label, a.keyword, a.item_id, a.pos as rank_index, a.requesttime, a.pvid from
    (select pvid, cast(item_id as bigint) as item_id, get_json_object(query, '$.queryStr') keyword, pos, requesttime from 
   (select * from dim.dim_unpack_search_pv where ds='${bizdate}' 
    and scenario ='category_item_search') m
  left join
    (select * from dw.dim_staff_info) n
  on m.uid = n.uid where n.uid is null) a 
   left join
    (select pvid, cast(item_id as bigint) as item_id from dim.dim_search_click where ds='${bizdate}' and scenario='category_item_search') b
   on a.pvid = b.pvid and a.item_id  = b.item_id;

--2.根据点击位置来调整负样本采用--
create table if not exists zhouyf.search_click_index_cate(pvid string, click_index bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_click_index_cate partition(ds='${bizdate}')
  select pvid, case max(rank_index) when 1 then 4 when 2 then 4 when 3 then 4 else max(rank_index) end as click_index from zhouyf.search_pv_click_cate where ds = '${bizdate}' and label = 1 group by pvid;

--3.根据skip-above选取样本--

create table if not exists zhouyf.search_pv_click_filter_cate(label tinyint, keyword string, item_id bigint,searchtime string, pvid string) partitioned by (ds string) stored as parquet;
  
insert overwrite table zhouyf.search_pv_click_filter_cate partition(ds='${bizdate}') 
select a.label, a.keyword, a.item_id, a.requesttime, a.pvid from
 (select * from zhouyf.search_pv_click_cate where ds = '${bizdate}') a, 
 (select * from zhouyf.search_click_index_cate where ds = '${bizdate}') b 
where a.pvid = b.pvid and a.rank_index <= b.click_index;

--4.联合搜索词的id--

create table if not exists zhouyf.search_pv_join_word_id_seg_word_cate(label tinyint, keyword string, item_id bigint, searchtime string, pvid string, seg_word string, query_id bigint) partitioned by (ds string) stored as parquet;
 
insert overwrite table zhouyf.search_pv_join_word_id_seg_word_cate partition(ds='${bizdate}')
select a.label, a.keyword, a.item_id, a.searchtime, a.pvid, segword(a.keyword) as seg_word, nvl(b.queryid,0) as query_id from
  (select label, keyword, item_id, searchtime, pvid from zhouyf.search_pv_click_filter_cate  where ds='${bizdate}') a
 left join
   zhouyf.top_keyword b on a.keyword = b.query;

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
