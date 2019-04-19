invalidate metadata;

create table if not exists zhouyf.search_pv_click_android(label tinyint, keyword string, item_id bigint,rank_index bigint, searchtime string, pvid string) partitioned by (ds string)
stored as parquet;

insert overwrite table zhouyf.search_pv_click_android partition(ds='${var:statsday}')
select case when a.item_id = b.item_id then 1 else 0 end as label, a.keyword,  cast(a.item_id as bigint) as item_id, a.pos as rank_index,
a.searchtime, a.pvid from
(select m.*, n.keyword from
(select split_part(spm, '.', 6) as pvid, id as item_id, pos, requesttime as searchtime
from logs.backend_log where ds = '${var:statsday}' and spm like 'dj.search.keyword.item%'
and platform = 'ANDROID' and split_part(spm, '.', 6) not in (select pvid from
(select split_part(spm, '.', 6) as pvid, count(*) as cnt
from logs.backend_log where ds = '${var:statsday}' and spm like 'dj.search.keyword.item%'
and platform = 'ANDROID' group by split_part(spm, '.', 6)) temp where temp.cnt > 20)) m,
(select distinct split_part(spm, '.', 6) as pvid, split_part(pid, '_', 3) as keyword from
logs.app_log where ds = '${var:statsday}' and pid like 'search_i%' and eid='click' and platform = '2') n where m.pvid = n.pvid) a
left join
(select pvid, keyword, item_id from
(select split_part(spm, '.', 6) as pvid, split_part(pid, '_', 3)
as keyword, time, pid, device_number from logs.app_log where ds = '${var:statsday}' and pid like 'search_i%'
and eid='click' and platform ='2') m
join
(select referer_id, split_part(pid, '_', 2) item_id, time, device_number from
logs.app_log where ds = '${var:statsday}' and
referer_id like 'search_i%' and pid like'item_%' and eid = 'view' and platform = '2') n
on m.pid = n.referer_id and m.time = n.time and m.device_number = n.device_number) b on a.pvid = b.pvid and a.item_id = b.item_id;

create table if not exists zhouyf.search_click_index_compliment_android(pvid string, click_index bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_click_index_compliment_android partition(ds='${var:statsday}')
select pvid, case max(rank_index) when 1 then 4 when 2 then 4 when 3 then 4 else max(rank_index) end as click_index from 
zhouyf.search_pv_click_android where ds = '${var:statsday}' and label =1 group by pvid;

create table if not exists zhouyf.search_pv_click_filter_compliment_android(label tinyint, keyword string, item_id bigint, searchtime string, pvid string) partitioned by (ds string)
stored as parquet;

insert overwrite table zhouyf.search_pv_click_filter_compliment_android partition(ds='${var:statsday}')
select a.label, a.keyword, a.item_id, a.searchtime, a.pvid from 
(select * from zhouyf.search_pv_click_android where ds = '${var:statsday}')  a, (select * from zhouyf.search_click_index_compliment_android 
where ds = '${var:statsday}') b where a.pvid = b.pvid and a.rank_index <= b.click_index;

create function if not exists tokenInSet(string,string) returns boolean location 'hdfs://hadoop-manager:8020/user/hdfs/wanbin/udf-1.0-SNAPSHOT.jar' symbol='com.dongjia.search.TokenInSet';
create function if not exists segword(string) returns string location 'hdfs://hadoop-manager:8020/user/hdfs/wanbin/seg_word-1.0-SNAPSHOT-all.jar' symbol='com.dongjia.search.SegWordWrapper';

--search_pv_click + query_id--
create table if not exists zhouyf.search_pv_join_word_id_seg_word_compliment_android(label tinyint, keyword string, item_id bigint, searchtime string, pvid string, seg_word string,query_id bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_pv_join_word_id_seg_word_compliment_android partition(ds='${var:statsday}')
select a.label, a.keyword, a.item_id, a.searchtime, a.pvid, segword(a.keyword) as seg_word, isnull(b.row_num,0) as query_id from 
(select label, keyword, item_id, searchtime, pvid from zhouyf.search_pv_click_filter_compliment_android where ds='${var:statsday}') a
left join 
zhouyf.top_keyword b on a.keyword = b.keyword;

create table if not exists zhouyf.item_pv_click_seg_word_train_compliment_android(label tinyint, keyword string, item_id bigint, searchtime bigint, pvid string, seg_word string, query_id bigint, attrs string, precategorylist string, price double, createtm int, contenttype int, title_smart string, categoryId int,evlcnt int, sellcnt int, collectcnt bigint, add_cart_cnt bigint, uid bigint, city string) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_pv_click_seg_word_train_compliment_android partition(ds='${var:statsday}') 
select b.label, b.keyword, b.item_id, unix_timestamp(to_timestamp(b.searchtime, 'yyyyMMddHHmmss')) as searchtime, b.pvid, b.seg_word, b.query_id, 
a.attrs, a.precategorylist, cast(a.price as double) as price, cast(cast(a.createtm as bigint)/1000 as int) as createtm, cast(a.contentType as int) contenttype, a.title_smart, cast(a.categoryId as int) categoryId, case when c.evlcnt = -1 then 0 else c.evlcnt end, case when c.sellcnt = -1 then 0 else c.sellcnt end, case when c.collectcnt = -1 then 0 else c.collectcnt end, c.add_cart_cnt , cast(a.uid as bigint), a.city from (select * from zhouyf.search_pv_join_word_id_seg_word_compliment_android where ds='${var:statsday}') b 
left join zhouyf.item_index_new a on cast(a.iid as bigint) = b.item_id 
left join (select * from zhouyf.item_stats_info where ds='${var:statsday}') c on b.item_id = c.item_id;

