invalidate metadata;

create table if not exists zhouyf.item_evl_sell(item_id bigint, evlcnt int, sellcnt int) 
partitioned by (ds string) stored as parquet;

insert into zhouyf.item_evl_sell partition(ds='${var:statsday}')
select iid, evlcnt, sellcnt from dw.dwd_item_category_dd where ds = '${var:statsday}';


create table if not exists zhouyf.item_collect(item_id bigint, collectcnt bigint) partitioned by (ds string)
stored as parquet;

insert into zhouyf.item_collect partition(ds='${var:statsday}')
select item.iid as item_id, count(*) as collectcn from 
kaipao.kp_item item
join
(select uid, targetid from kaipao.kp_collect where 
from_unixtime(createtm, 'yyyy-MM-dd') < '${var:statsday}') kc
on item.pid = kc.targetid group by item.iid;

create table if not exists zhouyf.item_add_cart(item_id bigint, add_cart_cnt bigint) partitioned by (ds string)
stored as parquet;

insert into zhouyf.item_add_cart partition(ds='${var:statsday}')
select item_id, count(*) as add_cart_cnt from kaipao.dj_cart where 
from_unixtime(cast(create_time/1000 as bigint), 'yyyy-MM-dd') < '${var:statsday}' group by item_id;

create table if not exists zhouyf.item_stats_info(item_id bigint, evlcnt int, sellcnt int,
collectcnt bigint, add_cart_cnt bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.item_stats_info partition(ds='${var:statsday}') 
select a.item_id, isnull(a.evlcnt, 0) as evlcnt, isnull(a.sellcnt, 0) as sellcnt, 
isnull(b.collectcnt, 0) as collectcnt, isnull(c.add_cart_cnt, 0) as add_cart_cnt from 
(select * from zhouyf.item_evl_sell where ds='${var:statsday}') a 
full join (select * from zhouyf.item_collect where ds='${var:statsday}') b on a.item_id = b.item_id
full join (select * from zhouyf.item_add_cart where ds='${var:statsday}') c on b.item_id = c.item_id;
