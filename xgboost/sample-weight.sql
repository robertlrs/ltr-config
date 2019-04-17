
create table if not exists zhouyf.item_evl_sell(item_id bigint, evlcnt int, sellcnt int) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_evl_sell partition(ds='${statsday}')
select iid, evlcnt, sellcnt from dw.dwd_item_category_dd where ds = '${statsday}';

create table if not exists zhouyf.item_evl_sell_interval(item_id bigint, evlcnt int, evlcnt_interval int, sellcnt int,
 sellcnt_interval int) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_evl_sell_interval partition(ds='${statsday}')
select m.item_id, m.evlcnt, (m.evlcnt - n.evlcnt) as evlcnt_interval, m.sellcnt,
  (m.sellcnt - n.sellcnt) as sellcnt_interval from 
(select * from zhouyf.item_evl_sell where ds = '${statsday}') m
left join
  (select iid, evlcnt, sellcnt from dw.dwd_item_category_dd where ds = '${startday}') n
on m.item_id = n.iid;

create table if not exists zhouyf.item_collect(item_id bigint, collectcnt int) partitioned by (ds string)
stored as parquet;

insert overwrite table zhouyf.item_collect partition(ds='${statsday}')
select item.iid as item_id, count(*) as collectcnt from
 kaipao.kp_item item
join
(select uid, targetid from kaipao.kp_collect where
from_unixtime(createtm, 'yyyy-MM-dd') <= '${statsday}' and type = 3) kc
on item.pid = kc.targetid group by item.iid;


create table if not exists zhouyf.item_collect_interval(item_id bigint, collectcnt_interval int) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_collect_interval partition(ds='${statsday}')
select item.iid as item_id, count(*) as collectcnt_interval from
kaipao.kp_item item
join
(select uid, targetid from kaipao.kp_collect where
from_unixtime(createtm, 'yyyy-MM-dd') between '${startday}' and '${statsday}' and type = 3) kc
on item.pid = kc.targetid group by item.iid;

create table if not exists zhouyf.item_add_cart(item_id bigint, add_cart_cnt int) partitioned by (ds string)
stored as parquet;

insert overwrite table zhouyf.item_add_cart partition(ds='${statsday}')
select item_id, count(*) as add_cart_cnt from kaipao.dj_cart where
from_unixtime(cast(create_time/1000 as bigint), 'yyyy-MM-dd') <= '${statsday}' group by item_id;

create table if not exists zhouyf.item_add_cart_interval(item_id bigint, add_cart_cnt_interval int) partitioned by (ds string)
stored as parquet;

insert overwrite table zhouyf.item_add_cart_interval partition(ds='${statsday}')
select item_id, count(*) as add_cart_cnt from kaipao.dj_cart where
from_unixtime(cast(create_time/1000 as bigint), 'yyyy-MM-dd') between '${startday}' and '${statsday}' group by item_id;

create table if not exists zhouyf.item_stats_info(item_id bigint, evlcnt int, evlcnt_interval int, sellcnt int, sellcnt_interval int,collectcnt int, collectcnt_interval int, add_cart_cnt int, add_cart_cnt_interval int) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.item_stats_info partition(ds='${statsday}') 
select a.item_id, nvl(a.evlcnt, 0) as evlcnt, nvl(a.evlcnt_interval, 0) as evlcnt_interval, 
nvl(a.sellcnt, 0) as sellcnt, nvl(a.sellcnt_interval, 0) as sellcnt_interval, 
nvl(b.collectcnt, 0) as collectcnt, nvl(b.collectcnt_interval, 0) as collectcnt_interval,  nvl(c.add_cart_cnt, 0) as add_cart_cnt, nvl(c.add_cart_cnt_interval, 0) as add_cart_cnt_interval from 
(select * from zhouyf.item_evl_sell_interval where ds='${statsday}') a 
full join 
  (select m.item_id, m.collectcnt, n.collectcnt_interval from
     (select * from zhouyf.item_collect where ds='${statsday}') m
 left join
     (select * from zhouyf.item_collect_interval where ds = '${statsday}') n
  on m.item_id = n.item_id) b on a.item_id = b.item_id
full join 
   (select p.item_id, p.add_cart_cnt, q.add_cart_cnt_interval from
     (select * from zhouyf.item_add_cart where ds='${statsday}') p
  left join
     (select * from zhouyf.item_add_cart_interval where ds = '${statsday}') q
   on p.item_id = q.item_id) c on b.item_id = c.item_id;
