drop table if exists zhouyf.item_pv_click;

--考虑累计数据--
create table  zhouyf.item_pv_click as 
select item_id, uid, categoryid, label from zhouyf.item_pv_click_seg_word where ds <= '${bizdate}';


--一段时间内的情况--
drop table if exists zhouyf.item_pv_click_interval;

create table  zhouyf.item_pv_click_interval as
select item_id, uid, categoryid, label from zhouyf.item_pv_click_seg_word where ds between
 '${startdate}' and '${bizdate}';

--按商品统计--
create table if not exists zhouyf.search_item_pv (item_id bigint, pv bigint) partitioned by (ds string) stored as parquet;
insert overwrite table zhouyf.search_item_pv partition(ds='${bizdate}')
select item_id, count(1) as  pv from zhouyf.item_pv_click group by item_id;

create table if not exists zhouyf.search_item_click (item_id bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_item_click partition(ds='${bizdate}')
select item_id, count(1) as cpv from (select * from zhouyf.item_pv_click where label = 1) p group by item_id;


 --一段时间内商品pv的统计--
create table if not exists zhouyf.search_item_pv_interval (item_id bigint, pv bigint) partitioned by (ds string) stored as parquet;
insert overwrite table zhouyf.search_item_pv_interval partition(ds='${bizdate}')
select item_id, count(1) as pv from zhouyf.item_pv_click_interval group by item_id;

create table if not exists zhouyf.search_item_click_interval (item_id bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_item_click_interval partition(ds='${bizdate}')
select item_id, count(1) as cpv from zhouyf.item_pv_click_interval where label = 1 group by item_id;


--商品综合统计--
create table if not exists zhouyf.search_item_ctr (item_id bigint, ctr double, interval_ctr double) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_item_ctr partition(ds='${bizdate}')
select a.item_id, a.ctr, b.ctr from 
   (select pv.item_id,
      case when click.cpv is null then 0.03
      else (click.cpv+3)/(pv.pv+100)
      end as ctr
     from (select * from zhouyf.search_item_pv where ds = '${bizdate}')  pv
       left join (select * from zhouyf.search_item_click where ds = '${bizdate}') click 
         on pv.item_id = click.item_id) a
  left join 
      (select m.item_id,
      case when n.cpv is null then 0.03
      else (n.cpv+3)/(m.pv+100)
      end as ctr
    from (select * from zhouyf.search_item_pv_interval where ds = '${bizdate}') m
       left join (select * from zhouyf.search_item_click_interval where ds = '${bizdate}') n 
     on m.item_id = n.item_id) b
  on a.item_id = b.item_id;


--匠人统计--
create table if not exists zhouyf.search_uid_pv (uid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_uid_pv partition(ds='${bizdate}')
select uid, count(1) as pv from zhouyf.item_pv_click group by uid;

create table if not exists zhouyf.search_uid_click (uid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_uid_click partition(ds='${bizdate}')
select uid, count(1) as cpv from zhouyf.item_pv_click where label = 1 group by uid;

--一段时间的匠人pv--
create table if not exists zhouyf.search_uid_pv_interval (uid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_uid_pv_interval partition(ds='${bizdate}')
select uid, count(1) as pv from zhouyf.item_pv_click_interval group by uid;

create table if not exists zhouyf.search_uid_click_interval (uid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_uid_click_interval partition(ds='${bizdate}')
select uid, count(1) as cpv from (select * from zhouyf.item_pv_click_interval where label = 1) p group by uid;



create table if not exists zhouyf.search_uid_ctr (uid bigint, ctr double, interval_ctr double) partitioned by (ds string) stored as parquet; 



insert overwrite table zhouyf.search_uid_ctr partition(ds='${bizdate}')
select a.uid, a.ctr, b.ctr from
 (select pv.uid, 
    case when click.cpv is null then 0.03
    else (click.cpv+3)/(pv.pv+100)
    end as ctr 
   from (select * from zhouyf.search_uid_pv where ds ='${bizdate}') pv
     left join (select * from zhouyf.search_uid_click where ds = '${bizdate}') click on pv.uid = click.uid) a
left join
  (select m.uid, 
        case when n.cpv is null then 0.03
        else (n.cpv+3)/(m.pv+100) 
        end as ctr
        from (select * from zhouyf.search_uid_pv_interval where ds = '${bizdate}') m
      left join 
        (select * from zhouyf.search_uid_click_interval where ds = '${bizdate}') n on m.uid = n.uid) b
on a.uid = b.uid ;

--类目统计信息--
create table if not exists zhouyf.search_cate_pv (categoryid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_cate_pv partition(ds='${bizdate}')
select categoryid, count(1) as pv from  
zhouyf.item_pv_click group by categoryid;

create table if not exists zhouyf.search_cate_click (categoryid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_cate_click partition(ds='${bizdate}')
select categoryid, count(1) as cpv from
zhouyf.item_pv_click_interval where label = 1 group by categoryid;


--一段时间的类目统计--
create table if not exists zhouyf.search_cate_pv_interval (categoryid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_cate_pv_interval partition(ds='${bizdate}')
select categoryid, count(1) as pv from
zhouyf.item_pv_click_interval group by categoryid;

create table if not exists zhouyf.search_cate_click_interval (categoryid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.search_cate_click_interval partition(ds='${bizdate}')
select categoryid, count(1) as cpv from 
(select * from zhouyf.item_pv_click_interval where label = 1) p group by categoryid;



create table if not exists zhouyf.search_cate_ctr (categoryid bigint, ctr double, interval_ctr double) partitioned by (ds string) stored as parquet; 

insert overwrite table zhouyf.search_cate_ctr partition(ds='${bizdate}')
select a.categoryid, a.ctr, b.ctr from
(select pv.categoryid, 
    case
    when click.cpv is null then 0.03
    else (click.cpv+3)/(pv.pv+100)
    end as ctr
  from (select * from zhouyf.search_cate_pv where ds = '${bizdate}') pv
  left join (select * from zhouyf.search_cate_click where ds = '${bizdate}') click on pv.categoryid = click.categoryid) a
left join 
  (select m.categoryid,
        case when n.cpv is null then 0.03
        else (n.cpv+3)/(m.pv+100)
        end as ctr
        from (select * from zhouyf.search_cate_pv_interval where ds = '${bizdate}') m
    left join
        (select * from zhouyf.search_cate_click_interval where ds = '${bizdate}') n on m.categoryid = n.categoryid) b
on a.categoryid = b.categoryid;;
