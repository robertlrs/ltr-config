invalidate metadata;

drop table if exists zhouyf.search_pv_click_union;

create table  zhouyf.search_pv_click_union as 
select item_id, label from zhouyf.search_pv_click where ds <= '${var:statsday}'
union all
select item_id, label from zhouyf.search_pv_click_android where ds <= '${var:statsday}'; 

create table if not exists zhouyf.search_item_pv (item_id bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_item_pv partition(ds='${var:statsday}')
select item_id, count(1) as  pv from zhouyf.search_pv_click_union group by item_id;

create table if not exists zhouyf.search_item_click (item_id bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_item_click partition(ds='${var:statsday}')
select item_id, count(1) as cpv from (select * from zhouyf.search_pv_click_union where label = 1) p group by item_id;

create table if not exists zhouyf.search_item_ctr (item_id bigint, ctr double) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_item_ctr partition(ds='${var:statsday}')
select pv.item_id, 
    case 
    when click.cpv is null then 0.03
    else (click.cpv+3)/(pv.pv+100)  
    end as ctr
from (select * from zhouyf.search_item_pv where ds = '${var:statsday}')  pv
left join (select * from zhouyf.search_item_click where ds = '${var:statsday}') click on pv.item_id = click.item_id;

drop table if exists zhouyf.item_pv_click_union;

create table  zhouyf.item_pv_click_union as 
select item_id, uid, categoryid, label from zhouyf.item_pv_click_seg_word_train_compliment where ds <= '${var:statsday}' union all 
select item_id, uid, categoryid, label  from zhouyf.item_pv_click_seg_word_train_compliment_android where ds < = '${var:statsday}'; 

create table if not exists zhouyf.search_uid_pv (uid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_uid_pv partition(ds='${var:statsday}')
select uid, count(1) as pv from zhouyf.item_pv_click_union group by uid;

create table if not exists zhouyf.search_uid_click (uid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_uid_click partition(ds='${var:statsday}')
select uid, count(1) as cpv from (select * from zhouyf.item_pv_click_union where label = 1) p group by uid;

create table if not exists zhouyf.search_uid_ctr (uid bigint, ctr double) partitioned by (ds string) stored as parquet; 



insert into zhouyf.search_uid_ctr partition(ds='${var:statsday}')
select pv.uid, 
    case
    when click.cpv is null then 0.03
    else (click.cpv+3)/(pv.pv+100)
    end as ctr
from (select * from zhouyf.search_uid_pv where ds ='${var:statsday}') pv
left join (select * from zhouyf.search_uid_click where ds = '${var:statsday}') click  on pv.uid = click.uid;


create table if not exists zhouyf.search_cate_pv (categoryid bigint, pv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_cate_pv partition(ds='${var:statsday}')
select categoryid, count(1) as pv from  
zhouyf.item_pv_click_union group by categoryid;

create table if not exists zhouyf.search_cate_click (categoryid bigint, cpv bigint) partitioned by (ds string) stored as parquet;

insert into zhouyf.search_cate_click partition(ds='${var:statsday}')
select categoryid, count(1) as cpv from 
(select * from zhouyf.item_pv_click_union where label = 1) p  group by categoryid;

create table if not exists zhouyf.search_cate_ctr (categoryid bigint, ctr double) partitioned by (ds string) stored as parquet; 



insert into zhouyf.search_cate_ctr partition(ds='${var:statsday}')
select  pv.categoryid, 
    case
    when click.cpv is null then 0.03
    else (click.cpv+3)/(pv.pv+100)
    end as ctr
from (select * from zhouyf.search_cate_pv where ds = '${var:statsday}') pv
left join (select * from zhouyf.search_cate_click where ds = '${var:statsday}') click on pv.categoryid = click.categoryid;
