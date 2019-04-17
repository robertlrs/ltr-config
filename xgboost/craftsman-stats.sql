create table if not exists zhouyf.crafts_refund (uid bigint, refund_rate double, interval_rate double) partitioned by (ds string) stored as parquet;

insert overwrite table zhouyf.crafts_refund partition(ds='${statsday}')
select p.craftsman_uid as uid, p.refund_rate,  
case when q.refund_rate is null then 0
     else q.refund_rate
     end as interval_rate from
( 
select a.craftsman_uid, 
  case when b.cnt is null then 0
  else (b.cnt * 1.0 / a.cnt)
  end as refund_rate from
  (select craftsman_uid, count(1) as cnt from dw.dwd_order_item_dd 
  where ds ='${bizdate}' and (status>=3 or close_type in (4, 5, 6)) and 
  from_unixtime(unix_timestamp(pay_time, 'yyyy-MM-dd'), 'yyyy-MM-dd') <= '${statsday}'
  group by craftsman_uid) a
left join 
  (select craftsman_uid, count(1) as cnt from dw.dwd_order_item_dd 
  where ds ='${bizdate}' and refund in (2,5,10,12) and 
  from_unixtime(unix_timestamp(refund_successtm, 'yyyy-MM-dd'), 'yyyy-MM-dd') <= '${statsday}'
  group by craftsman_uid) b
 on a.craftsman_uid = b.craftsman_uid) p 
left join 
(select u.craftsman_uid,
    case when v.cnt is null then 0
     else (v.cnt * 1.0 / u.cnt)
     end as refund_rate from
    (select craftsman_uid, count(1) as cnt from dw.dwd_order_item_dd
    where ds ='${bizdate}' and (status>=3 or close_type in (4, 5, 6)) and
    from_unixtime(unix_timestamp(pay_time, 'yyyy-MM-dd'), 'yyyy-MM-dd')  between '${startday}' and '${statsday}'
    group by craftsman_uid) u
  left join
    (select craftsman_uid, count(1) as cnt from dw.dwd_order_item_dd
    where ds ='${bizdate}' and refund in (2,5,10,12) and
    from_unixtime(unix_timestamp(refund_successtm, 'yyyy-MM-dd'), 'yyyy-MM-dd') between '${startday}' and '${statsday}'
    group by craftsman_uid) v
   on u.craftsman_uid = v.craftsman_uid) q
on p.craftsman_uid = q.craftsman_uid;
