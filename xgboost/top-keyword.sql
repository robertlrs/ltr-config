drop table if exists zhouyf.keyword_stats;

create table zhouyf.keyword_stats as
select * from 
  (select get_json_object(query, '$.queryStr') as keyword, count(1) as cnt from dim.dim_unpack_search_pv where scenario ='item_search'  and ds > '${bizdate}' group by get_json_object(query, '$.queryStr')) a
where a.keyword is not null;


drop table if exists zhouyf.top_keyword;

create table zhouyf.top_keyword as
select keyword as query, row_number() over (order by cnt desc) as queryid from
zhouyf.keyword_stats limit 1000;  
