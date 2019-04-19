cd /opt/task/zhouyf/xgboost/newmodel

drop table if exists zhouyf.top_keyword;
create table zhouyf.top_keyword
as
select row_number() over (order by count desc) as row_num,* from (
select  split_part(pid,"_", 3) as keyword,count(*) as count 
from logs.app_log  
where pid like "search_i_%" and eid = "request"
group by pid
order by count desc
limit 1000
) a;;
