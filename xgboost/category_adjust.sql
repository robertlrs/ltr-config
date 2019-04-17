insert overwrite table zhouyf.item_pv_click_seg_word partition(ds='${bizdate}')
select a.label, a.keyword, a.item_id, a.searchtime, a.pvid, a.seg_word, a.query_id, a.attrs, a.precategorylist,
a.price, a.createtm, a.updatetm, a.title, a.titleSmart, b.category_id, a.evlcnt, a.sellcnt, a.collectcnt, a.addCartCnt, a.uid, a.city 
from
  (select * from zhouyf.item_pv_click_seg_word where ds = '${bizdate}') a
left join 
  (select iid, category_id from dw.dwd_item_category_dd where ds = '${bizdate}') b
on a.item_id = b.iid;
