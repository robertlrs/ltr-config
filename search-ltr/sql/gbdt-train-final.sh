#recommend for you on the start page
cd /opt/task/zhouyf/xgboost
export JAVA_HOME=/opt/jdk1.8.0_121/
echo `date` "start stats"
zhouyf.gbdt_feature_train_craftsctr
impala-shell -i hadoop-data1:21000 -r -q "select label, hitattrs, hitprecategorylist, price, difftm, sellcnt,contenttype, evlcnt, collectcnt, add_cart_cnt, hittitle, query_id, ctr,uid_ctr,category_ctr,categoryid from zhouyf.gbdt_feature_train_craftsctr_hot  where ds <= '2018-05-06' union select label, hitattrs, hitprecategorylist, price, difftm, sellcnt,contenttype, evlcnt, collectcnt, add_cart_cnt, hittitle, query_id, ctr, uid_ctr, category_ctr, categoryid  from zhouyf.gbdt_feature_train_craftsctr_hot_android where ds <= '2018-05-08'"  -B --output_delimiter=" " -o gbdt_train_final_ctr_29.txt

