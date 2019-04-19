regress=172.16.4.25
test=172.16.4.27
mytest=172.16.253.165
online=172.16.9.39
dev=172.16.4.23
curl -XPOST http://${online}:9200/_ltr/_featureset/search_xgboost_v1 -d @xgboost-featuresetnew.json --header "Content-Type: application/json"
