#!/bin/bash

#curl -XPOST http://localhost:9200/_ltr/_featureset/index_lr_test/_createmodel -d @lr_model.json --header "Content-Type: application/json"

curl -XPOST http://172.16.4.25:9200/_ltr/_featureset/index_lr_test/_createmodel -d @lr_model.json --header "Content-Type: application/json"

