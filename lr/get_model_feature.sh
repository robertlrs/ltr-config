#!/bin/bash


scp -P10022 hdfs@hadoop-monitor:/data/pyspark/program/songwt/lr/lr_model.json .
scp -P10022 hdfs@hadoop-monitor:/data/pyspark/program/songwt/lr/lr_features.json .
