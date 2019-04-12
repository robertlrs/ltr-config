import os

os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession
import avro.schema
import getopt
import sys
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from datetime import datetime, date, timedelta

def getctr(ctr):
    if ctr <= 0.075:
        ctr_level = 0
    elif ctr > 0.075 and ctr <= 0.08730159:
        ctr_level = 1
    elif ctr > 0.08730159 and ctr <= 0.09428571:
        ctr_level = 2
    elif ctr > 0.09428571 and ctr <= 0.09803922:
        ctr_level = 3
    elif ctr > 0.09803922 and ctr <= 0.1015625:
        ctr_level = 4
    elif ctr > 0.1015625 and ctr <=  0.11111111:
        ctr_level = 5
    elif ctr > 0.11111111 and ctr <= 0.11956522:
        ctr_level = 6
    elif ctr > 0.11956522 and ctr <= 0.13461538:
        ctr_level = 7
    elif ctr > 0.13461538 and ctr <= 0.15957447:
        ctr_level = 8
    else:
        ctr_level = 9
    return ctr_level

def getcollect(collect):
    if collect == 0:
        collect_level = 0
    elif collect == 1:
        collect_level = 1
    elif collect > 1 and collect <= 3:
        collect_level = 2
    elif collect > 3 and collect <= 16:
        collect_level = 3
    else:
        collect_level = 4
    return collect_level

def getaddcart(addcart):
    if addcart == 0:
        addcart_level = 0
    elif addcart == 1:
        addcart_level = 1
    elif addcart == 2:
        addcart_level = 2
    elif addcart == 3:
        addcart_level = 3
    else:
        addcart_level = 4
    return addcart_level

def getsale(sale):
    sale_level = 4
    if sale == 0:
        sale_level = 0
    elif sale == 1:
        sale_leval = 1
    elif sale == 2:
        sale_level = 2
    elif sale == 2:
        sale_level = 3
    return sale_level

def craftsman():
    craftsman = dict()
    for ln in open("craftsman.csv", 'rb'):
        ln = ln.strip().decode("utf8")
        lnn = ln.split(",")
        city = lnn[0]
        normalize_city = lnn[3]
        craftsman[city] = normalize_city
    return craftsman
 

def main(f, ds):

    spark = SparkSession.builder.appName("lr_sample_train").getOrCreate()
    schema = avro.schema.Parse(open("lrAvro.avsc").read())
    writer = DataFileWriter(open("sample/%s.avro" % f, "wb"), DatumWriter(), schema)

    df_sale = spark.sql("select item_id, sale from songwt.item_sale where ds =%s" % ds) 
    sale_dict = dict()
    for row in df_sale.collect():
        sale_dict[row.item_id] = row.sale
    
    sql = """
    SELECT devoruid, collect_set(categoryid) as prefer_cid from 
    (SELECT devoruid, categoryid, row_number() OVER (PARTITION BY devoruid ORDER BY score DESC) as num 
    from features.user_categoryid_preference WHERE ds=%s and score > 1) t where num <= 5 GROUP BY devoruid
    """ % ds

    df_prefer = spark.sql(sql)
    prefer_dict = dict()
    for row in df_prefer.collect():
        prefer_dict[row.devoruid] = row.prefer_cid
 
    path = "/user/yarn/swt/sample/%s" % f


    sample = spark.sparkContext.textFile(path)

    lines = sample.collect()
    sellcity = craftsman()
    for line in lines:
        ln = line.replace("(", "")
        ln2 = ln.replace(")", "")
        lnn = ln2.split(",")
        features = dict()
        _l = list()
        prefer_tag = []
        
        if lnn == "null" or lnn == None:
            continue
    
        for idx, item in enumerate(lnn):
            ft = dict()
            if idx == 0:
                prefer_cid = prefer_dict.get(item, list())
                
            elif idx == 1:
                ft2 = dict()
                sale = sale_dict.get(int(item),0)
                level = getsale(sale)
                ft2["name"] = "sale"
                ft2["term"] = str(level)
                ft2["value"] = 1.0
                _l.append(ft2)
                
            elif idx == 2:
                features["label"] = int(item)
            else:
                values = item.split("=")
                name = values[0]
                if name in ["collect2", "add2", "show2", "click2", "city_city", "province"]:
                    continue
                term = values[1].split(":")[0]
                value = values[1].split(":")[1]
                if name.find("tag_ptag") != -1:
                    ptag = term.split("_")[1]
                    prefer_tag.append(ptag)
                    continue
                if name == "collect1":
                    new_term = getcollect(float(term))
                elif name == "add1":
                    new_term = getaddcart(float(term))
                elif name == "cid":
                    cid = int(term)
                    new_term = term
                elif name == "tag":
                    tag = term
                    new_term = term
                elif name.find("city") != -1:
                    
                    city = term.split("_")[1]
                    new_city = sellcity.get(city, "")
                    new_term = term.split("_")[0]+"_"+new_city
                    
                elif name == "show1":
                    exposure = float(term)
                    continue
                elif name == "click1":
                    click = float(term)
                    continue
                else:
                    new_term = term
                ft["name"] = name
                ft["term"] = str(new_term)
                ft["value"] = float(value)
                _l.append(ft)
        ctr = (click+5) / (exposure+50)
        ctr_level = getctr(ctr)
        ctr_dict = dict()
        ctr_dict["name"] = "ctr"
        ctr_dict["value"] = 1.0
        ctr_dict["term"] = str(ctr_level)
        _l.append(ctr_dict)
        
        if cid in prefer_cid:
            ifcid = "1"
        else:
            ifcid = "0"
            
        if tag in prefer_tag:
            iftag = "1"
        else:
            iftag = "0"
            
        prefer_cid_dict = dict()
        prefer_cid_dict["name"] = "ifcid"
        prefer_cid_dict["term"] = ifcid
        prefer_cid_dict["value"] = 1.0
        _l.append(prefer_cid_dict)
        
        prefer_tag_dict = dict()
        prefer_tag_dict["name"] = "iftag"
        prefer_tag_dict["term"] = iftag
        prefer_tag_dict["value"] = 1.0
        _l.append(prefer_tag_dict)
        
        ctr_sale_dict = dict()
        ctr_sale_dict["name"] ="ctr_sale"
        ctr_sale_dict["term"] = str(ctr_level)+"_"+str(level)
        ctr_sale_dict["value"] = 1.0
        _l.append(ctr_sale_dict)
 
        features["features"] = _l
        writer.append(features)
    writer.close()

if __name__ == "__main__":
    date = sys.argv[1]
    date2 = sys.argv[2]

    f = "sample_%s" % str(date)
    main(f, date2)
