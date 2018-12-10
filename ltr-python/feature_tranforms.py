#!/usr/bin/env python
# coding=utf-8
import json
import codecs
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def city():
    usercity = list()
    for ln in codecs.open("city.csv", "rb"):
        ln = ln.strip()
        name = "userCity=%s" % ln.encode("utf8")
        usercity.append(name)
    return usercity

def mobile():
    mobiletype = list()
    for ln in codecs.open("mobile.csv", "rb"):
        ln = ln.strip()
        name = "mobileType=%s" % ln.encode("utf8")
        mobiletype.append(name)
    return mobiletype

def seller():
    sellercity = list()
    sellercity_sort = []
    for ln in codecs.open("province.csv", "rb"):
        ln = ln.strip()
        name = "province=%s" % ln 
        sellercity.append(name)
        sellercity_sort.append(int(ln))
    _max = max(sellercity_sort)
    return [sellercity, _max]


def leaf():
    leafcate = list()
    leafcate_sort = []
    leafcate.append("categoryId=0")
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "categoryId=%s" % ln
        leafcate.append(name)
        leafcate_sort.append(int(ln))
    _max = max(leafcate_sort)
    return [leafcate, _max]

def pid():
    parentid = list()
    parentid_sort = []
    parentid.append("parentCategoryId=0")
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "parentCategoryId=%s" % ln
        parentid.append(name)
        parentid_sort.append(int(ln))
    _max = max(parentid_sort)
    return [parentid, _max]

def hashseller():
    craftsman = list()
    for i in range(0, 7165):
        name = "hashUid=%d" % i
        craftsman.append(name)
    return [craftsman, 7164]


def preferleaf1():
    preferleaf = list()
    preferleaf.append("preferedCategory1=0")
    _sort = []

    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "preferedCategory1=%s" % ln
        preferleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [preferleaf, _max]

def preferleaf2():
    preferleaf = list()
    preferleaf.append("preferedCategory2=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "preferedCategory2=%s" % ln
        preferleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [preferleaf, _max]

def preferleaf3():
    preferleaf = list()
    preferleaf.append("preferedCategory3=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "preferedCategory3=%s" % ln
        preferleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [preferleaf, _max]

def preferleaf4():
    preferleaf = list()
    preferleaf.append("preferedCategory4=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "preferedCategory4=%s" % ln
        preferleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [preferleaf, _max]

def preferleaf5():
    preferleaf = list()
    preferleaf.append("preferedCategory5=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "preferedCategory5=%s" % ln
        preferleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [preferleaf, _max]

def orderleaf1():
    orderleaf = list()
    orderleaf.append("buyCategory1=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "buyCategory1=%s" % ln
        orderleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [orderleaf, _max]

def orderleaf2():
    orderleaf = list()
    orderleaf.append("buyCategory2=0")
    _sort = []
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "buyCategory2=%s" % ln
        orderleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [orderleaf, _max]

def orderleaf3():
    orderleaf = list()
    _sort = []
    orderleaf.append("buyCategory3=0")
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "buyCategory3=%s" % ln
        orderleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [orderleaf, _max]

def orderleaf4():
    orderleaf = list()
    _sort = []
    orderleaf.append("buyCategory4=0")
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "buyCategory4=%s" % ln
        orderleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [orderleaf, _max]

def orderleaf5():
    orderleaf = list()
    _sort = []
    orderleaf.append("buyCategory5=0")
    for ln in codecs.open("category.csv", "rb"):
        ln = ln.strip()
        name = "buyCategory5=%s" % ln
        orderleaf.append(name)
        _sort.append(int(ln))
    _max = max(_sort)
    return [orderleaf, _max]



def main():
    reItemCtr = [("reItemCtr=0", 0, 0.00584), ("reItemCtr=1", 0.00584, 0.00662), ("reItemCtr=2", 0.00662, 0.00686), ("reItemCtr=3", 0.00686, 0.00694), ("reItemCtr=4", 0.00694, 0.00698), 
            ("reItemCtr=5", 0.00698, 0.00699), 
            ("reItemCtr=6", 0.00699, 0.00735), ("reItemCtr=7", 0.00735, 0.00831), ("reItemCtr=8", 0.00831, 0.01041), ("reItemCtr=9",  0.01041, 1)]
    reItemCvr = [("reItemCvr=0", 0, 0), ("reItemCvr=1", 0, 0.00387), ("reItemCvr=2", 0.00387, 0.00394), ("reItemCvr=3", 0.00394, 0.00398), ("reItemCvr=4", 0.00398, 1)]
    reCateCtr = [("reCateCtr=0", 0, 0.00665), ("reCateCtr=1", 0.00665, 0.00863), ("reCateCtr=2", 0.00863, 0.00999), ("reCateCtr=3", 0.00999, 0.01343), ("reCateCtr=4", 0.01343, 1)]
    collect = [("collectInterval=0", 0, 0), ("collectInterval=1", 0, 1), ("collectInterval=2", 1, 4), ("collectInterval=3", 4, 9), ("collectInterval=4", 9, 16), ("collectInterval=5", 16, 100000)]
    addcart = [("addcartInterval=0", 0, 0), ("addcartInterval=1", 0, 1), ("addcartInterval=2", 1, 2 ), ("addcartInterval=3", 2, 3), ("addcartInterval=4", 3, 5), ("addcartInterval=5", 5, 1000000)]
    refund = [("refundRate=0", 0, 0), ("refundRate=1", 0, 0.04819277), ("refundRate=2", 0.04819277, 0.11111111), ("refundRate=3", 0.11111111, 0.25), ("refundRate=4", 0.25, 1)]
    categorygrade = ["categoryGrade=0", "categoryGrade=1", "categoryGrade=2", "categoryGrade=3", "categoryGrade=4", "categoryGrade=5"]
    buycategory = ["purchasedCategories=0", "purchasedCategories=1"]
    buyuid = ["purchasedCraftsmans=0", "purchasedCraftsmans=1"]
    gender = ["gender=0", "gender=1", "gender=2"]
    usercity = city()
    sellercity = seller()
    leafcate = leaf()
    parentid = pid()
    hashcraftsman = hashseller()
    features = list()

    for idx, name in enumerate(usercity):
        feature = dict()
        fe = name.split("=")[0]
        feature["name"] = name
        feature["type"] = "categorical"
        feature["channel"] = "input"
        feature["index"] = idx
        feature["max"] = 0 
        feature["field"] = fe
        feature["field_type"] = "string"
        feature["params"] = [fe]
        feature["template_language"] = "mustache"
        feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
        features.append(feature)

    for _input in [ mobile(), gender]:
        for idx, name in enumerate(_input):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "input"
            feature["index"] = idx
            feature["max"] = 2 
            feature["field"] = fe
            feature["field_type"] = "string"
            feature["params"] = [fe]
            feature["template_language"] = "mustache"
            feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
            features.append(feature)

    for _input in [preferleaf1(), preferleaf2(), preferleaf3(), preferleaf4(), preferleaf5(), orderleaf1(), orderleaf2(), orderleaf3(), orderleaf4(), orderleaf5()]:
        _input1 = _input[0]
        _max = _input[1]
        for idx, name in enumerate(_input1):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "input"
            feature["index"] = idx
            feature["max"] = _max 
            feature["field"] = fe
            feature["field_type"] = "string"
            feature["params"] = [fe]
            feature["template_language"] = "mustache"
            feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
            features.append(feature)

    for query in [sellercity, hashcraftsman]:
        query1 = query[0]
        _max = query[1]
        for idx, name in enumerate(query1):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "query"
            feature["index"] = idx
            feature["max"] = _max 
            feature["field"] = fe
            feature["field_type"] = "int"
            feature["template_language"] = "mustache"
            feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
            features.append(feature)

    for query in [leafcate, parentid]:
        query1 = query[0]
        _max = query[1]
        for idx, name in enumerate(query1):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "query"
            feature["index"] = idx
            feature["max"] = _max 
            feature["field"] = fe
            feature["field_type"] = "int"
            feature["template_language"] = "mustache"
            feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
            features.append(feature)

    for query in [categorygrade]:
        for idx, name in enumerate(query):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "query"
            feature["index"] = idx
            feature["max"] = 5
            feature["field"] = fe
            feature["field_type"] = "int"
            feature["template_language"] = "mustache"
            feature["template"] = {"function_score":{"field_value_factor":{"field":fe,"missing":0}}}
            features.append(feature)

    for query in [reItemCvr, collect, addcart, refund]:
        for idx, name in enumerate(query):
            feature = dict()
            fea = name[0]
            fe = fea.split("=")[0]
            dayu = name[1]
            xiaoyu = name[2]
            feature["name"] = fea 
            feature["type"] = "range"
            feature["channel"] = "query"
            feature["index"] = idx
            feature["max"] = len(query)-1
            feature['field'] = fe
            feature["field_type"] = "float"
            feature["template_language"] = "mustache"
            if idx == 0:
                feature["template"] = {"range":{fe:{"gte":0, "lte":0}}}
            else:
                feature["template"] = {"range":{fe:{"gt":dayu, "lte":xiaoyu}}} 
            features.append(feature)

    for query in [reItemCtr, reCateCtr]:
        for idx, name in enumerate(query):
            feature = dict()
            fea = name[0]
            fe = fea.split("=")[0]
            dayu = name[1]
            xiaoyu = name[2]
            feature["name"] = fea 
            feature["type"] = "range"
            feature["channel"] = "query"
            feature["index"] = idx
            feature["max"] = len(query)-1
            feature['field'] = fe
            feature["field_type"] = "float"
            feature["template_language"] = "mustache"
            feature["template"] = {"range":{fe:{"gt":dayu, "lte":xiaoyu}}} 
            features.append(feature)


    for binary in [buyuid, buycategory]:
        for idx, name in enumerate(binary):
            feature = dict()
            fe = name.split("=")[0]
            feature["name"] = name
            feature["type"] = "categorical"
            feature["channel"] = "binary"
            feature["index"] = idx
            feature["max"] = len(binary)-1
            feature["field"] = fe
            feature["field_type"] = "int"
            feature["params"] = [fe]
            feature["template_language"] = "mustache"
            if fe == "purchaseCategories":
                field = "categoryId"
            else:
                field = "uid"
            feature["template"] = {"function_score":{"field_value_factor":{"field":field, "missing":0}}}
            features.append(feature)


                


    _finally = {"validation":{"params":{"userCity":"杭州市", "mobileType":"iPhone9", "gender":1, "preferedCategory1":656, "preferedCategory2":656, "preferedCategory4":656,"preferedCategory3":656,
        "preferedCategory5":656, "buyCategory1":0, "buyCategory2":0, "buyCategory3":0, "buyCategory4":0, "buyCategory5":0, "purchasedCategories":[656,692], 
        "purchasedCraftsmans":[76288,902]}, "index":"item"}, 
        "featureset":{"features":features}} 
    f = open("lr_features.json", "w")
    json.dump(_finally, f)
    print "finished"


if __name__ == "__main__":
    main()

