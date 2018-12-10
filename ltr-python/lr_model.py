import codecs
import json

feature = {"gender":"gender", "mobile":"mobileType","city":"userCity", "cid":"categoryId", "parent":"parentCategoryId",
        "ctr":"reItemCtr","cvr":"reItemCvr", "collect":"collectInterval", "addcart":"addcartInterval", "leafctr":"reCateCtr",
        "craftsman":"hashUid", "refund":"refundRate", "" 
        "cid_pcid1":"categoryId_preferedCategory1", "cid_pcid2":"categoryId_preferedCategory2", "cid_pcid3":"categoryId_preferedCategory3",
        "cid_pcid4":"categoryId_preferedCategory4", "cid_pcid5":"categoryId_preferedCategory5",
        "cid_ocid1":"categoryId_buyCategory1", "cid_ocid2":"categoryId_buyCategory2", "cid_ocid3":"categoryId_buyCategory3",
        "cid_ocid4":"categoryId_buyCategory4", "cid_ocid5":"categoryId_buyCategory5",
        "cid_order":"categoryId_purchasedCategories", 
        "gender_cid":"gender_categoryId", "gender_parent":"gender_parentCategoryId", "uid_order":"hashUid_purchasedCraftsmans",
        "cid_grade":"categoryId_categoryGrade","city_province":"userCity_province","province":"province"
        }



def main():
    definition = list()
    for fn in codecs.open("model_10","rb"):
        ln = fn.strip()

        lnn = ln.split("\t")
        name = lnn[0]
        value = lnn[1]
        weight = float(lnn[2])
        if weight == 0.0 or name == "(INTERCEPT)":
            continue
        if len(name.split("_")) == 2:
            new_name = feature[name]
            lr_name = [new_name.split("_")[0], new_name.split("_")[1]]
            lr_term = [value.split("_")[0], value.split("_")[1]]
        else:
            lr_name = [feature[name]]
            lr_term = [value]


        definition.append({"name":lr_name, "term":lr_term, "weight":weight})
    _finally = {"model":{"name":"index_lr_model","model":{"type":"model/logistic", "definition":definition}}}
    wf = open("lr_model.json", "w")
    json.dump(_finally, wf)
    print "finished"

        



if __name__ == "__main__":
    main()

