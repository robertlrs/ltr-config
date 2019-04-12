#!/bin/bash

curl -X GET "localhost:9200/item/_search" -H 'Content-Type: application/json' -d'
{
  "from": 0,
  "size": 64,
  "query": {
    "bool": {
      "must": [
        {
          "has_parent": {
            "query": {
              "match_all": {
                "boost": 1
              }
            },
            "parent_type": "user",
            "score": false,
            "ignore_unmapped": false,
            "boost": 0,
            "inner_hits": {
              "ignore_unmapped": false,
              "from": 0,
              "size": 20,
              "version": false,
              "explain": false,
              "track_scores": false
            }
          }
        }
      ],
      "filter": [
        {
          "bool": {
            "must": [
              {
                "range": {
                  "stock": {
                    "from": 0,
                    "to": null,
                    "include_lower": false,
                    "include_upper": true,
                    "boost": 1
                  }
                }
              },
              {
                "terms": {
                  "iid": [
					1017684,
                    111221,
                    242817,
                    143069,
                    337699,
                    256387,
                    865303,
                    856058,
                    337698,
                    1326914,
                    1248442,
                    1242216,
                    337697,
                    306711,
                    1125248,
                    1278024,
                    337679,
                    1259311,
                    1323045,
                    1311643,
                    337678,
                    1160980,
                    822892,
                    600666,
                    337677,
                    857957,
                    990466,
                    1226845,
                    337676,
                    1082174,
                    871159,
                    1218519,
                    337675,
                    1000306,
                    909708,
                    1301818,
                    337674,
                    114913,
                    396474,
                    1248313,
                    337672,
                    1311881,
                    986460,
                    1310055,
                    337671,
                    1109304,
                    1255184,
                    925330,
                    337553,
                    1311035,
                    924360,
                    756052,
                    337552,
                    1321266,
                    1301049,
                    672212,
                    337547,
                    1192408,
                    1251281,
                    104283,
                    337546,
                    1250490,
                    402516,
                    297019,
                    337529
                  ],
                  "boost": 1
                }
              },
              {
                "terms": {
                  "type": [
                    1,
                    2
                  ],
                  "boost": 1
                }
              },
              {
                "term": {
                  "itemStatus": {
                    "value": 1,
                    "boost": 1
                  }
                }
              },
              {
                "term": {
                  "bottom": {
                    "value": 0,
                    "boost": 1
                  }
                }
              },
              {
                "term": {
                  "postStatus": {
                    "value": 0,
                    "boost": 1
                  }
                }
              },
              {
                "term": {
                  "filter": {
                    "value": 0,
                    "boost": 1
                  }
                }
              },
              {
                "bool": {
                  "must_not": [
                    {
                      "terms": {
                        "saleType": [
                          1,
                          3
                        ],
                        "boost": 1
                      }
                    }
                  ],
                  "adjust_pure_negative": true,
                  "boost": 1
                }
              },
              {
                "has_parent": {
                  "query": {
                    "bool": {
                      "must": [
                        {
                          "term": {
                            "inBlackList": {
                              "value": false,
                              "boost": 1
                            }
                          }
                        },
                        {
                          "term": {
                            "craftsmanRemoved": {
                              "value": false,
                              "boost": 1
                            }
                          }
                        }
                      ],
                      "adjust_pure_negative": true,
                      "boost": 1
                    }
                  },
                  "parent_type": "user",
                  "score": true,
                  "ignore_unmapped": false,
                  "boost": 1
                }
              }
            ],
            "adjust_pure_negative": true,
            "boost": 1
          }
        }
      ],
      "adjust_pure_negative": true,
      "boost": 1
    }
  },
  "_source": {
    "includes": [
      "iid",
      "username",
      "createtm",
      "city",
      "pictures",
      "price",
      "saleType",
      "type",
      "tags",
      "title",
      "categoryId",
      "sellerId"
    ],
    "excludes": []
  },
  "track_scores": true,
  "rescore": [
    {
      "window_size": 200,
      "query": {
        "rescore_query": {
          "sltr": {
            "model": "index_lr_model",
            "params": {
              "uCity": "others",
              "preferenceCategory": [],
              "gender": 1,
              "uType": 0,
              "preferenceKind": []
            },
            "boost": 1
          }
        },
        "query_weight": 0,
        "rescore_query_weight": 1,
        "score_mode": "total"
      }
    }
  ]
}
'
