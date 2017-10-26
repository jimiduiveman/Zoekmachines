# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render

from django.http import HttpResponse

from wordcloud import WordCloud

import json




from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def index(request):
    context = {}
    return render(request, 'moties/index.html', context)


def search(request):
    search_query = request.POST['search_box']
    # Do whatever you need with the word the user looked for
    q = {
        "query" : {
            "match_phrase" : {
                "vraag" : search_query
            }
        },
        "highlight": {
            "fields" : {
                "titel" : {}
            }
        }
    }
    res = es.search(index="moties", body=q)
    result_list = []
    for x in range(len(res['hits']['hits'])):
        if 'highlight' in res['hits']['hits'][x]:
            result_list.append( { "id":res['hits']['hits'][x]['_id'],
                              "score": res['hits']['hits'][x]['_score'],
                              "titel": res['hits']['hits'][x]['highlight']['titel'][0]
                            } )
        else:
            result_list.append( { "id":res['hits']['hits'][x]['_id'],
                              "score": res['hits']['hits'][x]['_score'],
                              "titel": res['hits']['hits'][x]['_source']['titel']
                            } )

    context = { "result_list" : result_list, "query": search_query }

    return render(request, 'moties/query_result.html', context)

def adv_search(request):
    adv_search_query = request.POST['adv_search_box']
    partij_search_query = request.POST['partij_search_box']
    jaar_search_query = request.POST['jaar_search_box']
    # Do whatever you need with the word the user looked for
    q = {
      "query": {
        "bool": {
          "should": [
            { "match_phrase": { "vraag":  adv_search_query }},
            { "match_phrase": { "jaar": jaar_search_query   }},
            { "match_phrase": { "partij": partij_search_query   }}
          ]
        }
      },
      "highlight": {
          "fields" : {
              "titel" : {}
          }
      }
    }
    res = es.search(index="moties", body=q)
    result_list = []
    for x in range(len(res['hits']['hits'])):
        if 'highlight' in res['hits']['hits'][x]:
            result_list.append( { "id":res['hits']['hits'][x]['_id'],
                              "score": res['hits']['hits'][x]['_score'],
                              "titel": res['hits']['hits'][x]['highlight']['titel'][0]
                            } )
        else:
            result_list.append( { "id":res['hits']['hits'][x]['_id'],
                              "score": res['hits']['hits'][x]['_score'],
                              "titel": res['hits']['hits'][x]['_source']['titel']
                            } )

    context = { "result_list" : result_list, "adv_search_query": adv_search_query, "jaar_search_query": jaar_search_query, "partij_search_query":partij_search_query }

    return render(request, 'moties/adv_query_result.html', context)

def wordcloud(request):

    def gen_tags(words): # words is a dict with string:integer pairs
        return ' '.join([('<font size="%d">%s</font>'%(min(1+p*10/max(words.values()), 5), x)) for (x, p) in words.items()])

    def make_cloud(woorden):
        woorden = { k:woorden[k] for k in woorden if k != search_query }
        cloud='<center>'+gen_tags(woorden)+'</center>'
        return cloud

    search_query = request.POST['wordcloud_search_box']
    # Do whatever you need with the word the user looked for
    q = {
            "query": {
                "match": {
                    "vraag": search_query
                }
            },
            "aggs" : {
                "tag" : {
                    "significant_terms" : {
                        "field": "vraag",
                        "size" : 30
                    }
                }
            },
            "size":0
        }
    result = es.search(index="moties", body=q)
    result_list = { result['aggregations']['tag']['buckets'][x]['key']:result['aggregations']['tag']['buckets'][x]['doc_count'] for x in range(len(result['aggregations']['tag']['buckets'])) }
    wordcloud_data = make_cloud(result_list)
    context = { "wordcloud_data" : wordcloud_data, "query": search_query }

    return render(request, 'moties/wordcloud.html', context)


def timeline(request):
    search_query = request.POST['timeline_search_box']
    # Do whatever you need with the word the user looked for
    q = {

            "query": {
                "match": {
                    "vraag": search_query
                        }
                    },

      "size": 0,
      "aggs": {
        "prod_agg": {
          "terms": {
            "field": "jaar"
          },
          "aggs": {
            "brand_agg": {
              "terms": {
                "field": "partij"
              }
            }
          }
        }
      }
    }

    res = es.search(index="moties", body=q)
    bucket = res['aggregations']['prod_agg']['buckets']
    results_list = [{"jaar":bucket[y]["key"], "hits":bucket[y]["doc_count"], "partij_count": [ bucket[y]['brand_agg']['buckets'][x] for x in range(len(bucket[y]['brand_agg']['buckets']))] } for y in range(len(bucket)-1) ]
    result_list = sorted(results_list, key=lambda k: k['jaar'], reverse=True)
    context = { "result_list" : result_list, "query": search_query }

    return render(request, 'moties/timeline.html', context)


def faceted_search(request):
    search_query = request.POST['faceted_search_box']
    # Do whatever you need with the word the user looked for
    q= {
        "query": {
            "match": {
                "vraag": search_query
                    }
                }
        }
    res = es.search(index="moties", body=q)
    result_list = [ res['hits']['hits'][x]['_id'] for x in range(len(res['hits']['hits'])) ]
    context = { "result_list" : result_list, "query": search_query }

    return render(request, 'moties/faceted_query_result.html', context)

def detail(request, motie_id):
    q= {
        "query": {
            "match": {
                "_id": motie_id
                    }
                }
        }

    res = es.search(index="moties", body=q)
    titel = res['hits']['hits'][0]['_source']['titel']
    partij = res['hits']['hits'][0]['_source']['partij']
    jaar = res['hits']['hits'][0]['_source']['jaar']
    ministerie = res['hits']['hits'][0]['_source']['ministerie']
    vraag = res['hits']['hits'][0]['_source']['vraag']
    antwoord = res['hits']['hits'][0]['_source']['antwoord']

    context = { "motie_id":motie_id, "titel":titel, "partij":partij, "jaar":jaar, "vraag":vraag, "antwoord":antwoord, "ministerie":ministerie}
    return render(request, 'moties/detail.html', context)
