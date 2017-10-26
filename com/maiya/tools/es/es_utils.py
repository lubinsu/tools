from datetime import datetime
import json
from time import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def aaa():
    es = Elasticsearch(hosts=['master', 'slaver01', 'slaver02'], http_auth=('elastic', 'changeme'))

    doc = {
        'author': 'kimchy',
        'text': 'Elasticsearch: cool. bonsai cool.',
        'timestamp': datetime.now(),
    }
    res = es.index(index="myd_kafka.log", doc_type='myd_kafka', body=doc)

    print(res['created'])

    res = es.get(index="test-index", doc_type='tweet', id=1)
    print(res['_source'])

    es.indices.refresh(index="test-index")

    res = es.search(index="test-index", body={"query": {"match_all": {}}})
    print("Got %d Hits:" % res['hits']['total'])
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

if __name__ == '__main__':
    es = Elasticsearch(hosts=['master', 'slaver01', 'slaver02'], http_auth=('elastic', 'changeme'))
    index = 'myd_kafka.log-%s' % datetime.utcnow().strftime('%Y-%m-%d')

    docs = []
    doc1 = {'topic': 'hy_userlogininfo',
        'table': 'hy_userlogininfo',
        'status': '插入/更新成功',
        'message': '{"table_name":"hy_userlogininfo","data":[{"id":"14ea9684fcd34922bd924e8a68fbba62","deviceNo":"863837023689111","blackBox":null,"address":null,"addDate":"2017-08-10 09:17:27","memberId":"90000015","lbs":"{}","ip":"192.168.161.11","city":null,"memberNo":"90000015"}],"table_status":"insert"}',
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f'),
        'detail': 'message1'}
    # res = es.index(index=index, doc_type='myd_kafka', id='14ea9684fcd34922bd924e8a68fbba62', body=doc1)
    docs.append(json.dumps({
        'index':{
            '_index': index,
            '_type': 'myd_kafka'
        }
    }))
    docs.append(json.dumps(doc1))
    doc2 = {
        'topic': 'hy_userlogininfo',
        'table': 'hy_userlogininfo',
        'status': 'SUCCESS',
        'message': '{"table_name":"hy_userlogininfo","data":[{"id":"14ea9e84fcd34922bd924e8a68fbba62","deviceNo":"863837023689111","blackBox":null,"address":null,"addDate":"2017-08-10 09:17:27","memberId":"90000015","lbs":"{}","ip":"192.168.161.11","city":null,"memberNo":"90000015"}],"table_status":"insert"}',
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f'),
        'detail': 'message2'
    }
    # res = es.index(index=index, doc_type='myd_kafka', id='14ea9684fcd34922bd924e8a68fbba63', body=doc2)
    docs.append(json.dumps({
        'index':{
            '_index': index,
            '_type': 'myd_kafka'
        }
    }))
    docs.append(json.dumps(doc2))

    if es.indices.exists(index=index) is not True:
        es.indices.create(index=index)
    es.bulk(body='\n'.join(docs), index=index, doc_type='myd_kafka')

