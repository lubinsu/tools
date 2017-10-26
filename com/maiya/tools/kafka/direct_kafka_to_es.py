import datetime
import json
import time

from elasticsearch import Elasticsearch
from pykafka import KafkaClient


def connKafka():
    client = KafkaClient(hosts='kafka01:9092,kafka02:9092,kafka03:9092')
    topic = client.topics['my_busi_int_monitor'.encode()]
    consumer = topic.get_balanced_consumer(
        consumer_group='my_group2'.encode(),
        auto_commit_enable=True,
        auto_commit_interval_ms=1,
        zookeeper_connect='hadoop04,hadoop08,hadoop06,hadoop07,hadoop05'
    )
    return consumer


def get_es():
    es = Elasticsearch(hosts=['171.16.10.150', '171.16.10.151', '171.16.10.152', '171.16.10.153', '171.16.10.154'], http_auth=('elastic', 'myd123'))
    return es


def consume(consumer):
    es = get_es()
    for message in consumer:
        if message is not None:
            table_name = 'my_busi_int_monitor-%s' % datetime.datetime.now().strftime('%Y.%m.%d')
            print(message.partition.id, message.offset, message.value.decode())
            docs = toES('my_busi_int_monitor', message.value.decode(), table_name)
            if len(docs) >= 1:
                if es.indices.exists(index=table_name) is not True:
                    es.indices.create(index=table_name)
                es.bulk(body='\n'.join(docs), index=table_name, doc_type='topic')
            if message.offset % 100 == 0:
                time.sleep(0.5)
                consumer.commit_offsets()


def toES(topic, message, table_name):
    js = json.loads(message)
    docs = []
    for data in js['data']:
        doc = {'timestamp': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')}
        for col in data.keys():
            doc[col] = data[col]
        docs.append(json.dumps({
            'index': {
                '_index': table_name,
                '_type': topic
            }
        }))
        docs.append(json.dumps(doc))
    return docs


if __name__ == '__main__':
    consumer = connKafka()
    consume(consumer)
