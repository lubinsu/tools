import pykafka
from pykafka import KafkaClient


def connKafka():
    client = KafkaClient(hosts='kafka01:9092,kafka02:9092,kafka03:9092')
    topic = client.topics['my_busi_int_monitor'.encode()]
    consumer = topic.get_balanced_consumer(
        consumer_group='my_group2'.encode(),
        auto_commit_enable=True,
        zookeeper_connect='hadoop04,hadoop08,hadoop06,hadoop07,hadoop05'
    )

    return consumer


def consume(consumer):
    for message in consumer:
        if message is not None:
            print(message.partition.id, message.offset, message.value.decode())


if __name__ == '__main__':
    client = KafkaClient(hosts='myd3:9092,myd4:9092,myd5:9092,myd2:9092')
    topic = client.topics['hy_userinfo'.encode()]
    print(topic.partitions)
    print(topic.partitions.keys())
    for i in topic.partitions.keys():
        print(topic.partitions[i].latest_available_offset())
        print(i)
