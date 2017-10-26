"""
kafka 工具类，数据接入大数据
"""
from pykafka import KafkaClient
from pykafka.common import OffsetType


class Producer:
    """
    创建kafka连接
    """

    def __init__(self, topic_name, hosts='myd3:9092,myd4:9092,myd6:9092'):
        self.__client__ = KafkaClient(hosts=hosts)
        self.__topic__ = self.__client__.topics[topic_name.encode()]
        self.__producer__ = self.__topic__.get_producer(delivery_reports=False)

    def send_msg(self, message, partition_key):
        self.__producer__.produce(message.encode(), partition_key.encode())

    def close(self):
        self.__producer__.stop()


class Consumer:
    """
    创建consumer
    """

    def __init__(self, topic_name, consumer_group, hosts='myd3:9092,myd4:9092,myd6:9092',
                 zookeeper_connect='myd4:2181,myd5:2181,myd6:2181'):
        self.__client__ = KafkaClient(hosts=hosts)
        self.__topic__ = self.__client__.topics[topic_name.encode()]
        self.__consumer__ = self.__topic__.get_balanced_consumer(consumer_group=consumer_group.encode(),
                                                                 auto_commit_enable=True,
                                                                 zookeeper_connect=zookeeper_connect)

    def consume(self):
        for msg in self.__consumer__:
            if msg is not None:
                print('partition_id:%s, offset:%s, value:%s' % (
                str(msg.partition.id), str(msg.offset), str(msg.value.decode())))
                # self.__consumer__.commit_offsets()

    def close(self):
        self.__consumer__.stop()

    def get_offsets(self):
        print(self.__consumer__.held_offsets)
        for (parttion, offset) in self.__consumer__.held_offsets.items():
            print(parttion, offset)


if __name__ == '__main__':
    # 创建doge
    consumer = Consumer('hy_userinfo', consumer_group='my_group2')
    consumer.get_offsets()
    consumer.close()

    kafka = 'kafka01:9092,kafka02:9092,kafka03:9092'
    client = KafkaClient(kafka)
    topic = client.topics[b'tbl_policy']

    consumer = topic.get_simple_consumer(
        consumer_group=b'test',
        auto_offset_reset=OffsetType.EARLIEST,
        reset_offset_on_start=False
    )



def test_reset_offsets(self):
    """Test resetting to user-provided offsets"""
    with self._get_simple_consumer(
            auto_offset_reset=OffsetType.EARLIEST) as consumer:
        # Find us a non-empty partition "target_part"
        part_id, latest_offset = next(
            (p, res.offset[0])
            for p, res in consumer.topic.latest_available_offsets().items()
            if res.offset[0] > 0)
        target_part = consumer.partitions[part_id]

        # Set all other partitions to LATEST, to ensure that any consume()
        # calls read from target_part
        partition_offsets = {
            p: OffsetType.LATEST for p in consumer.partitions.values()}

        new_offset = latest_offset - 5
        partition_offsets[target_part] = new_offset
        consumer.reset_offsets(partition_offsets.items())

        self.assertEqual(consumer.held_offsets[part_id], new_offset)
        msg = consumer.consume()
        self.assertEqual(msg.offset, new_offset + 1)

        # Invalid offsets should get overwritten as per auto_offset_reset
        partition_offsets[target_part] = latest_offset + 5  # invalid!
        consumer.reset_offsets(partition_offsets.items())

        # SimpleConsumer's fetcher thread will detect the invalid offset
        # and reset it immediately.  RdKafkaSimpleConsumer however will
        # only get to write the valid offset upon a call to consume():
        msg = consumer.consume()
        expected_offset = target_part.earliest_available_offset()
        self.assertEqual(msg.offset, expected_offset)
        self.assertEqual(consumer.held_offsets[part_id], expected_offset)
