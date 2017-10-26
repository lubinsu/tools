# coding=utf-8
import datetime

import logging
from kazoo.client import KazooClient

# Zookeepers - no need to add ports
from kazoo.exceptions import NoNodeError
from pykafka import KafkaClient
from pykafka.common import OffsetType

from file_utils.properties import Properties
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
import os

zookeepers = 'hadoop04:2181,hadoop08:2181,hadoop06:2181,hadoop07:2181,hadoop05:2181'
# zookeepers = "myd4:2181,myd5:2181,myd6:2181"

# Kafka broker
kafka = 'kafka01:9092,kafka02:9092,kafka03:9092'
# kafka = "myd3:9092,myd4:9092,myd6:9092"

logger = logging.getLogger("kafka_logger")
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('kafka-topic.log')
ch.setLevel(logging.INFO)
fomatter = logging.Formatter('%(message)s')
ch.setFormatter(fomatter)
logger.addHandler(ch)


def sendMail(msg):
    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header
    mail_host = "smtp.163.com"  # 设置服务器
    mail_user = "sulubin123@163.com"  # 用户名
    mail_pass = "3003U9736"  # 口令
    sender = 'sulubin123@163.com'
    receivers = ['sulubin123@163.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(msg, 'plain', 'utf-8')
    message['From'] = sender
    message['To'] = Header('lubinsu', 'utf-8')
    subject = 'Kafka Offsets Warning'
    message['Subject'] = Header(subject, 'utf-8')
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

def get_group_offsets(group_name, topic, offset_type):

# consumer group
def check_group_offsets(group_name, topics_monitor_o):
    """
    kafka监控模块
    :param group_name:
    :type group_name str
    :param topics_monitor:
    :type topics_monitor list
    :return:
    """
    kafka_version = os.environ.get('KAFKA_VERSION', '0.8.0')
    topics_monitor = []
    for t in topics_monitor_o:
        topics_monitor.append(str(t).split(':')[0])
    client = KafkaClient(kafka, broker_version='0.8.2.1')
    lags = {}
    warn_lags = {}
    zk = KazooClient(hosts=zookeepers, read_only=True)  # zookeeper客户端，read_only确保不会对zookeeper更改
    zk.start()
    # topics = zk.get_children('/consumers/%s/owners' % group_name)
    topics = []
    for t in client.topics.keys():
        topics.append(t.decode())
    for topic in topics:
        if topic in topics_monitor:
            latest_offset = client.topics[topic.encode()].latest_available_offsets()
            consumer = client.topics[topic.encode()].get_simple_consumer(consumer_group=group_name.encode(),
                                                                         auto_offset_reset=OffsetType.LATEST,
                                                                         reset_offset_on_start=False)
            try:
                fetch_offsets = consumer.fetch_offsets()
                held_offsets = consumer.held_offsets
                print('fetch_offsets:' + str(fetch_offsets))
                print('held_offsets:' + str(held_offsets))
                print('latest_offset:' + str(latest_offset))
                return warn_lags


                # for partition in held_offsets.keys():
                #     lag = latest_offset[int(partition)].offset[0] - held_offsets[partition]
                #     if lag >= 50:
                #         warn_lags['lags|%s|%s|%s|%s' % (
                #             group_name, topic, partition, latest_offset[int(partition)].offset[0])] = lag
                #     lags['lags|%s|%s|%s|%s' % (
                #         group_name, topic, partition, latest_offset[int(partition)].offset[0])] = lag
            except NoNodeError as e:
                print('NoNodeError group: %s topic: %s' % (group_name, topic))
    zk.stop()
    for lg in lags.items():
        logger.info('%s|%s|%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), lg[0], lg[1]))
    return warn_lags


def get_application_id(spark_streaming, group_name, topic):
    rm = ResourceManager(address='hadoop03', port=8088)
    runningjobs = rm.cluster_applications(state='RUNNING').data
    key = group_name + '|' + topic
    for job in runningjobs['apps']['app']:
        if job['name'] == spark_streaming[key]:
            return job['id']


def convert_kafka_tables_to_dict():
    pro = Properties('../resources/kafka_talbes').getProperties()
    java_consumer = {}
    for i in pro.items():
        key = i[1].split(',')[3] + '|' + i[0]
        java_consumer[key] = 'kafka01'
    print(java_consumer)


def convert_kafka_tables_to_monitor():
    pro = Properties('../resources/kafka_talbes').getProperties()
    kafkaMonitor = {}
    for i in pro.items():
        key = i[1].split(',')[3]
        if key in kafkaMonitor.keys():
            kafkaMonitor[key] = kafkaMonitor[key] + ',' + i[0]
        else:
            kafkaMonitor[key] = i[0]
    for m in kafkaMonitor.keys():
        print(m + '=' + kafkaMonitor[m])


# if __name__ == '__main__':
#     convert_kafka_tables_to_monitor()

if __name__ == '__main__':
    pro = Properties('resources/kafkaMonitor.bk.properties').getProperties()
    java_consumer = {}
    spark_streaming = {}

    for kafka_key in pro.keys():
        group = kafka_key
        for topic_detail in str(pro[kafka_key]).split(','):
            topic = topic_detail.split(':')[0]
            topic_type = topic_detail.split(':')[1]
            host_or_class = topic_detail.split(':')[2]
            consumer_streaming_key = group + '|' + topic

            if topic_type == 'consumer':
                java_consumer[consumer_streaming_key] = host_or_class
            elif topic_type == 'streaming':
                spark_streaming[consumer_streaming_key] = host_or_class

    all_warn_lags = {}
    application_for_kill = {}
    consumer_for_kill = {}
    mail_detail = ''
    for i in pro.items():
        all_warn_lags.update(check_group_offsets(i[0], i[1].split(',')))
    if len(all_warn_lags) >= 1 and datetime.datetime.now().strftime('%M') == '00':
        # if len(all_warn_lags) >= 1 and datetime.datetime.now().strftime('%M') == '00':
        for lag in all_warn_lags.items():
            group_name = lag[0].split('|')[1]
            topic = lag[0].split('|')[2]
            group_topic = group_name + '|' + topic

            # kafka 消费重启
            if group_topic in java_consumer.keys():
                consumer_for_kill[
                    java_consumer[group_topic]] = 'ssh -t -p 22 hadoop@%s /opt/cloudera/maiya/restartConsumer.sh' % \
                                                  java_consumer[group_topic]
            # 流式计算重启
            if group_topic in spark_streaming.keys():
                application_for_kill[get_application_id(spark_streaming, group_name,
                                                        topic)] = 'yarn application -kill %s' % get_application_id(
                    spark_streaming, group_name, topic)
            mail_detail += ('%s|%s|%s \n' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), lag[0], lag[1]))
        sendMail(mail_detail)
    for app in application_for_kill.items():
        print('killing %s ' % app[1])
        os.system(app[1])
    for con in consumer_for_kill.items():
        print('killing %s ' % con[1])
        os.system(con[1])
