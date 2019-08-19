#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 19:08 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : sendData2kafkaDemo1.py 
# @Software: PyCharm Community Edition
from pykafka import KafkaClient
import json
import logging


def send_data2kafka():
    host = "xxxx:9092"

    client = KafkaClient(hosts=host)
    topic_list = ['xxx']
    for topic in topic_list:
        topicpro = client.topics[bytes(topic)]
        producer = topicpro.get_producer()
        data = ""
        send_data = {"t": data, "timestamp": "1515496933364", "topic": topic}
        producer.produce(bytes(json.dumps(send_data)))
        producer.stop()
    print(client.topics)

    # 消费
    # topic = client.topics[b'Application_taobao']
    # consumer = topic.get_simple_consumer(consumer_group=b'test', auto_commit_enable=True, consumer_id=b'test')
    # for message in consumer:
    #     if message is not None:
    #         # print(message.offset, message.value)
    #         data = json.loads(message.value).get('t')
    #         data_dic = dict()
    #         for item, value in data.items():


def create_logger(log_name):
    logger1 = logging.getLogger(log_name)
    logger1.setLevel(logging.DEBUG)

    if not logger.handlers:
        # The max size of the log file is 100M.
        # If exceeded, a new one will be created.
        console_hdlr = logging.StreamHandler()
        console_hdlr.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                "[%(asctime)s] [%(funcName)s in %(filename)s:%(lineno)d] [%(levelname)s]:%(message)s")
        console_hdlr.setFormatter(formatter)
        logger.addHandler(console_hdlr)
    return logger

logger = create_logger("/opt/logs/python.log")

if __name__ == '__main__':
    logger.debug("begin send data 2 kafka")
    send_data2kafka()
