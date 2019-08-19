#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time    : 2019/8/19 19:13 
# @Author  : Wenbo Hu 
# @Site    :  
# @File    : sendData2kafkaDemo2.py 
# @Software: PyCharm Community Edition
import json
import logging
from kafka import KafkaProducer
from uuid import uuid4
import time


def get_uuid():
    time_ticks = str(time.time()).split('.')[0]
    payload = str(uuid4()).replace("-", "")
    return time_ticks + payload


def send_data2kafka():
    producer = KafkaProducer(bootstrap_servers=["xxx:9092,xxx:9093,xxx:9094"])
    topic_list = ['xxxx']
    i = 0
    for topic in topic_list:
        data = ""
        send_data = {"t": data, "timestamp": str(time.time()).split('.')[0], "topic": topic,
                     "subType": data}
        # producer.produce(bytes(json.dumps(send_data), encoding="utf8"),partition_key=bytes(payload,encoding="utf8"))
        producer.send(topic, bytes(json.dumps(send_data), encoding='utf-8'), key=bytes(get_uuid(), encoding='utf-8'))
        logger.info('发送成功:{}条数据'.format(i))
    producer.close()


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
