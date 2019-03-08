#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: WangJY

import signal
from multiprocessing.dummy import Pool
from concurrent.futures import ThreadPoolExecutor

from kombu import Connection, Queue, Producer, pools
from kombu.mixins import ConsumerMixin

SUCCESS = 0
REDELIVER = 1
REJECT = 2


class Consumer(ConsumerMixin):
    """
    例子：
        def worker(data):
            ...
            :return SUCCESS


        consumer = Consumer("amqp://smallrabbit:123456@172.16.20.73:5672/order", "q.order.tyxb.zfk", worker)
        consumer.run()
    """

    def __init__(self, amqp_url, queues, func, prefetch_count=30, thread_num=5, heart_interval=30,
                 consumer_tag=None, is_rpc=False, durable=True):
        """
        :param amqp_url: 队列地址
        :param queues: 队列名,可以接收多个多列
        :param func: 处理函数
        :param prefetch_count: 一次拉取的消息数，默认30
        :param thread_num: 线程池线程个数，默认5
        :param heart_interval: 心跳间隔，默认30秒
        :param consumer_tag:
        :param is_rpc: 是否是RPC服务，默认为False
        :param durable: 是否持久化
        """
        self.connection = Connection(amqp_url, heart_interval=heart_interval)
        self.queue = [Queue(queue, durable=durable) for queue in queues] if type(queues) is list \
            else [Queue(queues, durable=durable)]
        self.consumers = list()
        self.consumer_tag = consumer_tag
        self.pool = Pool(thread_num)
        self.prefetch_count = prefetch_count
        self.func = func
        self.is_rpc = is_rpc

    def get_consumers(self, consumer_cls, channel):
        consumer = consumer_cls(self.queue, callbacks=[self.on_message], tag_prefix=self.consumer_tag,
                                auto_declare=False)
        consumer.qos(prefetch_count=self.prefetch_count)
        self.consumers.append(consumer)
        return self.consumers

    def on_message(self, body, message):
        """
        线程池调用函数
        :param body: 收到的内容
        :param message: 队列收到的对象
        :return:
        """
        try:
            if not self.connection.connected:
                self.on_connection_revived()
            self.pool.apply_async(self.message_work, args=(body, message))
        except AssertionError:
            message.requeue()

    def stop(self, sig_number, stack_frame):
        """
        入参暂时未处理
        :param sig_number: 信号
        :param stack_frame:
        :return:
        """
        for consumer in self.consumers:
            consumer.close()
        self.pool.close()
        self.pool.join()
        self.connection.release()
        del sig_number, stack_frame

    def message_work(self, body, message):
        result = self.func(body)
        if isinstance(result, tuple):
            code = result[0]
            msg = result[1]
        else:
            code = result
            msg = None

        if code is SUCCESS:
            message.ack()
        elif code is REDELIVER:
            message.requeue()
        elif code is REJECT:
            message.reject()
        else:
            message.reject()
        if self.is_rpc:
            Producer(message.channel).publish(body=str(msg), routing_key=message.preoperties["reply_to"],
                                              **{"correlation_id": message.preoperties["correlation_id"]})

    def run(self, _tokens=1, **kwargs):
        """
        启动位置
        :param _tokens: 父类参数默认值
        :param kwargs: 父类参数默认值
        :return:
        """
        assert self.func is not None
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        super(Consumer, self).run()


class Consumer2(Consumer):
    """
    使用不同的线程池，使用方法与父类一样
    """

    def __init__(self, amqp_url, queues, func, thread_num=5, prefetch_count=30, heart_interval=30,
                 consumer_tag=None, is_rpc=False, durable=True):
        super(Consumer2, self).__init__(amqp_url, queues, func, prefetch_count=prefetch_count, thread_num=thread_num,
                                        heart_interval=heart_interval, consumer_tag=consumer_tag,
                                        is_rpc=is_rpc, durable=durable)

        self.pool = ThreadPoolExecutor(max_workers=thread_num)

    def on_message(self, body, message):
        """
        线程池调用函数
        :param body: 收到的内容
        :param message: 队列收到的对象
        :return:
        """
        try:
            self.pool.submit(self.message_work, body, message)
        except AssertionError:
            message.requeue()

    def stop(self, sig_number, stack_frame):
        """
        入参暂时未处理
        :param sig_number: 信号
        :param stack_frame:
        :return:
        """
        for consumer in self.consumers:
            consumer.close()
        self.pool.shutdown(wait=True)
        self.connection.release()
        del sig_number, stack_frame


class RabbitMQ:
    """
    发送队列:
    EXCHANGE = "wjy.test"
    ROUTING_KEY = "domestic"
    sendmsg = RabbitMQ(host="172.16.20.73",
                       port="5672",
                       vhost="order",
                       userid="smallrabbit",
                       password="123456")

    data = {"a": 1}
    sendmsg.send(article=json.dumps(data), exchange=EXCHANGE, routing_key=ROUTING_KEY)
    """
    def __init__(self, host, port, vhost, userid, password, heartbeat=50):
        self.__params = {
            "HOST": host, "PORT": port, "VHOST": vhost, "HEARTBEAT": heartbeat,
            "USERID": userid, "PASSWORD": password
        }
        self.producers = None

    def init_connection(self):
        try:
            connnections = pools.Connections(limit=50)
            producers = pools.Producers(limit=connnections.limit)

            self.conn = Connection(hostname=self.__params["HOST"],
                                   port=self.__params["PORT"],
                                   virtual_host=self.__params["VHOST"],
                                   heartbeat=self.__params["HEARTBEAT"],
                                   userid=self.__params["USERID"],
                                   password=self.__params["PASSWORD"],
                                   insist=True)
            self.producers = producers[self.conn].acquire(block=True, timeout=3)
        except:
            return None

    def send(self, article, exchange, routing_key, delivery_mode=2, serializer="json"):
        try:
            if not self.producers:
                self.init_connection()
            self.producers.publish(
                body=article,
                exchange=exchange,
                routing_key=routing_key,
                delivery_mode=delivery_mode,
                serializer=serializer,
            )
            return True
        except:
            self.conn.revive(self.producers)
            result = self.send(article, exchange, routing_key)
            if result:
                return True
            return False

    def close(self):
        try:
            self.producers.close()
            self.conn.close()
        except:
            return
