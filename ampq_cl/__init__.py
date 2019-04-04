#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: WangJY
import signal
from multiprocessing.dummy import Pool
from concurrent.futures import ThreadPoolExecutor

from kombu import Connection, Queue, Producer, pools
from kombu.mixins import ConsumerMixin
import pika

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


class PikaConsumer(object):
    # TODO:未完成
    # EXCHANGE = ""
    # EXCHANGE_TYPE = ""
    # QUEUE = ""
    # ROUTING_KEY = ""

    def __init__(self, amqp_url, queue, logger, prefetch_count=30, thread_num=5, heart_interval=30,
                 consumer_tag=None, is_rpc=False):
        """
        :param amqp_url: 队列amqp地址
        :param queue: 队列名称
        :param logger: logger对象
        :param prefetch_count: 一次拉取消息数量，默认30
        :param thread_num: 线程或进程数量，默认5
        :param heart_interval: 心跳间隔，默认30秒
        :param is_rpc: 是否为RPC服务，默认为False
        :return:
        """
        self.logger = logger
        self._connection = None
        self._channel = None
        self._closing = False
        self._url = amqp_url + '?heartbeat=' + str(heart_interval)
        self.consumers = []
        self.pool = Pool(thread_num)
        self.queue = queue
        self.handle_message_worker = None
        # 限制单个channel可同时拉取的消息数量
        self.prefetch_count = prefetch_count
        self._consumer_tag = consumer_tag
        self.is_rpc = is_rpc

    def connect(self):
        self.logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self.logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self.logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                                reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object 

        """
        self.logger.info('Channel opened')
        self._channel = channel
        self._channel.basic_qos()
        self.add_on_channel_close_callback(prefetch_count=self.prefetch_count)
        self.start_consuming()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self.logger.warning('Channel %i was closed: (%s) %s',
                            channel, reply_code, reply_text)
        self._connection.close()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self.logger.info('Consumer was cancelled remotely, shutting down: %r',
                         method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        self.logger.info('Received message # %s from %s: %s',
                         basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        self.logger.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self.logger.info('Closing connection')
        self._connection.close()

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self.logger.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self.logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self.logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok,
                                    queue_name)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self.logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self.logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self.logger.info('Queue bound')
        self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self.logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self.logger.info('Stopped')


class RabbitMQSend(object):
    """
    基于kombu库的发送队列:
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


class RabbitMQSend2(object):
    """
    基于pika库的发送数据
    """
    pass



