===================
Project description
===================

**Python version must be greater than 3.7.1**

*Based on the package*

- kombu==3.0.35
- pika==0.13.1

*The sample code*

::
    def worker(data):
                ...
                return SUCCESS


            consumer = Consumer("amqp://account:password@ip:port/vhost", "queue", worker)
            consumer.run()

