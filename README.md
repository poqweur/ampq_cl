#### 该项目用于rabbitmq队列

Python version 3

两种consumer只是使用的线程池不同
```python
def worker(data):
                ...
                return SUCCESS


            consumer = Consumer("amqp://account:password@ip:port/vhost", "queue", worker)
            consumer.run()
```

```python
def worker(data):
                ...
                return SUCCESS


            consumer = Consumer2("amqp://account:password@ip:port/vhost", "queue", worker)
            consumer.run()
```

常用命令
- python3 setup.py sdist
- python3 setup.py sdist upload
- python3 setup.py bdist_wheel --universal
- python3 setup.py bdist_wheel upload