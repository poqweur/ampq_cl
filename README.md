### Project description

Based on the kombu, pika

**must kombu==3.0.35**

*The sample code*
```python
def worker(data):
            ...
            return SUCCESS


        consumer = Consumer("amqp://account:password@ip:port/vhost", "queue", worker)
        consumer.run()
```

打包命令

    python3 setup.py sdist
    python3 setup.py sdist upload
    python3 setup.py bdist_wheel --universal
    python3 setup.py bdist_wheel upload