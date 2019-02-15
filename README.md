### Project description

Based on the kombu

*The sample code*
```python
def worker(data):
            ...


        consumer = Consumer("amqp://smallrabbit:123456@172.16.20.73:5672/order", "q.order.tyxb.zfk", worker)
        consumer.run()
```