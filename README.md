### Project description

Based on the kombu

*The sample code*
```python
def worker(data):
            ...


        consumer = Consumer("amqp://account:password@ip:port/vhost", "queue", worker)
        consumer.run()
```