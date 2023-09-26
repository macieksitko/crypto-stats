from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:9094',
        'group.id': 'tsst_group',
        'auto.offset.reset': 'smallest'}

c = Consumer(conf)
c.subscribe(['test'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()