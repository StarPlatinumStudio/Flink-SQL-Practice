# https://pypi.org/project/kafka-python/
import pickle
import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         key_serializer=lambda k: pickle.dumps(k),
                         value_serializer=lambda v: pickle.dumps(v))
start_time = time.time()
for i in range(0, 10000):
    print('------{}---------'.format(i))
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),compression_type='gzip')
    producer.send('log',{"response":"res","status":0,"protocol":"protocol","timestamp":0})
    producer.send('log',{"response":"res","status":1,"protocol":"protocol","timestamp":0})
    producer.send('log',{"response":"resKEY","status":2,"protocol":"protocol","timestamp":0})
    producer.send('log',{"response":"res","status":3,"protocol":"protocol","timestamp":0})
    producer.send('log',{"response":"res","status":4,"protocol":"protocol","timestamp":0})
    producer.send('log',{"response":"res","status":5,"protocol":"protocol","timestamp":0})
producer.flush()
producer.close()
#
end_time = time.time()
time_counts = end_time - start_time
print(time_counts)
